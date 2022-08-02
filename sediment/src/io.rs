use std::{
    fmt::Debug,
    io::{self, ErrorKind},
    path::Path,
    sync::Arc,
};

pub use io::Result;
use parking_lot::{Condvar, Mutex};

use crate::io::{
    iobuffer::{AnyBacking, Backing, IoBuffer},
    paths::PathId,
};

pub mod fs;
pub mod iobuffer;
pub mod memory;
pub mod paths;
#[cfg(feature = "iouring")]
pub mod uring;

pub mod any;
pub mod ext;
#[cfg(target_os = "linux")]
mod linux;

pub trait FileManager: Debug + Default + Clone + Send + Sync + 'static {
    type File: File<Manager = Self>;
    type AsyncFile: AsyncFileWriter<Manager = Self>;

    fn resolve_path(&self, path: impl AsRef<Path>) -> PathId;

    fn read(&self, path: &PathId) -> io::Result<Self::File>;
    fn write(&self, path: &PathId) -> io::Result<Self::File>;
    fn write_async(&self, path: &PathId) -> io::Result<Self::AsyncFile>;

    fn synchronize(&self, path: &PathId) -> io::Result<()>;
    fn delete(&self, path: &PathId) -> io::Result<()>;
    fn delete_directory(&self, path: &Path) -> io::Result<()>;
}

pub trait File: Debug + WriteIoBuffer {
    type Manager: FileManager<File = Self>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|len| len == 0)
    }

    fn read_exact<B: Backing + Default + From<AnyBacking> + Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<IoBuffer<B>>,
        position: u64,
    ) -> BufferResult<(), B>;

    fn write_all<B: Backing + Default + From<AnyBacking> + Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<IoBuffer<B>>,
        position: u64,
    ) -> BufferResult<(), B>;

    fn synchronize(&mut self) -> io::Result<()>;
    fn set_length(&mut self, new_length: u64) -> io::Result<()>;
}

pub trait AsyncFileWriter: Debug + WriteIoBuffer + Send + Sync {
    type Manager: FileManager<AsyncFile = Self>;

    fn background_write_all<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<IoBuffer<B>>,
        position: u64,
    ) -> io::Result<()>;

    fn wait(&mut self) -> io::Result<()>;
}

pub type BufferResult<T, B> = (io::Result<T>, B);

#[derive(Debug)]
pub struct AsyncOpParams {
    path: PathId,
    position: u64,
    buffer: IoBuffer<AnyBacking>,
    result_sender: AsyncOpResultSender,
}

#[derive(Debug)]
pub enum AsyncOpResultSender {
    Buffer(flume::Sender<BufferResult<(), AnyBacking>>),
    Signal(AsyncOpSignal),
}

impl AsyncOpResultSender {
    pub(crate) fn send_result(&self, result: BufferResult<(), AnyBacking>) {
        match self {
            AsyncOpResultSender::Buffer(sender) => drop(sender.send(result)),
            AsyncOpResultSender::Signal(sender) => {
                if let Err(err) = result.0 {
                    sender.op_error(err);
                } else {
                    sender.op_complete();
                }
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AsyncOpSignal {
    data: Arc<AsyncOpSignalData>,
}

impl AsyncOpSignal {
    pub fn op_queued(&self) {
        let mut data = self.data.pending_writes.lock();
        data.0 += 1;
    }

    pub fn op_complete(&self) {
        let mut data = self.data.pending_writes.lock();
        data.0 -= 1;
        let signal = data.0 == 0;
        drop(data);

        if signal {
            self.data.sync.notify_one();
        }
    }

    pub fn op_error(&self, err: std::io::Error) {
        let mut data = self.data.pending_writes.lock();
        data.0 -= 1;
        if data.1.is_none() {
            data.1 = Some(err);
            drop(data);
            self.data.sync.notify_one();
        }
    }

    pub fn wait_all(&self) -> Result<()> {
        let mut data = self.data.pending_writes.lock();

        while data.0 > 0 && data.1.is_none() {
            self.data.sync.wait(&mut data);
        }

        if let Some(err) = data.1.take() {
            Err(err)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
pub struct AsyncOpSignalData {
    pending_writes: Mutex<(usize, Option<std::io::Error>)>,
    sync: Condvar,
}

#[cfg(test)]
#[macro_export]
macro_rules! io_test {
    ($name:ident, $test_body:block) => {
        #[cfg(test)]
        #[allow(unused_imports)]
        mod $name {
            use std::path::PathBuf;

            use $crate::io::{
                fs::StdFileManager, iobuffer::Backing, memory::MemoryFileManager, File, FileManager,
            };

            use super::*;

            #[allow(dead_code)]
            fn unique_file_path<Manager>() -> PathBuf {
                PathBuf::from(format!(
                    ".test-{}-{}.sediment",
                    stringify!($name),
                    std::any::type_name::<Manager>()
                        .rsplit_once("::")
                        .unwrap()
                        .1
                ))
            }
            #[allow(dead_code)]
            fn name_of<Manager>() -> &'static str {
                std::any::type_name::<Manager>()
                    .rsplit_once("::")
                    .unwrap()
                    .1
            }

            fn test<Manager: FileManager>() {
                $test_body
            }

            #[test]
            fn fs() {
                test::<StdFileManager>();
            }

            #[test]
            fn memory() {
                test::<MemoryFileManager>();
            }

            #[test]
            #[cfg(feature = "iouring")]
            fn uring() {
                test::<$crate::io::uring::UringFileManager>();
            }
        }
    };
}

pub trait WriteIoBuffer {
    fn write_all_at<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<IoBuffer<B>>,
        position: u64,
    ) -> std::io::Result<()>;
}

#[cfg(test)]
io_test!(basics, {
    let manager = Manager::default();
    let path = manager.resolve_path(unique_file_path::<Manager>());
    let mut file = manager.write(&path).unwrap();
    let data = b"hello, world".to_vec();
    let (result, _) = file.write_all(data, 0);
    result.unwrap();
    file.synchronize().unwrap();

    // Test overwriting.
    let data = b"new world".to_vec();
    // Replace "worl" with "new " (Overwrite without extend)
    let (result, data) = file.write_all(data.io_slice(..4), 7);
    result.unwrap();

    // Replace final "d" with "new world" (Overwrite with extend)
    let (result, _) = file.write_all(data, 11);
    result.unwrap();
    drop(file);

    let expected_result = b"hello, new new world";
    let mut file = manager.read(&path).unwrap();
    let buffer = vec![0; expected_result.len()];
    let (result, buffer) = file.read_exact(buffer.io_slice(..4), 0);
    result.unwrap();
    let (result, buffer) = file.read_exact(buffer.io_slice(4..expected_result.len()), 4);
    result.unwrap();
    assert_eq!(buffer, expected_result);
    drop(file);

    if path.as_ref().exists() {
        std::fs::remove_file(&path).unwrap();
    }
});

pub(crate) fn invalid_data_error<E>(message: E) -> std::io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    std::io::Error::new(ErrorKind::InvalidData, message)
}

trait IgnoreNotFoundError<T> {
    fn ignore_not_found(self) -> io::Result<T>;
}

impl<T> IgnoreNotFoundError<T> for io::Result<T>
where
    T: Default,
{
    fn ignore_not_found(self) -> io::Result<T> {
        match self {
            Ok(result) => Ok(result),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(T::default()),
            Err(err) => Err(err),
        }
    }
}
