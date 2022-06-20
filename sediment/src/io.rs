use std::{
    io::{self, ErrorKind},
    path::Path,
};

pub use io::Result;

use crate::io::{iobuffer::IoBuffer, paths::PathId};

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

pub trait FileManager: Default + Clone {
    type File: File<Manager = Self>;
    type AsyncFile: AsyncFileWriter<Manager = Self>;

    fn resolve_path(&self, path: impl AsRef<Path>) -> PathId;

    fn read(&self, path: &PathId) -> io::Result<Self::File>;
    fn write(&self, path: &PathId) -> io::Result<Self::File>;
    fn write_async(&self, path: &PathId) -> io::Result<Self::AsyncFile>;
}

pub trait File: WriteIoBuffer {
    type Manager: FileManager<File = Self>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|len| len == 0)
    }

    fn read_exact(&mut self, buffer: impl Into<IoBuffer>, position: u64) -> BufferResult<()>;

    fn write_all(&mut self, buffer: impl Into<IoBuffer>, position: u64) -> BufferResult<()>;

    fn synchronize(&mut self) -> io::Result<()>;
    fn set_length(&mut self, new_length: u64) -> io::Result<()>;
}

pub trait AsyncFileWriter: WriteIoBuffer {
    type Manager: FileManager<AsyncFile = Self>;

    fn background_write_all(
        &mut self,
        buffer: impl Into<IoBuffer>,
        position: u64,
    ) -> io::Result<()>;

    fn wait(&mut self) -> io::Result<()>;
}

pub type BufferResult<T> = (io::Result<T>, Vec<u8>);

#[derive(Debug)]
pub struct AsyncOpParams {
    path: PathId,
    position: u64,
    buffer: IoBuffer,
    result_sender: flume::Sender<BufferResult<()>>,
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
                fs::StdFileManager, iobuffer::IoBufferExt, memory::MemoryFileManager, File,
                FileManager,
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
    fn write_all_at(&mut self, buffer: impl Into<IoBuffer>, position: u64) -> std::io::Result<()>;
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
