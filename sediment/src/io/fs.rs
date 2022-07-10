use std::{
    collections::{HashMap, VecDeque},
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;

use crate::io::{
    self,
    iobuffer::IoBuffer,
    paths::{PathId, PathIds},
    AsyncFileWriter, AsyncOpParams, BufferResult, File, FileManager, WriteIoBuffer,
};

#[derive(Default, Debug, Clone)]
pub struct StdFileManager {
    data: Arc<Data>,
}

#[derive(Default, Debug)]
struct Data {
    path_ids: PathIds,
    thread_pool: Mutex<Option<flume::Sender<AsyncOpParams>>>,
    open_files: OpenFiles,
}

type OpenFiles = Arc<Mutex<HashMap<u64, VecDeque<(std::fs::File, u64)>>>>;

impl FileManager for StdFileManager {
    type File = StdFile;
    type AsyncFile = StdAsyncFileWriter;
    fn resolve_path(&self, path: impl AsRef<std::path::Path>) -> super::PathId {
        self.data.path_ids.get_or_insert(path.as_ref())
    }

    fn read(&self, path: &super::PathId) -> io::Result<Self::File> {
        self.write(path)
    }

    fn write(&self, path: &super::PathId) -> io::Result<Self::File> {
        let mut open_files = self.data.open_files.lock();
        let open_files_for_path = open_files.entry(path.id).or_default();
        let (file, location) = if let Some((file, location)) = open_files_for_path.pop_front() {
            (file, location)
        } else {
            (
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?,
                0,
            )
        };
        Ok(StdFile {
            file: Some(file),
            path_id: path.id,
            manager: self.clone(),
            location,
        })
    }

    fn write_async(&self, path: &PathId) -> std::io::Result<Self::AsyncFile> {
        let (result_sender, result_receiver) = flume::unbounded();
        let mut thread_pool = self.data.thread_pool.lock();
        if thread_pool.is_none() {
            // TODO maybe this shouldn't be unbounded?
            let (op_sender, op_receiver) = flume::unbounded();

            spawn_async_writer(
                Arc::downgrade(&self.data),
                op_receiver,
                std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
            );

            *thread_pool = Some(op_sender);
        }

        Ok(StdAsyncFileWriter {
            path: path.clone(),
            op_sender: thread_pool.as_ref().cloned().unwrap(),
            result_sender,
            result_receiver,
            operations_sent: 0,
        })
    }

    fn synchronize(&self, path: &io::paths::PathId) -> std::io::Result<()> {
        let file = std::fs::File::open(path)?;
        file.sync_data()
    }

    fn delete(&self, path: &io::paths::PathId) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }
}

#[derive(Debug)]
pub struct StdFile {
    file: Option<std::fs::File>,
    location: u64,
    path_id: u64,
    manager: StdFileManager,
}

impl StdFile {
    fn file(&self) -> &std::fs::File {
        self.file.as_ref().expect("only cleared during drop")
    }
}

impl File for StdFile {
    type Manager = StdFileManager;

    fn len(&self) -> io::Result<u64> {
        self.file().metadata().map(|metadata| metadata.len())
    }

    fn read_exact(
        &mut self,
        buffer: impl Into<super::iobuffer::IoBuffer>,
        position: u64,
    ) -> BufferResult<()> {
        let mut buffer = buffer.into();
        if position != self.location {
            if let Err(err) = self.file().seek(SeekFrom::Start(position)) {
                return (Err(err), buffer.buffer);
            }
            self.location = position;
        }

        while !buffer.is_empty() {
            match self.file().read(&mut buffer) {
                Ok(bytes_read) => {
                    self.location += u64::try_from(bytes_read).unwrap();
                    buffer.advance_by(bytes_read);
                }
                Err(err) => return (Err(err), buffer.buffer),
            }
        }

        (Ok(()), buffer.buffer)
    }

    fn write_all(
        &mut self,
        buffer: impl Into<super::iobuffer::IoBuffer>,
        position: u64,
    ) -> BufferResult<()> {
        let mut buffer = buffer.into();
        if position != self.location {
            if let Err(err) = self.file().seek(SeekFrom::Start(position)) {
                return (Err(err), buffer.buffer);
            }
            self.location = position;
        }

        while !buffer.is_empty() {
            match self.file().write(&buffer) {
                Ok(bytes_written) => {
                    self.location += u64::try_from(bytes_written).unwrap();
                    buffer.advance_by(bytes_written);
                }
                Err(err) => return (Err(err), buffer.buffer),
            }
        }

        (Ok(()), buffer.buffer)
    }

    #[cfg(all(
        feature = "fbarrier-fsync",
        any(target_os = "macos", target_os = "ios")
    ))]
    #[allow(unsafe_code)]
    fn synchronize(&mut self) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        const F_BARRIERFSYNC: i32 = 85;

        // SAFETY: As long as the underlying File has a valid descriptor, this
        // should always be safe to call.
        let result = unsafe {
            libc::fcntl(
                self.file.as_ref().expect("already dropped").as_raw_fd(),
                F_BARRIERFSYNC,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            // It is unsafe to try to resume from a failed fsync call, because
            // the OS may have an inconsistent state with regards to what the
            // disk has flushed and what it hasn't.
            Err(std::io::Error::last_os_error())
        }
    }

    #[cfg(not(all(
        feature = "fbarrier-fsync",
        any(target_os = "macos", target_os = "ios")
    )))]
    fn synchronize(&mut self) -> io::Result<()> {
        self.file().sync_data()
    }

    fn set_length(&mut self, new_length: u64) -> io::Result<()> {
        self.file().set_len(new_length)
    }
}

impl Drop for StdFile {
    fn drop(&mut self) {
        let mut open_files = self.manager.data.open_files.lock();
        let open_files_for_path = open_files.entry(self.path_id).or_default();
        open_files_for_path.push_front((self.file.take().expect("already dropped"), self.location));
    }
}

impl WriteIoBuffer for StdFile {
    fn write_all_at(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> std::io::Result<()> {
        let (result, _) = self.write_all(buffer, position);
        result
    }
}

#[derive(Debug)]
pub struct StdAsyncFileWriter {
    path: PathId,
    op_sender: flume::Sender<AsyncOpParams>,
    result_sender: flume::Sender<BufferResult<()>>,
    result_receiver: flume::Receiver<BufferResult<()>>,
    operations_sent: usize,
}

impl AsyncFileWriter for StdAsyncFileWriter {
    type Manager = StdFileManager;

    fn background_write_all(
        &mut self,
        buffer: impl Into<IoBuffer>,
        position: u64,
    ) -> std::io::Result<()> {
        self.op_sender
            .send(AsyncOpParams {
                path: self.path.clone(),
                buffer: buffer.into(),
                result_sender: self.result_sender.clone(),
                position,
            })
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::BrokenPipe, err))?;
        self.operations_sent += 1;
        Ok(())
    }

    fn wait(&mut self) -> std::io::Result<()> {
        for _ in 0..self.operations_sent {
            let (result, _) = self
                .result_receiver
                .recv()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::BrokenPipe, err))?;
            result?;
        }
        Ok(())
    }
}

impl WriteIoBuffer for StdAsyncFileWriter {
    fn write_all_at(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> std::io::Result<()> {
        self.background_write_all(buffer, position)
    }
}

fn spawn_async_writer(
    manager: Weak<Data>,
    op_receiver: flume::Receiver<AsyncOpParams>,
    additional_thread_limit: usize,
) {
    std::thread::spawn(move || async_writer(&manager, &op_receiver, additional_thread_limit));
}

fn async_writer(
    weak_manager: &Weak<Data>,
    op_receiver: &flume::Receiver<AsyncOpParams>,
    additional_thread_limit: usize,
) {
    let mut spawned_additional_thread = additional_thread_limit == 0;
    while let (Ok(write), Some(manager)) = (op_receiver.recv(), weak_manager.upgrade()) {
        let manager = StdFileManager { data: manager };
        if !spawned_additional_thread && !op_receiver.is_empty() {
            spawned_additional_thread = true;
            spawn_async_writer(
                weak_manager.clone(),
                op_receiver.clone(),
                additional_thread_limit - 1,
            );
        }

        let result = async_write(&manager, &write.path, write.buffer, write.position);
        drop(write.result_sender.send(result));
    }
}

fn async_write(
    manager: &StdFileManager,
    path: &PathId,
    buffer: IoBuffer,
    position: u64,
) -> BufferResult<()> {
    let mut file = match manager.write(path) {
        Ok(file) => file,
        Err(err) => return (Err(err), buffer.buffer),
    };
    file.write_all(buffer, position)
}
