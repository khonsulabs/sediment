use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    sync::Arc,
};

use parking_lot::Mutex;

use crate::io::{
    self,
    iobuffer::IoBuffer,
    paths::{PathId, PathIds},
    AsyncFileWriter, BufferResult, File, FileManager, WriteIoBuffer,
};

#[derive(Default, Debug, Clone)]
pub struct StdFileManager {
    path_ids: PathIds,
    thread_pool: Arc<Mutex<Option<flume::Sender<AsyncWrite>>>>,
}

impl FileManager for StdFileManager {
    type File = StdFile;
    type AsyncFile = StdAsyncFileWriter;
    fn resolve_path(&self, path: impl AsRef<std::path::Path>) -> super::PathId {
        self.path_ids.get_or_insert(path.as_ref())
    }

    fn read(&self, path: &super::PathId) -> io::Result<Self::File> {
        std::fs::File::open(path).map(|file| StdFile { file, location: 0 })
    }

    fn write(&self, path: &super::PathId) -> io::Result<Self::File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .map(|file| StdFile { file, location: 0 })
    }

    fn write_async(&self, path: &PathId) -> std::io::Result<Self::AsyncFile> {
        let (result_sender, result_receiver) = flume::unbounded();
        let mut thread_pool = self.thread_pool.lock();
        if thread_pool.is_none() {
            // TODO maybe this shouldn't be unbounded?
            let (op_sender, op_receiver) = flume::unbounded();

            spawn_async_writer(
                self.clone(),
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
}

#[derive(Debug)]
pub struct StdFile {
    file: std::fs::File,
    location: u64,
}

impl File for StdFile {
    type Manager = StdFileManager;

    fn len(&self) -> io::Result<u64> {
        self.file.metadata().map(|metadata| metadata.len())
    }

    fn read_at(
        &mut self,
        buffer: impl Into<super::iobuffer::IoBuffer>,
        position: u64,
    ) -> BufferResult<usize> {
        let mut buffer = buffer.into();
        if position != self.location {
            if let Err(err) = self.file.seek(SeekFrom::Start(position)) {
                return (Err(err), buffer.buffer);
            }
            self.location = position;
        }

        match self.file.read(&mut buffer) {
            Ok(bytes_read) => {
                self.location += u64::try_from(bytes_read).unwrap();
                (Ok(bytes_read), buffer.buffer)
            }
            Err(err) => (Err(err), buffer.buffer),
        }
    }

    fn write_at(
        &mut self,
        buffer: impl Into<super::iobuffer::IoBuffer>,
        position: u64,
    ) -> BufferResult<usize> {
        let buffer = buffer.into();
        if position != self.location {
            if let Err(err) = self.file.seek(SeekFrom::Start(position)) {
                return (Err(err), buffer.buffer);
            }
            self.location = position;
        }

        match self.file.write(&buffer) {
            Ok(bytes_written) => {
                self.location += u64::try_from(bytes_written).unwrap();
                (Ok(bytes_written), buffer.buffer)
            }
            Err(err) => (Err(err), buffer.buffer),
        }
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
        let result = unsafe { libc::fcntl(self.file.as_raw_fd(), F_BARRIERFSYNC) };
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
        self.file.sync_data()
    }

    fn set_length(&mut self, new_length: u64) -> io::Result<()> {
        self.file.set_len(new_length)
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
    op_sender: flume::Sender<AsyncWrite>,
    result_sender: flume::Sender<io::Result<()>>,
    result_receiver: flume::Receiver<io::Result<()>>,
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
            .send(AsyncWrite {
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
            self.result_receiver
                .recv()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::BrokenPipe, err))??;
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

#[derive(Debug)]
struct AsyncWrite {
    path: PathId,
    position: u64,
    buffer: IoBuffer,
    result_sender: flume::Sender<io::Result<()>>,
}

fn spawn_async_writer(
    manager: StdFileManager,
    op_receiver: flume::Receiver<AsyncWrite>,
    additional_thread_limit: usize,
) {
    std::thread::spawn(move || async_writer(&manager, &op_receiver, additional_thread_limit));
}

fn async_writer(
    manager: &StdFileManager,
    op_receiver: &flume::Receiver<AsyncWrite>,
    additional_thread_limit: usize,
) {
    let mut spawned_additional_thread = additional_thread_limit == 0;
    while let Ok(write) = op_receiver.recv() {
        if !spawned_additional_thread && !op_receiver.is_empty() {
            spawned_additional_thread = true;
            spawn_async_writer(
                manager.clone(),
                op_receiver.clone(),
                additional_thread_limit - 1,
            );
        }

        let result = async_write(manager, &write.path, write.buffer, write.position);
        drop(write.result_sender.send(result));
    }
}

fn async_write(
    manager: &StdFileManager,
    path: &PathId,
    buffer: IoBuffer,
    position: u64,
) -> io::Result<()> {
    let mut file = manager.write(path)?;
    let (result, _) = file.write_all(buffer, position);
    result
}
