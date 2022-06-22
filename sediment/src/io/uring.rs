use std::{
    collections::{HashMap, VecDeque},
    io::ErrorKind,
    sync::Arc,
};

use parking_lot::Mutex;
use tokio_uring::buf::IoBuf;

use crate::io::{
    self, fs::StdFileManager, iobuffer::IoBuffer, paths::PathId, AsyncFileWriter, AsyncOpParams,
    BufferResult, File, FileManager, WriteIoBuffer,
};

#[derive(Debug)]
pub struct UringFile {
    path: PathId,
    result_sender: flume::Sender<BufferResult<()>>,
    result_receiver: flume::Receiver<BufferResult<()>>,
    manager: UringFileManager,
}

impl io::File for UringFile {
    type Manager = UringFileManager;

    fn len(&self) -> std::io::Result<u64> {
        let file = self.manager.std.write(&self.path)?;
        file.len()
    }

    fn read_exact(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> io::BufferResult<()> {
        match self.manager.op_sender.send(AsyncOp {
            op: Op::Read,
            params: AsyncOpParams {
                path: self.path.clone(),
                position,
                buffer: buffer.into(),
                result_sender: self.result_sender.clone(),
            },
        }) {
            Ok(_) => match self.result_receiver.recv() {
                Ok(result) => result,
                Err(disconnected) => (
                    Err(std::io::Error::new(ErrorKind::BrokenPipe, disconnected)),
                    Vec::new(),
                ),
            },
            Err(disconnected) => (
                Err(std::io::Error::new(ErrorKind::BrokenPipe, disconnected)),
                Vec::new(),
            ),
        }
        // .block_on(async {
        //     {
        //         let buffer = buffer.into();

        //         let (result, buf) = if let Some(range) = buffer.range {
        //             self.file
        //                 .read_at(buffer.buffer.slice(range), position)
        //                 .await
        //         } else {
        //             self.file.read_at(buffer.buffer.slice(..), position).await
        //         };

        //         (result, buf.into_inner())
        //     }
        // })
    }

    fn write_all(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> io::BufferResult<()> {
        match self.manager.op_sender.send(AsyncOp {
            op: Op::Write,
            params: AsyncOpParams {
                path: self.path.clone(),
                position,
                buffer: buffer.into(),
                result_sender: self.result_sender.clone(),
            },
        }) {
            Ok(_) => match self.result_receiver.recv() {
                Ok(result) => result,
                Err(disconnected) => (
                    Err(std::io::Error::new(ErrorKind::BrokenPipe, disconnected)),
                    Vec::new(),
                ),
            },
            Err(disconnected) => (
                Err(std::io::Error::new(ErrorKind::BrokenPipe, disconnected)),
                Vec::new(),
            ),
        }
        // self.manager.runtime.block_on(async {
        //     {
        //         let buffer = buffer.into();

        //         let (result, buf) = if let Some(range) = buffer.range {
        //             self.file
        //                 .write_at(buffer.buffer.slice(range), position)
        //                 .await
        //         } else {
        //             self.file.write_at(buffer.buffer.slice(..), position).await
        //         };

        //         (result, buf.into_inner())
        //     }
        // })
    }

    fn synchronize(&mut self) -> std::io::Result<()> {
        self.manager
            .op_sender
            .send(AsyncOp {
                op: Op::Synchronize,
                params: AsyncOpParams {
                    path: self.path.clone(),
                    position: 0,
                    buffer: Vec::new().into(),
                    result_sender: self.result_sender.clone(),
                },
            })
            .map_err(|err| std::io::Error::new(ErrorKind::BrokenPipe, err))?;

        self.result_receiver
            .recv()
            .map(|_| ())
            .map_err(|err| std::io::Error::new(ErrorKind::BrokenPipe, err))
    }

    fn set_length(&mut self, new_length: u64) -> std::io::Result<()> {
        let mut file = self.manager.std.write(&self.path)?;
        file.set_length(new_length)
    }
}

impl WriteIoBuffer for UringFile {
    fn write_all_at(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> std::io::Result<()> {
        let (result, _) = self.write_all(buffer, position);
        result
    }
}

#[derive(Debug, Clone)]
pub struct UringFileManager {
    std: StdFileManager,
    op_sender: flume::Sender<AsyncOp>,
}

impl Default for UringFileManager {
    fn default() -> Self {
        let (task_sender, task_receiver) = flume::bounded(1);
        std::thread::spawn(move || uring_thread(task_receiver));
        Self {
            std: StdFileManager::default(),
            op_sender: task_sender,
        }
    }
}

impl io::FileManager for UringFileManager {
    type File = UringFile;

    type AsyncFile = AsyncUringFile;

    fn resolve_path(&self, path: impl AsRef<std::path::Path>) -> io::paths::PathId {
        self.std.resolve_path(path)
    }

    fn read(&self, path: &io::paths::PathId) -> std::io::Result<Self::File> {
        let (result_sender, result_receiver) = flume::unbounded();
        Ok(UringFile {
            path: path.clone(),
            manager: self.clone(),
            result_sender,
            result_receiver,
        })
    }

    fn write(&self, path: &io::paths::PathId) -> std::io::Result<Self::File> {
        let (result_sender, result_receiver) = flume::unbounded();
        Ok(UringFile {
            path: path.clone(),
            manager: self.clone(),
            result_sender,
            result_receiver,
        })
    }

    fn write_async(&self, path: &io::paths::PathId) -> std::io::Result<Self::AsyncFile> {
        let (result_sender, result_receiver) = flume::unbounded();
        Ok(AsyncUringFile {
            path: path.clone(),
            manager: self.clone(),
            result_sender,
            result_receiver,
            operations_sent: 0,
        })
    }
}

fn uring_thread(ops: flume::Receiver<AsyncOp>) {
    tokio_uring::start(async move {
        let local = tokio::task::LocalSet::new();
        let open_files = OpenFiles::default();
        local
            .run_until(async {
                while let Ok(op) = ops.recv_async().await {
                    let open_files = open_files.clone();
                    local.spawn_local(async move {
                        let result = match op.op {
                            Op::Write => {
                                perform_async_write_all(
                                    op.params.buffer,
                                    op.params.position,
                                    op.params.path,
                                    &open_files,
                                )
                                .await
                            }
                            Op::Read => {
                                perform_async_read_all(
                                    op.params.buffer,
                                    op.params.position,
                                    op.params.path,
                                    &open_files,
                                )
                                .await
                            }
                            Op::Synchronize => {
                                perform_async_sync(op.params.path, &open_files).await
                            }
                        };
                        drop(op.params.result_sender.send(result));
                    });
                }
            })
            .await;
    });
}

#[derive(Debug)]
pub struct AsyncUringFile {
    path: PathId,
    manager: UringFileManager,
    result_sender: flume::Sender<BufferResult<()>>,
    result_receiver: flume::Receiver<BufferResult<()>>,
    operations_sent: usize,
}

impl io::AsyncFileWriter for AsyncUringFile {
    type Manager = UringFileManager;

    fn background_write_all(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> std::io::Result<()> {
        let path = self.path.clone();
        let buffer: IoBuffer = buffer.into();
        self.manager
            .op_sender
            .send(AsyncOp {
                op: Op::Write,
                params: AsyncOpParams {
                    path,
                    position,
                    buffer,
                    result_sender: self.result_sender.clone(),
                },
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

impl WriteIoBuffer for AsyncUringFile {
    fn write_all_at(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> std::io::Result<()> {
        self.background_write_all(buffer, position)
    }
}

struct AsyncOp {
    op: Op,
    params: AsyncOpParams,
}

enum Op {
    Write,
    Read,
    Synchronize,
}

async fn perform_async_write_all(
    buffer: IoBuffer,
    mut position: u64,
    path: PathId,
    open_files: &OpenFiles,
) -> (io::Result<()>, Vec<u8>) {
    let file = match open_files.open(&path).await {
        Ok(file) => file,
        Err(err) => return (Err(err), buffer.buffer),
    };
    let mut buffer = if let Some(range) = buffer.range {
        buffer.buffer.slice(range)
    } else {
        let end = buffer.buffer.len();
        if end == 0 {
            return (Ok(()), buffer.buffer);
        }

        buffer.buffer.slice(0..end)
    };
    while buffer.len() > 0 {
        let (result, returned_buffer) = file.write_at(buffer, position).await;

        let bytes_written = match result {
            Ok(bytes_written) => bytes_written,
            Err(err) => return (Err(err), returned_buffer.into_inner()),
        };
        buffer = returned_buffer;
        let start = buffer.begin();
        let end = buffer.end();
        let new_start = start.saturating_add(bytes_written).min(end);
        if new_start == end {
            break;
        }

        buffer = buffer.into_inner().slice(new_start..end);
        position += u64::try_from(bytes_written).unwrap();
    }
    open_files.return_file(&path, file);

    (Ok(()), buffer.into_inner())
}

async fn perform_async_read_all(
    buffer: IoBuffer,
    mut position: u64,
    path: PathId,
    open_files: &OpenFiles,
) -> (io::Result<()>, Vec<u8>) {
    let file = match open_files.open(&path).await {
        Ok(file) => file,
        Err(err) => return (Err(err), buffer.buffer),
    };
    let mut buffer = if let Some(range) = buffer.range {
        buffer.buffer.slice(range)
    } else {
        let end = buffer.buffer.len();
        if end == 0 {
            return (Ok(()), buffer.buffer);
        }
        buffer.buffer.slice(0..end)
    };
    while buffer.len() > 0 {
        let (result, returned_buffer) = file.read_at(buffer, position).await;
        buffer = returned_buffer;

        let bytes_read = match result {
            Ok(bytes_read) => bytes_read,
            Err(err) => return (Err(err), buffer.into_inner()),
        };
        let start = buffer.begin();
        let end = buffer.end();
        let new_start = start.saturating_add(bytes_read).min(end);
        if new_start == end {
            break;
        }

        buffer = buffer.into_inner().slice(new_start..end);
        position += u64::try_from(bytes_read).unwrap();
    }
    open_files.return_file(&path, file);

    (Ok(()), buffer.into_inner())
}

async fn perform_async_sync(path: PathId, open_files: &OpenFiles) -> (io::Result<()>, Vec<u8>) {
    let file = match open_files.open(&path).await {
        Ok(file) => file,
        Err(err) => return (Err(err), Vec::new()),
    };
    let result = file.sync_data().await;
    open_files.return_file(&path, file);
    (result, Vec::new())
}

#[derive(Clone, Default)]
struct OpenFiles(Arc<Mutex<HashMap<u64, VecDeque<tokio_uring::fs::File>>>>);

impl OpenFiles {
    async fn open(&self, path: &PathId) -> io::Result<tokio_uring::fs::File> {
        let file = {
            let mut files = self.0.lock();
            let open_files_for_path = files.entry(path.id).or_default();
            open_files_for_path.pop_front()
        };

        if let Some(file) = file {
            Ok(file)
        } else {
            tokio_uring::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .await
        }
    }

    fn return_file(&self, path: &PathId, file: tokio_uring::fs::File) {
        let mut files = self.0.lock();
        files.get_mut(&path.id).unwrap().push_front(file);
    }
}
