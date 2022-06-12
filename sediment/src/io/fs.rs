use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::io::{self, paths::PathIds, BufferResult, File, FileManager};

#[derive(Default, Debug, Clone)]
pub struct StdFileManager {
    path_ids: PathIds,
}

impl FileManager for StdFileManager {
    type File = StdFile;
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
        if result != 0 {
            // It is unsafe to try to resume from a failed fsync call, because
            // the OS may have an inconsistent state with regards to what the
            // disk has flushed and what it hasn't.
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
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
