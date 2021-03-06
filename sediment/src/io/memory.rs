use std::{collections::HashMap, io::ErrorKind, sync::Arc};

use parking_lot::{Mutex, RwLock};

use crate::io::{
    self,
    iobuffer::{AnyBacking, Backing},
    paths::PathIds,
    AsyncFileWriter, File, FileManager, WriteIoBuffer,
};

#[derive(Debug, Default, Clone)]
pub struct MemoryFileManager {
    path_ids: PathIds,
    files: Arc<Mutex<HashMap<u64, MemoryFile>>>,
}

impl FileManager for MemoryFileManager {
    type File = MemoryFile;
    type AsyncFile = MemoryFile;

    fn resolve_path(&self, path: impl AsRef<std::path::Path>) -> super::paths::PathId {
        self.path_ids.get_or_insert(path.as_ref())
    }

    fn read(&self, path: &super::paths::PathId) -> io::Result<Self::File> {
        let files = self.files.lock();

        files
            .get(&path.id)
            .cloned()
            .ok_or_else(|| std::io::Error::from(ErrorKind::NotFound))
    }

    fn write(&self, path: &super::paths::PathId) -> io::Result<Self::File> {
        let mut files = self.files.lock();
        Ok(files.entry(path.id).or_default().clone())
    }

    fn write_async(&self, path: &io::paths::PathId) -> std::io::Result<Self::AsyncFile> {
        self.write(path)
    }

    fn synchronize(&self, _path: &io::paths::PathId) -> std::io::Result<()> {
        Ok(())
    }

    fn delete(&self, path: &io::paths::PathId) -> std::io::Result<()> {
        let mut files = self.files.lock();
        files.remove(&path.id);
        Ok(())
    }

    fn delete_directory(&self, path: &std::path::Path) -> std::io::Result<()> {
        let mut files = self.files.lock();
        for path in self.path_ids.paths_with_prefix(path) {
            files.remove(&path.id);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryFile(Arc<RwLock<Vec<u8>>>);

impl File for MemoryFile {
    type Manager = MemoryFileManager;

    fn len(&self) -> io::Result<u64> {
        let file = self.0.read();
        Ok(u64::try_from(file.len()).unwrap())
    }

    fn read_exact<B: Backing + Default + From<AnyBacking> + Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<super::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> super::BufferResult<(), B> {
        let file = self.0.read();
        let mut buffer = buffer.into();
        let position = match usize::try_from(position) {
            Ok(position) => position,
            Err(err) => {
                return (
                    Err(std::io::Error::new(ErrorKind::Other, err)),
                    buffer.buffer,
                );
            }
        };
        let length_after_position = file.len().saturating_sub(position);
        if length_after_position == 0 {
            (Ok(()), buffer.buffer)
        } else {
            let buffer_slice = &mut *buffer;
            let bytes_to_read = buffer_slice.len().min(length_after_position);
            buffer_slice[..bytes_to_read]
                .copy_from_slice(&file[position..position + bytes_to_read]);
            (Ok(()), buffer.buffer)
        }
    }

    fn write_all<B: Backing + Default + From<AnyBacking> + Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<super::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> super::BufferResult<(), B> {
        let mut file = self.0.write();
        let buffer = buffer.into();
        let position = match usize::try_from(position) {
            Ok(position) => position,
            Err(err) => {
                return (
                    Err(std::io::Error::new(ErrorKind::Other, err)),
                    buffer.buffer,
                );
            }
        };
        let buffer_slice = &*buffer;
        let length_after_position = file.len().saturating_sub(position);
        if length_after_position < buffer_slice.len() {
            // The wrote will extend past the buffer
            file.truncate(position);
            file.extend(buffer_slice.iter().copied());
        } else {
            // Overwrite existing data
            file[position..position + buffer_slice.len()].copy_from_slice(buffer_slice);
        }

        (Ok(()), buffer.buffer)
    }

    fn synchronize(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn set_length(&mut self, new_length: u64) -> io::Result<()> {
        let mut file = self.0.write();
        let new_length = match usize::try_from(new_length) {
            Ok(new_length) => new_length,
            Err(err) => return Err(std::io::Error::new(ErrorKind::Other, err)),
        };
        file.resize(new_length, 0);
        Ok(())
    }
}

impl WriteIoBuffer for MemoryFile {
    fn write_all_at<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> std::io::Result<()> {
        let (result, _) = self.write_all(buffer.into().map_any(), position);
        result
    }
}

impl AsyncFileWriter for MemoryFile {
    type Manager = MemoryFileManager;

    fn background_write_all<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> std::io::Result<()> {
        let (result, _) = self.write_all(buffer.into().map_any(), position);
        result
    }

    fn wait(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
