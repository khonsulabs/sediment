use crate::io::{
    self,
    fs::{StdAsyncFileWriter, StdFile, StdFileManager},
    iobuffer::{AnyBacking, Backing},
    memory::{MemoryFile, MemoryFileManager},
    AsyncFileWriter, File, WriteIoBuffer,
};

#[derive(Debug)]
pub enum AnyFile {
    Std(StdFile),
    Memory(MemoryFile),
}

impl io::File for AnyFile {
    type Manager = AnyFileManager;

    fn len(&self) -> std::io::Result<u64> {
        match self {
            AnyFile::Std(file) => file.len(),
            AnyFile::Memory(file) => file.len(),
        }
    }

    fn read_exact<B: Backing + Default + From<AnyBacking> + Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> io::BufferResult<(), B> {
        match self {
            AnyFile::Std(file) => file.read_exact(buffer, position),
            AnyFile::Memory(file) => file.read_exact(buffer, position),
        }
    }

    fn write_all<B: Backing + Default + From<AnyBacking> + Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> io::BufferResult<(), B> {
        match self {
            AnyFile::Std(file) => file.write_all(buffer, position),
            AnyFile::Memory(file) => file.write_all(buffer, position),
        }
    }

    fn synchronize(&mut self) -> std::io::Result<()> {
        match self {
            AnyFile::Std(file) => file.synchronize(),
            AnyFile::Memory(file) => file.synchronize(),
        }
    }

    fn set_length(&mut self, new_length: u64) -> std::io::Result<()> {
        match self {
            AnyFile::Std(file) => file.set_length(new_length),
            AnyFile::Memory(file) => file.set_length(new_length),
        }
    }
}

impl WriteIoBuffer for AnyFile {
    fn write_all_at<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> std::io::Result<()> {
        let (result, _) = self.write_all(buffer.into().map_any(), position);
        result
    }
}

#[derive(Debug, Clone)]
pub enum AnyFileManager {
    Std(StdFileManager),
    Memory(MemoryFileManager),
}

impl Default for AnyFileManager {
    fn default() -> Self {
        Self::new_file()
    }
}

impl io::FileManager for AnyFileManager {
    type File = AnyFile;
    type AsyncFile = AnyAsyncFileWriter;

    fn resolve_path(&self, path: impl AsRef<std::path::Path>) -> io::paths::PathId {
        match self {
            AnyFileManager::Std(manager) => manager.resolve_path(path),
            AnyFileManager::Memory(manager) => manager.resolve_path(path),
        }
    }

    fn read(&self, path: &io::paths::PathId) -> std::io::Result<Self::File> {
        match self {
            AnyFileManager::Std(manager) => manager.read(path).map(AnyFile::Std),
            AnyFileManager::Memory(manager) => manager.read(path).map(AnyFile::Memory),
        }
    }

    fn write(&self, path: &io::paths::PathId) -> std::io::Result<Self::File> {
        match self {
            AnyFileManager::Std(manager) => manager.write(path).map(AnyFile::Std),
            AnyFileManager::Memory(manager) => manager.write(path).map(AnyFile::Memory),
        }
    }

    fn write_async(&self, path: &io::paths::PathId) -> std::io::Result<Self::AsyncFile> {
        match self {
            AnyFileManager::Std(manager) => manager.write_async(path).map(AnyAsyncFileWriter::Std),
            AnyFileManager::Memory(manager) => {
                manager.write_async(path).map(AnyAsyncFileWriter::Memory)
            }
        }
    }

    fn synchronize(&self, path: &io::paths::PathId) -> std::io::Result<()> {
        match self {
            AnyFileManager::Std(manager) => manager.synchronize(path),
            AnyFileManager::Memory(manager) => manager.synchronize(path),
        }
    }

    fn delete(&self, path: &io::paths::PathId) -> std::io::Result<()> {
        match self {
            AnyFileManager::Std(manager) => manager.delete(path),
            AnyFileManager::Memory(manager) => manager.delete(path),
        }
    }

    fn delete_directory(&self, path: &std::path::Path) -> std::io::Result<()> {
        match self {
            AnyFileManager::Std(manager) => manager.delete_directory(path),
            AnyFileManager::Memory(manager) => manager.delete_directory(path),
        }
    }
}

impl AnyFileManager {
    #[must_use]
    pub fn new_file() -> Self {
        Self::Std(StdFileManager::default())
    }

    #[must_use]
    pub fn new_memory() -> Self {
        Self::Memory(MemoryFileManager::default())
    }
}

#[derive(Debug)]
pub enum AnyAsyncFileWriter {
    Std(StdAsyncFileWriter),
    Memory(MemoryFile),
}

impl AsyncFileWriter for AnyAsyncFileWriter {
    type Manager = AnyFileManager;

    fn background_write_all<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> std::io::Result<()> {
        match self {
            AnyAsyncFileWriter::Std(writer) => writer.background_write_all(buffer, position),
            AnyAsyncFileWriter::Memory(writer) => writer.background_write_all(buffer, position),
        }
    }

    fn wait(&mut self) -> std::io::Result<()> {
        match self {
            AnyAsyncFileWriter::Std(writer) => writer.wait(),
            AnyAsyncFileWriter::Memory(writer) => writer.wait(),
        }
    }
}

impl WriteIoBuffer for AnyAsyncFileWriter {
    fn write_all_at<B: Into<AnyBacking>>(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer<B>>,
        position: u64,
    ) -> std::io::Result<()> {
        self.background_write_all(buffer, position)
    }
}
