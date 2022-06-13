use crate::io::{
    self,
    fs::{StdFile, StdFileManager},
    memory::{MemoryFile, MemoryFileManager},
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

    fn read_at(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> io::BufferResult<usize> {
        match self {
            AnyFile::Std(file) => file.read_at(buffer, position),
            AnyFile::Memory(file) => file.read_at(buffer, position),
        }
    }

    fn write_at(
        &mut self,
        buffer: impl Into<io::iobuffer::IoBuffer>,
        position: u64,
    ) -> io::BufferResult<usize> {
        match self {
            AnyFile::Std(file) => file.write_at(buffer, position),
            AnyFile::Memory(file) => file.write_at(buffer, position),
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
