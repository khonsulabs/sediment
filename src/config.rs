use std::path;

use okaywal::file_manager;
use okaywal::file_manager::fs::StdFileManager;
use okaywal::file_manager::memory::MemoryFileManager;

use crate::{Database, Result};

#[derive(Clone)]
pub struct Config<FileManager> {
    pub wal: okaywal::Configuration<FileManager>,
}

impl Config<StdFileManager> {
    pub fn for_directory<Path>(directory: Path) -> Self
    where
        Path: AsRef<path::Path>,
    {
        Self {
            wal: okaywal::Configuration::default_for(directory),
        }
    }
}

impl Config<MemoryFileManager> {
    pub fn in_memory() -> Self {
        Self {
            wal: okaywal::Configuration::default_with_manager("/", MemoryFileManager::default()),
        }
    }
}
impl<FileManager> Config<FileManager>
where
    FileManager: file_manager::FileManager,
{
    pub fn configure_wal<
        Configuration: FnOnce(okaywal::Configuration<FileManager>) -> okaywal::Configuration<FileManager>,
    >(
        mut self,
        configurator: Configuration,
    ) -> Self {
        self.wal = configurator(self.wal);
        self
    }

    pub fn recover(self) -> Result<Database<FileManager>> {
        Database::recover_config(self)
    }
}

impl<FileManager> From<okaywal::Configuration<FileManager>> for Config<FileManager> {
    fn from(wal: okaywal::Configuration<FileManager>) -> Self {
        Self { wal }
    }
}
