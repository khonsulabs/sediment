use std::path::{self};

use crate::{
    database::Database,
    io::{self, fs::StdFileManager, memory::MemoryFileManager},
};

#[derive(Debug)]
#[must_use]
pub struct Configuration<Manager, Path, Recovered>
where
    Manager: io::FileManager,
    Path: AsRef<std::path::Path>,
    Recovered: RecoveryCallback<Manager::File>,
{
    manager: Manager,
    path: Path,
    recovered_callback: Recovered,
}

impl<Manager, Path, Recovered> Configuration<Manager, Path, Recovered>
where
    Manager: io::FileManager,
    Path: AsRef<path::Path>,
    Recovered: RecoveryCallback<Manager::File>,
{
    pub fn open(self) -> io::Result<Database<Manager::File>> {
        Database::new(self.path, &self.manager, self.recovered_callback)
    }
}

impl<Manager, Path> Configuration<Manager, Path, ()>
where
    Manager: io::FileManager,
    Path: AsRef<path::Path>,
{
    pub fn new(manager: Manager, path: Path) -> Self {
        Self {
            manager,
            path,
            recovered_callback: (),
        }
    }
}

impl<Path> Configuration<StdFileManager, Path, ()>
where
    Path: AsRef<path::Path>,
{
    pub fn for_file(path: Path) -> Self {
        Self {
            manager: StdFileManager::default(),
            path,
            recovered_callback: (),
        }
    }
}

impl Configuration<MemoryFileManager, &'static path::Path, ()> {
    pub fn in_memory() -> Self {
        Self {
            manager: MemoryFileManager::default(),
            path: path::Path::new("db.sediment"),
            recovered_callback: (),
        }
    }
}

impl<Manager, Path> Configuration<Manager, Path, ()>
where
    Manager: io::FileManager,
    Path: AsRef<path::Path>,
{
    pub fn when_recovered<Recovered>(
        self,
        recovered: Recovered,
    ) -> Configuration<Manager, Path, Recovered>
    where
        Recovered: RecoveryCallback<Manager::File>,
    {
        Configuration {
            manager: self.manager,
            path: self.path,
            recovered_callback: recovered,
        }
    }
}

pub trait RecoveryCallback<File> {
    fn recovered(self, database: &mut Database<File>, error: std::io::Error) -> io::Result<()>;
}

impl<File> RecoveryCallback<File> for () {
    fn recovered(self, _database: &mut Database<File>, error: std::io::Error) -> io::Result<()> {
        eprintln!("Database recovered from a failure to read the most recent commit: {error}");
        Ok(())
    }
}

impl<T, File> RecoveryCallback<File> for T
where
    T: FnOnce(&mut Database<File>, std::io::Error) -> io::Result<()>,
{
    fn recovered(self, database: &mut Database<File>, error: std::io::Error) -> io::Result<()> {
        self(database, error)
    }
}
