use std::{
    io::{self},
    num::TryFromIntError,
    path::Path,
    sync::{Arc, PoisonError},
};

use okaywal::WriteAheadLog;

use crate::{
    atlas::{Atlas, GrainReader},
    config::Config,
    format::GrainId,
    store::Store,
};

pub use transaction::Transaction;

mod allocations;
mod atlas;
mod basinmap;
mod commit_log;
pub mod config;
pub mod format;
mod fsync;
mod store;
mod transaction;
mod util;
mod wal;

#[derive(Debug, Clone)]
pub struct Database {
    data: Arc<Data>,
    wal: WriteAheadLog,
}

impl Database {
    pub fn recover<AsRefPath: AsRef<Path>>(directory: AsRefPath) -> Result<Self> {
        Config::for_directory(directory).recover()
    }

    fn recover_config(config: Config) -> Result<Self> {
        // Opening the store restores the database to the last fully committed
        // state. Each commit happens when the write ahead log is checkpointed.
        let store = Store::recover(config.wal.directory.as_ref())?;
        let atlas = Atlas::new(&store);
        let data = Arc::new(Data { store, atlas });
        // Recover any transactions from the write ahead log that haven't been
        // checkpointed to the store already.
        let wal = config.wal.open(wal::Manager::new(&data))?;

        Ok(Self { data, wal })
    }

    pub fn begin_transaction(&self) -> Result<Transaction<'_>> {
        let wal_entry = self.wal.begin_entry()?;

        Transaction::new(self, wal_entry)
    }

    pub fn read(&self, grain: GrainId) -> Result<Option<GrainReader>> {
        self.data.atlas.find(grain, &self.wal)
    }

    pub fn shutdown(self) -> Result<()> {
        self.wal.shutdown()?;
        self.data.store.syncer.shutdown()?;

        Ok(())
    }
}

impl Eq for Database {}

impl PartialEq for Database {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
}

#[derive(Debug)]
struct Data {
    store: Store,
    atlas: Atlas,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("a poisoned lock was encountered, the database must be closed and reopened")]
    LockPoisoned,
    #[error("a thread was not able to be joined")]
    ThreadJoin,
    #[error("crc32 checksum mismatch")]
    ChecksumFailed,
    #[error("value too large for target: {0}")]
    ValueOutOfBounds(#[from] TryFromIntError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("the service has shut down")]
    Shutdown,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(err) => err,
            other => io::Error::new(io::ErrorKind::Other, other),
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::LockPoisoned
    }
}

#[test]
fn basic() {
    use std::io::Read;
    let path = Path::new("test");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }

    let db = Database::recover(path).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    assert!(db.read(grain).unwrap().is_none());
    tx.commit().unwrap();

    let mut reader = db.read(grain).unwrap().expect("grain not found");
    let mut contents = Vec::new();
    reader.read_to_end(&mut contents).unwrap();
    assert_eq!(contents, b"hello, world");

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint() {
    use std::io::Read;
    let path = Path::new(".test-checkpoint");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }

    // Configure the WAL to checkpoint after 10 bytes -- "hello, world" is 12.
    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    assert!(db.read(grain).unwrap().is_none());
    tx.commit().unwrap();
    db.shutdown().unwrap();

    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();
    let mut reader = db.read(grain).unwrap().expect("grain not found");
    let mut contents = Vec::new();
    reader.read_to_end(&mut contents).unwrap();
    assert_eq!(contents, b"hello, world");
    drop(reader);

    db.shutdown().unwrap();

    std::fs::remove_dir_all(path).unwrap();
}
