use std::{
    io::{self},
    num::TryFromIntError,
    path::Path,
    sync::{Arc, PoisonError},
};

use okaywal::WriteAheadLog;
pub use transaction::Transaction;

use crate::{
    atlas::{Atlas, GrainReader},
    commit_log::CommitLogEntry,
    config::Config,
    format::GrainId,
    store::Store,
};

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

    pub fn embedded_header(&self) -> Result<Option<GrainId>> {
        Ok(self
            .data
            .atlas
            .current_index_metadata()?
            .embedded_header_data)
    }

    pub fn commit_log_head(&self) -> Result<Option<CommitLogEntry>> {
        if let Some(entry_id) = self.data.atlas.current_index_metadata()?.commit_log_head {
            if let Some(mut reader) = self.read(entry_id)? {
                return CommitLogEntry::read_from(&mut reader).map(Some);
            }
        }

        Ok(None)
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
    let path = Path::new("test");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }

    let db = Database::recover(path).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    assert!(db.read(grain).unwrap().is_none());
    let tx_id = tx.commit().unwrap();

    let read_contents = db
        .read(grain)
        .unwrap()
        .expect("grain not found")
        .read_all_data()
        .unwrap();
    assert_eq!(read_contents, b"hello, world");

    let commit = db.commit_log_head().unwrap().expect("commit log missing");
    assert_eq!(commit.transaction_id, tx_id);
    assert_eq!(commit.new_grains.len(), 1);
    assert_eq!(commit.new_grains[0].id, grain);
    assert!(commit.freed_grains.is_empty());
    assert!(commit.archived_grains.is_empty());
    assert!(commit.next_entry(&db).unwrap().is_none());

    db.shutdown().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint() {
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
    let contents = db
        .read(grain)
        .unwrap()
        .expect("grain not found")
        .read_all_data()
        .unwrap();
    assert_eq!(contents, b"hello, world");

    db.shutdown().unwrap();

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn checkpoint_loop() {
    let path = Path::new(".test-checkpoint-loop");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }

    // Configure the WAL to checkpoint after 10 bytes -- "hello, world" is 12.
    let mut grains_written = Vec::new();
    for i in 0_usize..10 {
        let db = Config::for_directory(path)
            .configure_wal(|wal| wal.checkpoint_after_bytes(10))
            .recover()
            .unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let grain = tx.write(&i.to_be_bytes()).unwrap();
        assert!(db.read(grain).unwrap().is_none());
        grains_written.push(grain);
        tx.commit().unwrap();

        for (index, grain) in grains_written.iter().enumerate() {
            let contents = db
                .read(*grain)
                .unwrap()
                .expect("grain not found")
                .read_all_data()
                .unwrap();
            assert_eq!(contents, &index.to_be_bytes());
        }

        db.shutdown().unwrap();
    }

    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();
    for (index, grain) in grains_written.iter().enumerate() {
        let contents = db
            .read(*grain)
            .unwrap()
            .expect("grain not found")
            .read_all_data()
            .unwrap();
        assert_eq!(contents, &index.to_be_bytes());
    }

    // Verify the commit log is correct. The commit log head will contain the
    // addition of the most recent grain, and we should be able to iterate
    // backwards and find each grain in each entry.
    let mut grains_to_read = grains_written.iter().rev();
    let mut current_commit_log_entry = db.commit_log_head().unwrap();
    while let Some(commit_log_entry) = current_commit_log_entry {
        let expected_grain = grains_to_read.next().expect("too many commit log entries");
        assert_eq!(&commit_log_entry.new_grains[0].id, expected_grain);
        current_commit_log_entry = commit_log_entry.next_entry(&db).unwrap();
    }

    db.shutdown().unwrap();

    std::fs::remove_dir_all(path).unwrap();
}
