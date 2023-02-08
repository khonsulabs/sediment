#![forbid(unsafe_code)]

use std::io::{self};
use std::num::TryFromIntError;
use std::path::Path;
use std::sync::{Arc, PoisonError};

use okaywal::file_manager::fs::StdFileManager;
use okaywal::file_manager::memory::MemoryFileManager;
use okaywal::file_manager::FSyncError;
use okaywal::{file_manager, WriteAheadLog};
pub use transaction::Transaction;

use crate::atlas::{Atlas, GrainReader};
use crate::checkpointer::Checkpointer;
use crate::commit_log::{CommitLogEntry, CommitLogs};
use crate::config::Config;
use crate::format::{GrainId, Stored, TransactionId};
use crate::store::Store;
use crate::transaction::TransactionLock;

mod allocations;
mod atlas;
mod basinmap;
mod checkpointer;
mod commit_log;
pub mod config;
pub mod format;
mod store;
#[cfg(test)]
mod tests;
mod transaction;
mod util;
mod wal;

#[derive(Debug, Clone)]
pub struct Database<FileManager = StdFileManager>
where
    FileManager: file_manager::FileManager,
{
    data: Arc<Data<FileManager>>,
    wal: WriteAheadLog<FileManager>,
}

impl Database<StdFileManager> {
    pub fn recover<AsRefPath: AsRef<Path>>(directory: AsRefPath) -> Result<Self> {
        Config::for_directory(directory).recover()
    }
}

impl Database<MemoryFileManager> {
    pub fn in_memory() -> Self {
        Config::in_memory()
            .recover()
            .expect("somehow failed to recover on default memory file manager")
    }
}

impl<FileManager> Database<FileManager>
where
    FileManager: file_manager::FileManager,
{
    fn recover_config(config: Config<FileManager>) -> Result<Self> {
        // Opening the store restores the database to the last fully committed
        // state. Each commit happens when the write ahead log is checkpointed.
        let store = Store::recover(
            config.wal.directory.as_ref(),
            config.wal.file_manager.clone(),
        )?;
        let atlas = Atlas::new(&store);
        let current_metadata = atlas.current_index_metadata()?;
        let (checkpointer, cp_spawner) = Checkpointer::new(current_metadata.checkpointed_to);
        let data = Arc::new(Data {
            store,
            atlas,
            tx_lock: TransactionLock::new(current_metadata),
            checkpointer,
            commit_logs: CommitLogs::default(),
        });

        // Recover any transactions from the write ahead log that haven't been
        // checkpointed to the store already.
        let wal = config.wal.open(wal::WalManager::new(&data))?;

        // The wal recovery process may have recovered sediment checkpoints that
        // are in the WAL but not yet in permanent storage. Refresh the metadata.
        let current_metadata = data.atlas.current_index_metadata()?;
        cp_spawner.spawn(current_metadata.checkpointed_to, &data, &wal)?;
        if current_metadata.checkpoint_target > current_metadata.checkpointed_to {
            data.checkpointer
                .checkpoint_to(current_metadata.checkpoint_target);
        }

        Ok(Self { data, wal })
    }

    pub fn begin_transaction(&self) -> Result<Transaction<'_, FileManager>> {
        let tx_guard = self.data.tx_lock.lock();
        let wal_entry = self.wal.begin_entry()?;

        Transaction::new(self, wal_entry, tx_guard)
    }

    pub fn read(&self, grain: GrainId) -> Result<Option<GrainReader<FileManager>>> {
        self.data.atlas.find(grain, &self.wal)
    }

    pub fn read_commit_log_entry(&self, grain: GrainId) -> Result<Option<Arc<CommitLogEntry>>> {
        self.data.commit_logs.get_or_lookup(grain, self)
    }

    pub fn shutdown(self) -> Result<()> {
        // Shut the checkpointer down first, since it may try to access the
        // write-ahead log.
        self.data.checkpointer.shutdown()?;
        // Shut down the write-ahead log, which may still end up having its own
        // checkpointing process finishing up. This may require the file syncer.
        self.wal.shutdown()?;
        // With everything else shut down, we can now shut down the file
        // manager.
        self.data.store.file_manager.shutdown()?;

        Ok(())
    }

    pub fn checkpoint_target(&self) -> Result<TransactionId> {
        Ok(self.data.atlas.current_index_metadata()?.checkpoint_target)
    }

    pub fn checkpointed_to(&self) -> Result<TransactionId> {
        Ok(self.data.atlas.current_index_metadata()?.checkpointed_to)
    }

    pub fn embedded_header(&self) -> Result<Option<GrainId>> {
        Ok(self
            .data
            .atlas
            .current_index_metadata()?
            .embedded_header_data)
    }

    pub fn commit_log_head(&self) -> Result<Option<Stored<Arc<CommitLogEntry>>>> {
        if let Some(entry_id) = self.data.atlas.current_index_metadata()?.commit_log_head {
            if let Some(entry) = self.read_commit_log_entry(entry_id)? {
                return Ok(Some(Stored {
                    grain_id: entry_id,
                    stored: entry,
                }));
            }
        }

        Ok(None)
    }
}

impl<FileManager> Eq for Database<FileManager> where FileManager: file_manager::FileManager {}

impl<FileManager> PartialEq for Database<FileManager>
where
    FileManager: file_manager::FileManager,
{
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
}

#[derive(Debug)]
struct Data<FileManager>
where
    FileManager: file_manager::FileManager,
{
    store: Store<FileManager>,
    checkpointer: Checkpointer,
    atlas: Atlas<FileManager>,
    commit_logs: CommitLogs,
    tx_lock: TransactionLock,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("a GrainId was used that was not allocated")]
    GrainNotAllocated,
    #[error("a poisoned lock was encountered, the database must be closed and reopened")]
    LockPoisoned,
    #[error("a thread was not able to be joined")]
    ThreadJoin,
    #[error("crc32 checksum mismatch")]
    ChecksumFailed,
    #[error("the value is too large to be stored in Sediment")]
    GrainTooLarge,
    #[error("an invalid grain id was encountered")]
    InvalidGrainId,
    #[error("the transaction id is not valid for this database")]
    InvalidTransactionId,
    #[error("value too large for target")]
    ValueOutOfBounds,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("the service has shut down")]
    Shutdown,
    #[error("database verification failed: {0}")]
    VerificationFailed(String),
}

impl Error {
    fn verification_failed(reason: impl Into<String>) -> Self {
        Self::VerificationFailed(reason.into())
    }
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

impl From<TryFromIntError> for Error {
    fn from(_: TryFromIntError) -> Self {
        Self::ValueOutOfBounds
    }
}

impl From<FSyncError> for Error {
    fn from(error: FSyncError) -> Self {
        match error {
            FSyncError::Shutdown => Self::Shutdown,
            FSyncError::ThreadJoin => Self::ThreadJoin,
            FSyncError::InternalInconstency => Self::LockPoisoned,
            FSyncError::Io(io) => Self::Io(io),
        }
    }
}
