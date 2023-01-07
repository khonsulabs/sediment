use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};

use crc32c::crc32c;
use okaywal::{EntryWriter, LogPosition};

use crate::{
    atlas::IndexMetadata,
    commit_log::{CommitLogEntry, NewGrain},
    format::{GrainId, TransactionId},
    util::usize_to_u32,
    wal::WalChunk,
    Database, Result,
};

#[derive(Debug)]
pub struct Transaction<'db> {
    database: &'db Database,
    entry: Option<EntryWriter<'db>>,
    guard: Option<TransactionGuard>,
    state: Option<CommittingTransaction>,
}

impl<'db> Transaction<'db> {
    pub(super) fn new(
        database: &'db Database,
        entry: EntryWriter<'db>,
        guard: TransactionGuard,
    ) -> Result<Self> {
        let metadata = guard.current_index_metadata();
        Ok(Self {
            database,
            state: Some(CommittingTransaction {
                metadata,
                written_grains: Vec::new(),
                log_entry: CommitLogEntry::new(
                    TransactionId::from(entry.id()),
                    metadata.commit_log_head,
                    metadata.embedded_header_data,
                    metadata.checkpoint_target,
                    metadata.checkpointed_to,
                ),
            }),

            entry: Some(entry),
            guard: Some(guard),
        })
    }

    pub fn write(&mut self, data: &[u8]) -> Result<GrainId> {
        let data_length = usize_to_u32(data.len())?;
        let grain_id = self.database.data.atlas.reserve(data_length)?;

        let entry = self.entry.as_mut().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::new_grain_length(data_length))?;
        WalChunk::write_new_grain(grain_id, data, &mut chunk)?;
        let record = chunk.finish()?;

        let state = self.state.as_mut().expect("state missing");
        state.written_grains.push((grain_id, record.position));
        state.log_entry.new_grains.push(NewGrain {
            id: grain_id,
            crc32: crc32c(data),
        });
        Ok(grain_id)
    }

    pub fn archive(&mut self, grain: GrainId) -> Result<()> {
        self.database.data.atlas.check_grain_validity(grain)?;

        let entry = self.entry.as_mut().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_archive_grain(grain, &mut chunk)?;
        chunk.finish()?;

        let state = self.state.as_mut().expect("state missing");
        state.log_entry.archived_grains.push(grain);

        Ok(())
    }

    pub(crate) fn free_grains(&mut self, grains: &[GrainId]) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        for grain in grains {
            let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
            WalChunk::write_free_grain(*grain, &mut chunk)?;
            chunk.finish()?;
        }

        let state = self.state.as_mut().expect("state missing");
        state.log_entry.freed_grains.extend(grains.iter().copied());

        Ok(())
    }

    #[allow(clippy::drop_ref)]
    pub fn set_embedded_header(&mut self, new_header: Option<GrainId>) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_embedded_header_update(new_header, &mut chunk)?;
        chunk.finish()?;

        let mut state = self.state.as_mut().expect("state missing");
        if let Some(old_header) = state.log_entry.embedded_header_data {
            drop(state);
            self.archive(old_header)?;
            state = self.state.as_mut().expect("state missing");
        }

        state.log_entry.embedded_header_data = new_header;

        Ok(())
    }

    pub fn checkpoint_to(&mut self, tx_id: TransactionId) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        let mut state = self.state.as_mut().expect("state missing");
        if tx_id <= state.log_entry.checkpoint_target {
            // already the checkpoint target
            return Ok(());
        } else if tx_id >= entry.id() {
            todo!("checkpointing too new of a transaction")
        }

        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_checkpoint_to(tx_id, &mut chunk)?;
        chunk.finish()?;

        state.log_entry.checkpoint_target = tx_id;

        Ok(())
    }

    pub(crate) fn checkpointed_to(&mut self, tx_id: TransactionId) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        let mut state = self.state.as_mut().expect("state missing");
        if tx_id <= state.log_entry.checkpointed_to {
            // already the checkpoint target
            return Ok(());
        } else if tx_id >= entry.id() {
            todo!("checkpointing too new of a transaction")
        }

        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_checkpointed_to(tx_id, &mut chunk)?;
        chunk.finish()?;

        state.log_entry.checkpointed_to = tx_id;

        Ok(())
    }

    #[allow(clippy::drop_ref)]
    pub fn commit(mut self) -> Result<TransactionId> {
        let state = self.state.as_mut().expect("state missing");
        // Write the commit log entry
        state.log_entry.freed_grains.sort_unstable();
        let mut log_entry_bytes = Vec::new();
        state.log_entry.serialize_to(&mut log_entry_bytes)?;
        drop(state);
        let new_commit_log_head = self.write(&log_entry_bytes)?;

        let mut state = self.state.take().expect("state missing");
        state.metadata.commit_log_head = Some(new_commit_log_head);

        // Write the transaction tail
        let mut entry = self.entry.take().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_transaction_tail(new_commit_log_head, &mut chunk)?;
        chunk.finish()?;

        let guard = self.guard.take().expect("tx guard missing");

        let transaction_id = state.log_entry.transaction_id;
        let finalizer = guard.stage(state, self.database);

        entry.commit()?;

        finalizer.finalize()?;

        Ok(transaction_id)
    }

    pub fn rollback(mut self) -> Result<()> {
        self.rollback_transaction()
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        let mut state = self.state.take().expect("state missing");
        let entry = self.entry.take().expect("entry missing");

        let result = entry.rollback();

        self.database
            .data
            .atlas
            .rollback_grains(state.written_grains.drain(..).map(|(g, _)| g))?;

        result?;

        Ok(())
    }
}

impl<'db> Drop for Transaction<'db> {
    fn drop(&mut self) {
        if self.entry.is_some() {
            self.rollback_transaction()
                .expect("error rolling back transaction");
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionLock {
    data: Arc<TransactionLockData>,
}

impl TransactionLock {
    pub fn new(initial_metadata: IndexMetadata) -> Self {
        Self {
            data: Arc::new(TransactionLockData {
                tx_lock: Mutex::new(TransactionState::new(initial_metadata)),
                tx_sync: Condvar::new(),
            }),
        }
    }

    pub(super) fn lock(&self) -> TransactionGuard {
        let mut state = self.data.tx_lock.lock().expect("can't panick");

        // Wait for the locked status to be relinquished
        while state.in_transaction {
            state = self.data.tx_sync.wait(state).expect("can't panick");
        }

        // Acquire the locked status
        state.in_transaction = true;

        // Return the guard
        TransactionGuard { lock: self.clone() }
    }
}

#[derive(Debug)]
struct TransactionLockData {
    tx_lock: Mutex<TransactionState>,
    tx_sync: Condvar,
}

/// Ensures only one thread has access to begin a transaction at any given time.
///
/// This guard ensures that no two threads try to update some of the in-memory
/// state at the same time. The Write-Ahead Log always ensures only one thread
/// can write to it already, but we need extra guarantees because we don't want
/// to publish some state until after the WAL has confirmed its commit.
#[derive(Debug)]
pub(super) struct TransactionGuard {
    lock: TransactionLock,
}

impl TransactionGuard {
    pub fn current_index_metadata(&self) -> IndexMetadata {
        let state = self.lock.data.tx_lock.lock().expect("cannot panic");
        state.metadata
    }

    pub(super) fn stage(
        self,
        tx: CommittingTransaction,
        db: &'_ Database,
    ) -> TransactionFinalizer<'_> {
        let id = tx.log_entry.transaction_id;
        let mut state = self.lock.data.tx_lock.lock().expect("cannot panic");
        state.metadata = tx.metadata;
        state.committing_transactions.push_back(tx);

        TransactionFinalizer {
            db,
            lock: self.lock.clone(),
            id,
        }
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        // Reset the locked status
        let mut state = self.lock.data.tx_lock.lock().expect("can't panick");
        state.in_transaction = false;
        drop(state);

        // Notify the next waiter.
        self.lock.data.tx_sync.notify_one();
    }
}

#[derive(Debug)]
struct TransactionState {
    in_transaction: bool,
    metadata: IndexMetadata,
    committing_transactions: VecDeque<CommittingTransaction>,
}

impl TransactionState {
    pub fn new(initial_metadata: IndexMetadata) -> Self {
        Self {
            in_transaction: false,
            metadata: initial_metadata,
            committing_transactions: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
pub(super) struct CommittingTransaction {
    metadata: IndexMetadata,
    written_grains: Vec<(GrainId, LogPosition)>,
    log_entry: CommitLogEntry,
}

#[derive(Debug)]
pub(super) struct TransactionFinalizer<'a> {
    db: &'a Database,
    lock: TransactionLock,
    id: TransactionId,
}

impl<'a> TransactionFinalizer<'a> {
    pub fn finalize(self) -> Result<()> {
        let mut state = self.lock.data.tx_lock.lock().expect("can't panic");

        while state
            .committing_transactions
            .front()
            .map_or(false, |tx| tx.log_entry.transaction_id <= self.id)
        {
            let mut tx_to_commit = state
                .committing_transactions
                .pop_front()
                .expect("just checked");
            self.db.data.atlas.note_transaction_committed(
                tx_to_commit.metadata,
                tx_to_commit.written_grains.drain(..),
                &tx_to_commit.log_entry.freed_grains,
                false,
            )?;
            self.db.data.commit_logs.cache(
                tx_to_commit
                    .metadata
                    .commit_log_head
                    .expect("commit log must be present"),
                Arc::new(tx_to_commit.log_entry),
            )?;
        }

        Ok(())
    }
}
