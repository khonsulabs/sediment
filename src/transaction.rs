use std::sync::{Arc, Condvar, Mutex};

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
    written_grains: Vec<(GrainId, LogPosition)>,
    log_entry: CommitLogEntry,
    _guard: TransactionGuard,
}

impl<'db> Transaction<'db> {
    pub(super) fn new(
        database: &'db Database,
        entry: EntryWriter<'db>,
        guard: TransactionGuard,
    ) -> Result<Self> {
        let index_metadata = database.data.atlas.current_index_metadata()?;
        Ok(Self {
            database,
            written_grains: Vec::new(),
            log_entry: CommitLogEntry::new(
                TransactionId::from(entry.id()),
                index_metadata.commit_log_head,
                index_metadata.embedded_header_data,
                index_metadata.checkpoint_target,
                index_metadata.checkpointed_to,
            ),
            entry: Some(entry),
            _guard: guard,
        })
    }

    pub fn write(&mut self, data: &[u8]) -> Result<GrainId> {
        let data_length = usize_to_u32(data.len())?;
        let grain_id = self.database.data.atlas.reserve(data_length)?;

        let entry = self.entry.as_mut().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::new_grain_length(data_length))?;
        WalChunk::write_new_grain(grain_id, data, &mut chunk)?;
        let record = chunk.finish()?;

        self.written_grains.push((grain_id, record.position));
        self.log_entry.new_grains.push(NewGrain {
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

        self.log_entry.archived_grains.push(grain);

        Ok(())
    }

    pub(crate) fn free_grains(&mut self, grains: &[GrainId]) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        for grain in grains {
            let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
            WalChunk::write_free_grain(*grain, &mut chunk)?;
            chunk.finish()?;
        }

        self.log_entry.freed_grains.extend(grains.iter().copied());

        Ok(())
    }

    pub fn set_embedded_header(&mut self, new_header: Option<GrainId>) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_embedded_header_update(new_header, &mut chunk)?;
        chunk.finish()?;

        if let Some(old_header) = self.log_entry.embedded_header_data {
            self.archive(old_header)?;
        }

        self.log_entry.embedded_header_data = new_header;

        Ok(())
    }

    pub fn checkpoint_to(&mut self, tx_id: TransactionId) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        if tx_id <= self.log_entry.checkpoint_target {
            // already the checkpoint target
            return Ok(());
        } else if tx_id >= entry.id() {
            todo!("checkpointing too new of a transaction")
        }

        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_checkpoint_to(tx_id, &mut chunk)?;
        chunk.finish()?;

        self.log_entry.checkpoint_target = tx_id;

        Ok(())
    }

    pub(crate) fn checkpointed_to(&mut self, tx_id: TransactionId) -> Result<()> {
        let entry = self.entry.as_mut().expect("entry missing");
        if tx_id <= self.log_entry.checkpointed_to {
            // already the checkpoint target
            return Ok(());
        } else if tx_id >= entry.id() {
            todo!("checkpointing too new of a transaction")
        }

        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_checkpointed_to(tx_id, &mut chunk)?;
        chunk.finish()?;

        self.log_entry.checkpointed_to = tx_id;

        Ok(())
    }

    pub fn commit(mut self) -> Result<TransactionId> {
        // Write the commit log entry
        self.log_entry.freed_grains.sort_unstable();
        let mut log_entry_bytes = Vec::new();
        self.log_entry.serialize_to(&mut log_entry_bytes)?;
        let new_commit_log_head = self.write(&log_entry_bytes)?;

        let new_metadata = IndexMetadata {
            embedded_header_data: self.log_entry.embedded_header_data,
            commit_log_head: Some(new_commit_log_head),
            checkpoint_target: self.log_entry.checkpoint_target,
            checkpointed_to: self.log_entry.checkpointed_to,
        };

        // Write the transaction tail
        let mut entry = self.entry.take().expect("entry missing");
        let mut chunk = entry.begin_chunk(WalChunk::COMMAND_LENGTH)?;
        WalChunk::write_transaction_tail(new_commit_log_head, &mut chunk)?;
        chunk.finish()?;

        let transaction_id = TransactionId::from(entry.commit()?);

        self.database.data.atlas.note_transaction_committed(
            new_metadata,
            self.written_grains.drain(..),
            &self.log_entry.freed_grains,
            false,
        )?;

        Ok(transaction_id)
    }

    pub fn rollback(mut self) -> Result<()> {
        self.rollback_transaction()
    }

    fn rollback_transaction(&mut self) -> Result<()> {
        let entry = self.entry.take().expect("entry missing");

        let result = entry.rollback();

        self.database
            .data
            .atlas
            .rollback_grains(self.written_grains.drain(..).map(|(g, _)| g))?;

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

#[derive(Debug, Default, Clone)]
pub struct TransactionLock {
    data: Arc<TransactionLockData>,
}

impl TransactionLock {
    pub fn lock(&self) -> TransactionGuard {
        let mut locked = self.data.tx_lock.lock().expect("can't panick");

        // Wait for the locked status to be relinquished
        while *locked {
            locked = self.data.tx_sync.wait(locked).expect("can't panick");
        }

        // Acquire the locked status
        *locked = true;

        // Return the guard
        TransactionGuard(self.clone())
    }
}

#[derive(Debug, Default)]
struct TransactionLockData {
    tx_lock: Arc<Mutex<bool>>,
    tx_sync: Condvar,
}

/// Ensures only one thread has access to begin a transaction at any given time.
///
/// This guard ensures that no two threads try to update some of the in-memory
/// state at the same time. The Write-Ahead Log always ensures only one thread
/// can write to it already, but we need extra guarantees because we don't want
/// to publish some state until after the WAL has confirmed its commit.
#[derive(Debug)]
pub struct TransactionGuard(TransactionLock);

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        // Reset the locked status
        let mut locked = self.0.data.tx_lock.lock().expect("can't panick");
        *locked = false;
        drop(locked);

        // Notify the next waiter.
        self.0.data.tx_sync.notify_one();
    }
}
