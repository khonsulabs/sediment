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
    embedded_header_data: Option<GrainId>,
    log_entry: CommitLogEntry,
    scratch: Vec<u8>,
}

impl<'db> Transaction<'db> {
    pub(super) fn new(database: &'db Database, entry: EntryWriter<'db>) -> Result<Self> {
        let index = database.data.atlas.current_index_metadata()?;
        Ok(Self {
            database,
            written_grains: Vec::new(),
            embedded_header_data: index.embedded_header_data,
            log_entry: CommitLogEntry::new(TransactionId::from(entry.id()), index.commit_log_head),
            entry: Some(entry),
            scratch: Vec::new(),
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

    pub fn commit(mut self) -> Result<TransactionId> {
        // Write the commit log entry
        let mut log_entry_bytes = Vec::new();
        self.log_entry.serialize_to(&mut log_entry_bytes)?;
        let new_commit_log_head = self.write(&log_entry_bytes)?;

        let entry = self.entry.take().expect("entry missing");

        // Write the transaction tail
        self.scratch.clear();
        WalChunk::write_transaction_tail(
            new_commit_log_head,
            self.embedded_header_data,
            &mut self.scratch,
        )?;

        let transaction_id = TransactionId::from(entry.commit()?);

        let new_metadata = IndexMetadata {
            embedded_header_data: self.embedded_header_data,
            commit_log_head: Some(new_commit_log_head),
        };

        self.database
            .data
            .atlas
            .note_grains_written(new_metadata, self.written_grains.drain(..))?;

        Ok(transaction_id)
    }

    pub fn set_embedded_header(&mut self, new_header: Option<GrainId>) {
        self.embedded_header_data = new_header;
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
