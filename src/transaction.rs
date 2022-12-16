use okaywal::{EntryWriter, LogPosition};

use crate::{
    format::{GrainId, TransactionId},
    util::usize_to_u32,
    wal::WalChunk,
    Database, Result,
};

#[derive(Debug)]
pub struct Transaction<'db> {
    database: &'db Database,
    entry: EntryWriter<'db>,
    written_grains: Vec<(GrainId, LogPosition)>,
    scratch: Vec<u8>,
}

impl<'db> Transaction<'db> {
    pub(super) fn new(database: &'db Database, entry: EntryWriter<'db>) -> Self {
        Self {
            database,
            entry,
            written_grains: Vec::new(),
            scratch: Vec::new(),
        }
    }

    pub fn write(&mut self, data: &[u8]) -> Result<GrainId> {
        let data_length = usize_to_u32(data.len())?;
        let grain_id = self.database.data.atlas.reserve(data_length)?;
        self.scratch.clear();
        // TODO this shouldn't require an extra buffer -- okaywal should allow
        // serializing directly to its own buffered writer.
        WalChunk::write_new_grain(grain_id, data, &mut self.scratch)?;
        let record = self.entry.write_chunk(&self.scratch)?;
        self.written_grains.push((grain_id, record.position));
        Ok(grain_id)
    }

    pub fn commit(self) -> Result<TransactionId> {
        let transaction_id = TransactionId::from(self.entry.commit()?);

        self.database
            .data
            .atlas
            .note_grains_written(self.written_grains);

        Ok(transaction_id)
    }

    pub fn rollback(self) -> Result<()> {
        let result = self.entry.rollback();

        self.database
            .data
            .atlas
            .rollback_grains(self.written_grains.into_iter().map(|(g, _)| g));

        result
    }
}
