use std::{
    io::{Read, Write},
    sync::{Arc, Weak},
};

use okaywal::{LogManager, ReadChunkResult};

use crate::{format::GrainId, util::u32_to_usize, Data as DatabaseData, Result};

#[derive(Debug)]
pub struct Manager {
    db: Weak<DatabaseData>,
    scratch: Vec<u8>,
}

impl Manager {
    pub(super) fn new(database: &Arc<DatabaseData>) -> Self {
        Self {
            db: Arc::downgrade(database),
            scratch: Vec::new(),
        }
    }
}

impl LogManager for Manager {
    fn recover(&mut self, entry: &mut okaywal::Entry<'_>) -> Result<()> {
        if let Some(database) = self.db.upgrade() {
            let mut written_grains = Vec::new();
            loop {
                match entry.read_chunk()? {
                    ReadChunkResult::Chunk(mut chunk) => {
                        let position = chunk.log_position();
                        self.scratch
                            .resize(u32_to_usize(chunk.bytes_remaining())?, 0);
                        chunk.read_exact(&mut self.scratch)?;

                        let chunk = WalChunk::read(&self.scratch)?;
                        match chunk {
                            WalChunk::NewGrainInWal { id, .. } => {
                                written_grains.push((id, position))
                            }
                            WalChunk::ArchiveGrain(_) => {
                                todo!("process grain archivals in the wal")
                            }
                            WalChunk::FreeGrain(_) => todo!("process grain freeing in the wal"),
                        }
                    }
                    ReadChunkResult::EndOfEntry => break,
                    ReadChunkResult::AbortedEntry => return Ok(()),
                }
            }

            database.atlas.note_grains_written(written_grains);
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: okaywal::EntryId,
        checkpointed_entries: &mut okaywal::SegmentReader,
    ) -> std::io::Result<()> {
        todo!("copy changes from wal to the disk state")
    }
}

pub enum WalChunk<'a> {
    NewGrainInWal { id: GrainId, data: &'a [u8] },
    ArchiveGrain(GrainId),
    FreeGrain(GrainId),
}

impl<'a> WalChunk<'a> {
    pub fn read(buffer: &'a [u8]) -> Result<Self> {
        if buffer.len() < 9 {
            todo!("error: buffer too short")
        }
        let kind = buffer[0];
        let id = GrainId::from_bytes(&buffer[1..9]).unwrap(); // TODO real error
        match kind {
            0 => Ok(Self::NewGrainInWal {
                id,
                data: &buffer[9..],
            }),
            1 => todo!("archive grain"),
            2 => todo!("free grain"),
            _ => todo!("invalid chunk"),
        }
    }

    pub fn write_new_grain<W: Write>(grain_id: GrainId, data: &[u8], writer: &mut W) -> Result<()> {
        writer.write_all(&[0])?;
        writer.write_all(&grain_id.to_bytes())?;
        writer.write_all(data)?;
        Ok(())
    }
}
