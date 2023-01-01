use std::{
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    sync::{Arc, Weak},
};

use okaywal::{LogManager, ReadChunkResult};

use crate::{
    format::{ByteUtil, GrainAllocationInfo, GrainId, TransactionId},
    store::BasinState,
    util::{u32_to_usize, usize_to_u32},
    Data as DatabaseData, Result,
};

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
            let mut index_metadata = database.atlas.current_index_metadata();
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
                            WalChunk::FinishTransaction {
                                commit_log_entry,
                                embedded_header,
                            } => {
                                index_metadata.commit_log_head = Some(commit_log_entry);
                                index_metadata.embedded_header_data = embedded_header;
                            }
                        }
                    }
                    ReadChunkResult::EndOfEntry => break,
                    ReadChunkResult::AbortedEntry => return Ok(()),
                }
            }

            database
                .atlas
                .note_grains_written(index_metadata, written_grains);
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: okaywal::EntryId,
        checkpointed_entries: &mut okaywal::SegmentReader,
    ) -> std::io::Result<()> {
        if let Some(database) = self.db.upgrade() {
            let latest_tx_id = TransactionId::from(last_checkpointed_id);
            let mut store = database.store.lock();
            let mut all_new_grains = Vec::new();
            let mut latest_commit_log_entry = store.index.active.commit_log_head;
            let mut latest_embedded_header_data = store.index.active.embedded_header_data;
            // We allocate the transaction grains vec once and reuse the vec to
            // avoid reallocating.
            let mut transaction_grains = Vec::new();
            while let Some(mut entry) = checkpointed_entries.read_entry()? {
                let checkpointed_tx = TransactionId::from(entry.id());
                // Because an entry could be aborted, we need to make sure we don't
                // modify our DiskState until after we've read every chunk. We will
                // write new grain data directly to the segments, but the headers
                // won't be updated until after the loop as well.
                transaction_grains.clear();
                while let Some(mut chunk) = match entry.read_chunk()? {
                    ReadChunkResult::Chunk(chunk) => Some(chunk),
                    ReadChunkResult::EndOfEntry => None,
                    ReadChunkResult::AbortedEntry => todo!(),
                } {
                    self.scratch.clear();
                    chunk.read_to_end(&mut self.scratch)?;
                    assert!(chunk.check_crc().expect("crc not validated")); // TODO make this an error
                    match WalChunk::read(&self.scratch)? {
                        WalChunk::NewGrainInWal { id, data } => {
                            let basin = store.basins.get_or_insert_with(id.basin_id(), || {
                                BasinState::default_for(id.basin_id())
                            });
                            let stratum = basin.get_or_allocate_stratum(
                                id.stratum_id(),
                                &database.store.directory,
                            );
                            let mut file = BufWriter::new(stratum.get_or_open_file()?);

                            // Write the grain data to disk.
                            file.seek(SeekFrom::Start(id.file_position()))?;
                            file.write_all(&checkpointed_tx.to_be_bytes())?;
                            file.write_all(&usize_to_u32(data.len())?.to_be_bytes())?;
                            file.write_all(data)?;
                            let crc32 = crc32c::crc32c(data);
                            file.write_all(&crc32.to_be_bytes())?;
                            file.flush()?;

                            transaction_grains.push(id);
                        }
                        WalChunk::ArchiveGrain(_) => todo!(),
                        WalChunk::FreeGrain(_) => todo!(),
                        WalChunk::FinishTransaction {
                            commit_log_entry,
                            embedded_header,
                        } => {
                            latest_commit_log_entry = Some(commit_log_entry);
                            latest_embedded_header_data = embedded_header;
                        }
                    }
                }

                all_new_grains.append(&mut transaction_grains);
            }

            // TODO allocate a grain for the new commit log entry and write the
            // commit log entry.

            all_new_grains.sort_unstable();

            while let Some(first_id) = all_new_grains.first().cloned() {
                let basin = store.basins.get_or_insert_with(first_id.basin_id(), || {
                    BasinState::default_for(first_id.basin_id())
                });
                let stratum =
                    basin.get_or_allocate_stratum(first_id.stratum_id(), &database.store.directory);

                // Update the stratum header for the disk state.
                loop {
                    // This is a messy match statement, but the goal is to only
                    // re-lookup basin and stratum when we jump to a new
                    // stratum.
                    match all_new_grains.first().copied() {
                        Some(id)
                            if id.basin_id() == first_id.basin_id()
                                && id.stratum_id() == first_id.stratum_id() =>
                        {
                            all_new_grains.pop();
                            for index in 0..id.grain_count() {
                                stratum.header.active.grains[usize::from(
                                    id.local_grain_index().as_u16(),
                                ) + usize::from(index)] =
                                    GrainAllocationInfo::allocated(id.grain_count() - index).0;
                            }
                        }
                        _ => break,
                    }
                }
                stratum.write_header(latest_tx_id)?;
            }

            store.index.active.commit_log_head = latest_commit_log_entry;
            store.index.active.embedded_header_data = latest_embedded_header_data;
            store.write_header(latest_tx_id)?;

            database.atlas.note_grains_checkpointed(&all_new_grains);

            Ok(())
        } else {
            todo!("error")
        }
    }
}

pub enum WalChunk<'a> {
    NewGrainInWal {
        id: GrainId,
        data: &'a [u8],
    },
    ArchiveGrain(GrainId),
    FreeGrain(GrainId),
    FinishTransaction {
        commit_log_entry: GrainId,
        embedded_header: Option<GrainId>,
    },
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
            3 => {
                if buffer.len() == 25 {
                    // TODO real error
                    let commit_log_entry = GrainId::from_bytes(&buffer[9..17]).unwrap();
                    let embedded_header = GrainId::from_bytes(&buffer[17..]);
                    Ok(Self::FinishTransaction {
                        commit_log_entry,
                        embedded_header,
                    })
                } else {
                    todo!("error: buffer incorrect length")
                }
            }
            _ => todo!("invalid chunk"),
        }
    }

    pub fn write_new_grain<W: Write>(grain_id: GrainId, data: &[u8], writer: &mut W) -> Result<()> {
        writer.write_all(&[0])?;
        writer.write_all(&grain_id.to_be_bytes())?;
        writer.write_all(data)?;
        Ok(())
    }

    pub fn write_transaction_tail<W: Write>(
        commit_log_entry_id: GrainId,
        embedded_header: Option<GrainId>,
        writer: &mut W,
    ) -> Result<()> {
        writer.write_all(&[3])?;
        writer.write_all(&commit_log_entry_id.to_be_bytes())?;
        writer.write_all(&embedded_header.to_be_bytes())?;
        Ok(())
    }
}
