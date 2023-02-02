use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Weak};

use okaywal::{EntryId, LogManager, ReadChunkResult, WriteAheadLog};

use crate::format::{ByteUtil, GrainAllocationInfo, GrainAllocationStatus, GrainId, TransactionId};
use crate::store::BasinState;
use crate::util::{u32_to_usize, usize_to_u32};
use crate::{Data as DatabaseData, Database, Error, Result};

#[derive(Debug)]
pub struct WalManager {
    db: Weak<DatabaseData>,
    scratch: Vec<u8>,
}

impl WalManager {
    pub(super) fn new(database: &Arc<DatabaseData>) -> Self {
        Self {
            db: Arc::downgrade(database),
            scratch: Vec::new(),
        }
    }
}

impl LogManager for WalManager {
    fn recover(&mut self, entry: &mut okaywal::Entry<'_>) -> io::Result<()> {
        if let Some(database) = self.db.upgrade() {
            let mut written_grains = Vec::new();
            let mut freed_grains = Vec::new();
            let mut index_metadata = database.atlas.current_index_metadata()?;
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
                            WalChunk::FinishTransaction { commit_log_entry } => {
                                index_metadata.commit_log_head = Some(commit_log_entry);
                            }
                            WalChunk::UpdatedEmbeddedHeader(header) => {
                                index_metadata.embedded_header_data = header;
                            }
                            WalChunk::CheckpointTo(tx_id) => {
                                index_metadata.checkpoint_target = tx_id;
                            }
                            WalChunk::CheckpointedTo(tx_id) => {
                                index_metadata.checkpointed_to = tx_id;
                            }
                            WalChunk::FreeGrain(id) => {
                                freed_grains.push(id);
                            }
                            // Archiving a grain doesn't have any effect on the in-memory state
                            WalChunk::ArchiveGrain(_) => {}
                        }
                    }
                    ReadChunkResult::EndOfEntry => break,
                    ReadChunkResult::AbortedEntry => return Ok(()),
                }
            }

            freed_grains.sort_unstable();

            database.atlas.note_transaction_committed(
                index_metadata,
                written_grains,
                &freed_grains,
                true,
            )?;
        }

        Ok(())
    }

    fn checkpoint_to(
        &mut self,
        last_checkpointed_id: okaywal::EntryId,
        checkpointed_entries: &mut okaywal::SegmentReader,
        wal: &WriteAheadLog,
    ) -> std::io::Result<()> {
        if let Some(database) = self.db.upgrade() {
            let database = Database {
                data: database,
                wal: wal.clone(),
            };
            let fsyncs = database.data.store.syncer.new_batch()?;
            let latest_tx_id = TransactionId::from(last_checkpointed_id);
            let mut store = database.data.store.lock()?;
            let mut needs_directory_sync = store.needs_directory_sync;
            let mut all_changed_grains = Vec::new();
            let mut latest_commit_log_entry = store.index.active.commit_log_head;
            let mut latest_embedded_header_data = store.index.active.embedded_header_data;
            let mut latest_checkpoint_target = store.index.active.checkpoint_target;
            let mut latest_checkpointed_to = store.index.active.checkpointed_to;
            // We allocate the transaction grains vec once and reuse the vec to
            // avoid reallocating.
            let mut transaction_grains = Vec::new();
            'entry_loop: while let Some(mut entry) = checkpointed_entries.read_entry()? {
                let checkpointed_tx = TransactionId::from(entry.id());
                // Because an entry could be aborted, we need to make sure we don't
                // modify our DiskState until after we've read every chunk. We will
                // write new grain data directly to the segments, but the headers
                // won't be updated until after the loop as well.
                transaction_grains.clear();
                while let Some(mut chunk) = match entry.read_chunk()? {
                    ReadChunkResult::Chunk(chunk) => Some(chunk),
                    ReadChunkResult::EndOfEntry => None,
                    ReadChunkResult::AbortedEntry => continue 'entry_loop,
                } {
                    self.scratch.clear();
                    chunk.read_to_end(&mut self.scratch)?;
                    if !chunk.check_crc()? {
                        return Err(Error::ChecksumFailed.into());
                    }

                    match WalChunk::read(&self.scratch)? {
                        WalChunk::NewGrainInWal { id, data } => {
                            let basin = store.basins.get_or_insert_with(id.basin_id(), || {
                                BasinState::default_for(id.basin_id())
                            });
                            let stratum = basin.get_or_allocate_stratum(
                                id.stratum_id(),
                                &database.data.store.directory,
                            );
                            let mut file = BufWriter::new(
                                stratum.get_or_open_file(&mut needs_directory_sync)?,
                            );

                            // Write the grain data to disk.
                            let file_position = id.file_position();
                            file.seek(SeekFrom::Start(file_position))?;
                            file.write_all(&checkpointed_tx.to_be_bytes())?;
                            file.write_all(&usize_to_u32(data.len())?.to_be_bytes())?;
                            file.write_all(data)?;
                            let crc32 = crc32c::crc32c(data);
                            file.write_all(&crc32.to_be_bytes())?;
                            file.flush()?;

                            transaction_grains.push((id, GrainAllocationStatus::Allocated));
                        }
                        WalChunk::ArchiveGrain(id) => {
                            transaction_grains.push((id, GrainAllocationStatus::Archived));
                        }
                        WalChunk::FreeGrain(id) => {
                            transaction_grains.push((id, GrainAllocationStatus::Free));
                        }
                        WalChunk::FinishTransaction { commit_log_entry } => {
                            latest_commit_log_entry = Some(commit_log_entry);
                        }
                        WalChunk::UpdatedEmbeddedHeader(header) => {
                            latest_embedded_header_data = header;
                        }
                        WalChunk::CheckpointTo(tx_id) => {
                            latest_checkpoint_target = tx_id;
                        }
                        WalChunk::CheckpointedTo(tx_id) => {
                            latest_checkpointed_to = tx_id;
                        }
                    }
                }

                all_changed_grains.append(&mut transaction_grains);
            }

            all_changed_grains.sort_unstable();

            let mut index = 0;
            while let Some((first_id, _)) = all_changed_grains.get(index).cloned() {
                let basin = store.basins.get_or_insert_with(first_id.basin_id(), || {
                    BasinState::default_for(first_id.basin_id())
                });
                let stratum = basin
                    .get_or_allocate_stratum(first_id.stratum_id(), &database.data.store.directory);

                // Update the stratum header for the disk state.
                loop {
                    // This is a messy match statement, but the goal is to only
                    // re-lookup basin and stratum when we jump to a new
                    // stratum.
                    match all_changed_grains.get(index).copied() {
                        Some((id, status))
                            if id.basin_id() == first_id.basin_id()
                                && id.stratum_id() == first_id.stratum_id() =>
                        {
                            let local_index = usize::from(id.local_grain_index().as_u16());
                            if status == GrainAllocationStatus::Free {
                                // Free grains are just 0s.
                                stratum.header.active.grains
                                    [local_index..local_index + usize::from(id.grain_count())]
                                    .fill(0);
                            } else {
                                for index in 0..id.grain_count() {
                                    let status = if status == GrainAllocationStatus::Allocated {
                                        GrainAllocationInfo::allocated(id.grain_count() - index)
                                    } else {
                                        GrainAllocationInfo::archived(id.grain_count() - index)
                                    };
                                    stratum.header.active.grains
                                        [local_index + usize::from(index)] = status.0;
                                }
                            }
                        }
                        _ => break,
                    }
                    index += 1;
                }
                stratum.write_header(latest_tx_id, &fsyncs)?;
            }

            store.index.active.commit_log_head = latest_commit_log_entry;
            store.index.active.embedded_header_data = latest_embedded_header_data;
            store.index.active.checkpoint_target = latest_checkpoint_target;
            store.index.active.checkpointed_to = latest_checkpointed_to;

            store.write_header(latest_tx_id, &fsyncs)?;

            if needs_directory_sync {
                store.needs_directory_sync = false;
                fsyncs.queue_fsync_all(store.directory.try_clone()?)?;
            }

            fsyncs.wait_all()?;

            database
                .data
                .atlas
                .note_grains_checkpointed(&all_changed_grains)?;

            database
                .data
                .checkpointer
                .checkpoint_to(latest_checkpoint_target);

            Ok(())
        } else {
            todo!("error")
        }
    }
}

pub enum WalChunk<'a> {
    NewGrainInWal { id: GrainId, data: &'a [u8] },
    ArchiveGrain(GrainId),
    FreeGrain(GrainId),
    UpdatedEmbeddedHeader(Option<GrainId>),
    CheckpointTo(TransactionId),
    CheckpointedTo(TransactionId),
    FinishTransaction { commit_log_entry: GrainId },
}

impl<'a> WalChunk<'a> {
    pub const COMMAND_LENGTH: u32 = 9;
    pub const COMMAND_LENGTH_USIZE: usize = Self::COMMAND_LENGTH as usize;

    pub fn read(buffer: &'a [u8]) -> Result<Self> {
        if buffer.len() < Self::COMMAND_LENGTH_USIZE {
            todo!("error: buffer too short")
        }
        let kind = buffer[0];
        match kind {
            0 => Ok(Self::NewGrainInWal {
                id: GrainId::from_bytes(&buffer[1..9]).unwrap(), // TODO real error,
                data: &buffer[9..],
            }),
            1 => Ok(Self::ArchiveGrain(
                GrainId::from_bytes(&buffer[1..9]).unwrap(),
            )),
            2 => Ok(Self::FreeGrain(GrainId::from_bytes(&buffer[1..9]).unwrap())),
            3 => Ok(Self::UpdatedEmbeddedHeader(GrainId::from_bytes(
                &buffer[1..9],
            ))),
            4 => Ok(Self::CheckpointTo(TransactionId::from(EntryId(
                u64::from_be_bytes(buffer[1..9].try_into().expect("u64 is 8 bytes")),
            )))),
            5 => Ok(Self::CheckpointedTo(TransactionId::from(EntryId(
                u64::from_be_bytes(buffer[1..9].try_into().expect("u64 is 8 bytes")),
            )))),
            255 => {
                Ok(Self::FinishTransaction {
                    commit_log_entry: GrainId::from_bytes(&buffer[1..9]).unwrap(), /* TODO real error, */
                })
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

    pub fn write_archive_grain<W: Write>(grain_id: GrainId, writer: &mut W) -> Result<()> {
        writer.write_all(&[1])?;
        writer.write_all(&grain_id.to_be_bytes())?;
        Ok(())
    }

    pub fn write_free_grain<W: Write>(grain_id: GrainId, writer: &mut W) -> Result<()> {
        writer.write_all(&[2])?;
        writer.write_all(&grain_id.to_be_bytes())?;
        Ok(())
    }

    pub fn write_embedded_header_update<W: Write>(
        new_embedded_header: Option<GrainId>,
        writer: &mut W,
    ) -> Result<()> {
        writer.write_all(&[3])?;
        writer.write_all(&new_embedded_header.unwrap_or(GrainId::NONE).to_be_bytes())?;
        Ok(())
    }

    pub fn write_checkpoint_to<W: Write>(
        checkpoint_to: TransactionId,
        writer: &mut W,
    ) -> Result<()> {
        writer.write_all(&[4])?;
        writer.write_all(&checkpoint_to.to_be_bytes())?;
        Ok(())
    }

    pub fn write_checkpointed_to<W: Write>(
        checkpointed_to: TransactionId,
        writer: &mut W,
    ) -> Result<()> {
        writer.write_all(&[5])?;
        writer.write_all(&checkpointed_to.to_be_bytes())?;
        Ok(())
    }

    pub const fn new_grain_length(data_length: u32) -> u32 {
        data_length + 9
    }

    pub fn write_transaction_tail<W: Write>(
        commit_log_entry_id: GrainId,
        writer: &mut W,
    ) -> Result<()> {
        writer.write_all(&[255])?;
        writer.write_all(&commit_log_entry_id.to_be_bytes())?;
        Ok(())
    }
}
