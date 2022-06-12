use std::collections::{HashMap, HashSet};

use parking_lot::{Condvar, Mutex};

use crate::{
    database::{
        disk::{BasinState, GrainMapState, StratumState},
        DatabaseState, GrainReservation,
    },
    format::{
        crc, BasinIndex, BatchId, CommitLogEntry, GrainChange, GrainMapPage, GrainOperation,
        LogEntryIndex, StratumIndex, PAGE_SIZE, PAGE_SIZE_U64,
    },
    io::{self, ext::ToIoResult},
    todo_if,
    utils::Multiples,
};

#[derive(Debug, Default)]
pub(super) struct Committer {
    commit_sync: Condvar,
    state: Mutex<CommitBatches>,
}

impl Committer {
    pub fn commit<File: io::File>(
        &self,
        batch: impl Iterator<Item = GrainReservation>,
        database: &DatabaseState,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<BatchId> {
        let mut state = self.state.lock();
        let batch_id = state.batch_id;
        state.batch_id.0 += 1;
        let batch = GrainBatch {
            batch_id,
            grains: batch.collect(),
        };
        state.pending_batches.push(batch);

        loop {
            if state.committing {
                // Wait for a commit to notify that a batch has been committed.
                self.commit_sync.wait(&mut state);
                // Check if our batch has been written.
                if let Some(commit_sequence) = state.committed_batches.remove(&batch_id) {
                    return Ok(commit_sequence);
                } else {
                    continue;
                }
            } else {
                // No other thread has taken over committing, so this thread
                // should.
                let (last_batch_id, grains) = state.become_commit_thread();
                // Release the mutex, allowing other threads to queue batches
                // while we write and sync the file.
                drop(state);

                let new_batch_id = self.commit_batches(grains, database, file, scratch)?;

                // Re-acquire the state to update the knowledge of committed batches
                let mut state = self.state.lock();
                for id in (state.committed_batch_id.0 + 1)..=last_batch_id.0 {
                    state
                        .committed_batches
                        .insert(GrainBatchId(id), new_batch_id);
                }
                state.committing = false;
                drop(state);

                // Even if we didn't commit multiple, another thread could still
                // be waiting.
                self.commit_sync.notify_all();

                return Ok(new_batch_id);
            }
        }
    }

    fn commit_batches<File: io::File>(
        &self,
        mut grains: Vec<GrainReservation>,
        database: &DatabaseState,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<BatchId> {
        // Sort the grains to ensure we can work with a single grain map at a
        // time.
        grains.sort_by(|a, b| a.grain_id.cmp(&b.grain_id));

        let mut disk_state = database.disk_state.lock();

        let committing_batch = disk_state.header.sequence.next();

        // Gather info about new basins and strata that the atlas has allocated.
        let atlas = database.atlas.lock();
        let mut modified_basins = Vec::<usize>::new();
        let mut modified_grain_maps = HashSet::<(usize, usize, usize)>::new();
        let mut new_pages = HashSet::<u64>::new();
        for (basin_index, basin_atlas) in atlas.basins.iter().enumerate() {
            let mut changed = false;
            let basin = if basin_index < disk_state.basins.len() {
                todo_if!(
                    disk_state.header.basins[basin_index].file_offset != basin_atlas.location,
                    "need to handle relocation"
                );

                &mut disk_state.basins[basin_index]
            } else {
                changed = true;
                disk_state.basins.push(BasinState::default());
                disk_state.header.basins.push(BasinIndex {
                    sequence_id: committing_batch,
                    file_offset: basin_atlas.location,
                });
                disk_state.basins.last_mut().unwrap()
            };

            for (stratum_index, stratum_atlas) in basin_atlas.strata.iter().enumerate() {
                todo_if!(stratum_atlas.grain_maps.len() > 1);

                let stratum = if stratum_index < basin.strata.len() {
                    &mut basin.strata[stratum_index]
                } else {
                    changed = true;
                    new_pages.insert(
                        stratum_atlas.grain_maps[0].offset + stratum_atlas.grain_map_header_length,
                    );
                    basin.header.strata.push(StratumIndex {
                        grain_map_count: u32::try_from(stratum_atlas.grain_maps.len()).to_io()?,
                        grain_count_exp: stratum_atlas.grain_count_exp(),
                        grain_length_exp: stratum_atlas.grain_length_exp(),
                        grain_map_location: stratum_atlas.grain_maps[0].offset,
                    });
                    basin.strata.push(StratumState {
                        grain_maps: vec![GrainMapState::new(
                            stratum_atlas.grain_maps[0].offset,
                            stratum_atlas.grains_per_map,
                        )],
                    });
                    modified_grain_maps.insert((basin_index, stratum_index, 0));
                    basin.strata.last_mut().unwrap()
                };

                todo_if!(
                    stratum_atlas.grain_maps[0].offset != stratum.grain_maps[0].offset,
                    "need to handle relocation"
                );
            }

            if changed {
                modified_basins.push(basin_index);
            }
        }
        // Free the atlas, allowing writes to the database resume.
        drop(atlas);

        let mut modified_pages = Vec::<(u64, GrainMapPage)>::new();
        let mut grain_changes = Vec::with_capacity(grains.len());
        for reservation in grains {
            // Load any existing grain map pages, modify then, and add them to
            // `modified_pages`. We must keep the atlas locked as little as
            // possible during this phase.
            let basin_index = usize::from(reservation.grain_id.basin_index());
            let basin = &mut disk_state.basins[basin_index];
            let stratum_index = usize::from(reservation.grain_id.stratum_index());
            let stratum_info = &basin.header.strata[stratum_index];
            let stratum = &mut basin.strata[stratum_index];

            let grain_index = reservation.grain_id.grain_index();
            let grains_per_map = stratum_info.grains_per_map();
            let grain_map_index = usize::try_from(grain_index / grains_per_map).to_io()?;
            let local_grain_index = usize::try_from(grain_index % grains_per_map).to_io()?;
            let grain_count = usize::try_from(
                (reservation.length + stratum_info.grain_length() - 1)
                    / stratum_info.grain_length(),
            )
            .to_io()?;

            todo_if!(local_grain_index >= 170);
            let grain_map_page_offset =
                stratum.grain_maps[grain_map_index].offset + stratum_info.header_length();
            let grain_map_page = match modified_pages.last_mut() {
                Some((offset, grain_map_page)) if offset == &grain_map_page_offset => {
                    grain_map_page
                }
                _ => {
                    let page = if new_pages.remove(&grain_map_page_offset) {
                        // A new page, no need to load from disk.
                        GrainMapPage::default()
                    } else {
                        database.grain_map_page_cache.fetch(
                            grain_map_page_offset,
                            true,
                            file,
                            scratch,
                        )?
                    };
                    modified_pages.push((grain_map_page_offset, page));

                    &mut modified_pages.last_mut().unwrap().1
                }
            };

            stratum.grain_maps[grain_map_index].map.allocation_state
                [local_grain_index..local_grain_index + grain_count]
                .fill(true);
            for local_grain_index in local_grain_index..local_grain_index + grain_count {
                let grain = &mut grain_map_page.grains[local_grain_index];
                assert!(grain.allocated_at.is_none());
                grain.allocated_at = Some(committing_batch);
                grain.crc = reservation.crc;
                grain.length = reservation.length;
                grain.archived_at = None;
            }
            modified_grain_maps.insert((basin_index, stratum_index, grain_map_index));
            grain_changes.push(GrainChange {
                operation: GrainOperation::Allocate,
                start: reservation.grain_id,
                count: u32::try_from(grain_count).unwrap(),
            });
        }

        // TODO These writes should be able to be parallelized

        // Write the commit log for these changes.
        let log_entry = CommitLogEntry { grain_changes };
        log_entry.serialize_into(scratch);
        let log_entry_crc = crc(scratch);
        let log_entry_len = u32::try_from(scratch.len()).to_io()?;
        // pad the entry to the next page size
        scratch.resize(scratch.len().round_to_multiple_of(PAGE_SIZE).unwrap(), 0);
        let log_entry_offset = database
            .file_allocations
            .allocate(u64::try_from(scratch.len()).to_io()?, file)?;
        let buffer = std::mem::take(scratch);
        let (result, buffer) = file.write_all(buffer, log_entry_offset);
        *scratch = buffer;
        result?;

        // Point to the new entry from the commit log.
        let mut log = database.log.write();
        log.push(
            committing_batch,
            LogEntryIndex {
                position: log_entry_offset,
                length: log_entry_len,
                crc: log_entry_crc,
                embedded_header: None,
            },
        );
        log.write_to(&database.file_allocations, file, scratch)?;
        disk_state.header.log_offset = log.position().unwrap();
        drop(log);

        for (offset, grain_map_page) in &modified_pages {
            grain_map_page.write_to(*offset, file, scratch)?;
        }

        for (basin_index, stratum_index, grain_map_index) in modified_grain_maps {
            let grain_map = &mut disk_state.basins[basin_index].strata[stratum_index].grain_maps
                [grain_map_index];
            grain_map.map.sequence = committing_batch;
            let offset = grain_map.offset_to_write_at();
            grain_map.map.write_to(offset, file, scratch)?;
            grain_map.first_is_current = !grain_map.first_is_current;
        }

        for basin_index in modified_basins {
            disk_state.header.basins[basin_index].sequence_id = committing_batch;
            let mut offset = disk_state.header.basins[basin_index].file_offset;

            let basin = &mut disk_state.basins[basin_index];
            if basin.first_is_current {
                offset += PAGE_SIZE_U64
            }

            basin.header.sequence = committing_batch;
            basin.header.write_to(offset, file, false, scratch)?;
            basin.first_is_current = !basin.first_is_current;
        }

        disk_state.header.sequence = committing_batch;
        disk_state.header.write_to(
            if disk_state.first_header_is_current {
                PAGE_SIZE_U64
            } else {
                0
            },
            false,
            file,
            scratch,
        )?;
        disk_state.first_header_is_current = !disk_state.first_header_is_current;

        // Publish the updated grain maps
        database
            .grain_map_page_cache
            .update_pages(modified_pages.into_iter());

        Ok(committing_batch)
    }
}

#[derive(Debug)]
struct GrainBatch {
    pub batch_id: GrainBatchId,
    pub grains: Vec<GrainReservation>,
}

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
struct GrainBatchId(u64);

#[derive(Debug)]
struct CommitBatches {
    committing: bool,
    batch_id: GrainBatchId,
    committed_batch_id: GrainBatchId,
    committed_batches: HashMap<GrainBatchId, BatchId>,
    pending_batches: Vec<GrainBatch>,
}

impl CommitBatches {
    fn become_commit_thread(&mut self) -> (GrainBatchId, Vec<GrainReservation>) {
        self.committing = true;
        let mut grains =
            Vec::with_capacity(self.pending_batches.iter().map(|b| b.grains.len()).sum());
        let mut latest_batch_id = self.pending_batches.last().unwrap().batch_id;
        for mut batch in self.pending_batches.drain(..) {
            grains.append(&mut batch.grains);
            latest_batch_id = batch.batch_id;
        }
        (latest_batch_id, grains)
    }
}

impl Default for CommitBatches {
    fn default() -> Self {
        Self {
            committing: false,
            batch_id: GrainBatchId(1),
            committed_batch_id: GrainBatchId(0),
            pending_batches: Vec::new(),
            committed_batches: HashMap::new(),
        }
    }
}
