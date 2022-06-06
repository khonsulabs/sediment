use std::collections::{HashMap, HashSet};

use parking_lot::{Condvar, Mutex};

use crate::{
    database::{
        disk::{self, BasinState, StrataState},
        DatabaseState, GrainReservation,
    },
    format::{
        BasinHeader, BasinIndex, BatchId, GrainMap, GrainMapHeader, GrainMapPage, StrataIndex,
    },
    io::{self, ext::ToIoResult},
    todo_if,
};

#[derive(Default, Debug)]
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
            if state.already_committing {
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
                return self.commit_batches(last_batch_id, grains, database, file, scratch);
            }
        }
    }

    fn commit_batches<File: io::File>(
        &self,
        last_batch_id: GrainBatchId,
        mut grains: Vec<GrainReservation>,
        database: &DatabaseState,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<BatchId> {
        // Sort the grains to ensure we can work with a single grain map at a
        // time.
        grains.sort_by(|a, b| a.grain_id.cmp(&b.grain_id));

        let mut disk_state = database.disk_state.lock();

        let mut file_header = disk_state.header.write_next();
        let committing_batch = file_header.sequence.next();

        // Gather info about new basins and strata that the atlas has allocated.
        let atlas = database.atlas.lock();
        let mut modified_basins = Vec::<usize>::new();
        let mut modified_grain_maps = HashSet::<(usize, usize, usize)>::new();
        let mut new_pages = HashSet::<u64>::new();
        for (basin_index, basin_atlas) in atlas.basins.iter().enumerate() {
            let mut changed = false;
            let mut basin = if basin_index < disk_state.basins.len() {
                todo_if!(
                    disk_state.header.next_mut().basins[basin_index].file_offset
                        != basin_atlas.location,
                    "need to handle relocation"
                );

                &mut disk_state.basins[basin_index]
            } else {
                changed = true;
                disk_state.basins.push(BasinState {
                    header: BasinHeader::default(),
                    stratum: Vec::new(),
                });
                disk_state.header.next_mut().basins.push(BasinIndex {
                    sequence_id: committing_batch,
                    file_offset: basin_atlas.location,
                });
                disk_state.basins.last_mut().unwrap()
            };

            for (strata_index, strata_atlas) in basin_atlas.stratum.iter().enumerate() {
                todo_if!(strata_atlas.grain_maps.len() > 1);

                let strata = if strata_index < basin.stratum.len() {
                    &mut basin.stratum[strata_index]
                } else {
                    changed = true;
                    new_pages.insert(strata_atlas.grain_maps[0].offset);
                    basin.header.next_mut().stratum.push(StrataIndex {
                        grain_map_count: u32::try_from(strata_atlas.grain_maps.len()).to_io()?,
                        grain_count_exp: strata_atlas.grain_count_exp(),
                        grain_length_exp: strata_atlas.grain_length_exp(),
                        grain_map_location: strata_atlas.grain_maps[0].offset,
                    });
                    basin.stratum.push(StrataState {
                        grain_maps: vec![GrainMapHeader::new(
                            strata_atlas.grain_maps[0].offset,
                            strata_atlas.grains_per_map,
                        )],
                    });
                    modified_grain_maps.insert((basin_index, strata_index, 0));
                    basin.stratum.last_mut().unwrap()
                };

                todo_if!(
                    strata_atlas.grain_maps[0].offset != strata.grain_maps[0].offset(),
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
        for reservation in grains {
            // Load any existing grain map pages, modify then, and add them to
            // `modified_pages`. We must keep the atlas locked as little as
            // possible during this phase.
            let basin_index = usize::from(reservation.grain_id.basin_index());
            let basin = &mut disk_state.basins[basin_index];
            let strata_index = usize::from(reservation.grain_id.strata_index());
            let strata_info = &basin.header.next().stratum[strata_index];
            let strata = &mut basin.stratum[strata_index];

            let grain_index = reservation.grain_id.grain_index();
            let grains_per_map = strata_info.grains_per_map();
            let grain_map_index = usize::try_from(grain_index / grains_per_map).to_io()?;
            let local_grain_index = usize::try_from(grain_index % grains_per_map).to_io()?;
            let grain_count = usize::try_from(
                (reservation.length + strata_info.grain_length() - 1) / strata_info.grain_length(),
            )
            .to_io()?;

            let grain_map_offset = strata.grain_maps[grain_map_index].offset();
            let grain_map_page = match modified_pages.last_mut() {
                Some((offset, grain_map_page)) if offset == &grain_map_offset => grain_map_page,
                _ => {
                    if new_pages.remove(&grain_map_offset) {
                        // A new page, no need to load from disk.
                        modified_pages.push((grain_map_offset, GrainMapPage::default()));
                    } else {
                        todo!("load grain map from disk")
                    }

                    &mut modified_pages.last_mut().unwrap().1
                }
            };

            strata.grain_maps[grain_map_index]
                .next_mut()
                .allocation_state[local_grain_index..local_grain_index + grain_count]
                .fill(true);
            for local_grain_index in local_grain_index..local_grain_index + grain_count {
                let grain = &mut grain_map_page.grains[local_grain_index];
                assert!(grain.allocated_at.is_none());
                grain.allocated_at = Some(committing_batch);
                grain.crc = reservation.crc;
                grain.length = reservation.length;
                grain.archived_at = None;
            }
            modified_grain_maps.insert((basin_index, strata_index, grain_map_index));
        }

        // TODO These writes should be able to be parallelized
        for (offset, grain_map_page) in &modified_pages {
            grain_map_page.write_to(*offset, file, scratch)?;
        }
        // TODO overwrite pages in cache with the newly written pages

        for (basin_index, strata_index, grain_map_index) in modified_grain_maps {
            let grain_count =
                disk_state.basins[basin_index].header.next().stratum[strata_index].grains_per_map();
            let grain_map = &mut disk_state.basins[basin_index].stratum[strata_index].grain_maps
                [grain_map_index];
            grain_map.write_to(committing_batch, grain_count, file, scratch)?;
        }

        for basin_index in modified_basins {
            let offset = disk_state.header.next().basins[basin_index].file_offset;
            disk_state.basins[basin_index].header.write_to(
                committing_batch,
                file,
                offset,
                scratch,
            )?;
        }

        disk_state.header.next_mut().sequence = committing_batch;
        disk_state
            .header
            .flush_to_file(committing_batch, file, scratch)?;

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
    already_committing: bool,
    batch_id: GrainBatchId,
    committed_batch_id: GrainBatchId,
    committed_batches: HashMap<GrainBatchId, BatchId>,
    pending_batches: Vec<GrainBatch>,
}

impl CommitBatches {
    fn become_commit_thread(&mut self) -> (GrainBatchId, Vec<GrainReservation>) {
        self.already_committing = true;
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
            already_committing: false,
            batch_id: GrainBatchId(1),
            committed_batch_id: GrainBatchId(0),
            pending_batches: Vec::new(),
            committed_batches: HashMap::new(),
        }
    }
}
