use std::{
    collections::{HashMap, HashSet},
    iter,
    sync::{atomic::Ordering, Arc},
};

use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};
use rebytes::{Allocator, Buffer};

use crate::{
    database::{
        checkpoint_guard::CheckpointGuard,
        disk::{BasinState, DiskState, GrainMapState, StratumState},
        embedded::{EmbeddedHeaderUpdate, GrainMutexGuard},
        page_cache::LoadedGrainMapPage,
        DatabaseState, GrainReservation,
    },
    format::{
        crc, Allocation, BasinIndex, BatchId, CommitLogEntry, GrainChange, GrainId, GrainMapPage,
        GrainOperation, LogEntryIndex, StratumIndex, PAGE_SIZE, PAGE_SIZE_U64,
    },
    io::{self, ext::ToIoResult, paths::PathId, File, FileManager, WriteIoBuffer},
    todo_if,
    utils::Multiples,
};

#[derive(Debug)]
pub(super) struct Committer<AsyncFile: io::AsyncFileWriter> {
    commit_sync: Condvar,
    state: Mutex<CommitBatches<AsyncFile>>,
    pending_reservations: RwLock<HashMap<GrainId, GrainReservation>>,
}

impl<AsyncFile: io::AsyncFileWriter> Default for Committer<AsyncFile> {
    fn default() -> Self {
        Self {
            commit_sync: Condvar::default(),
            state: Mutex::default(),
            pending_reservations: RwLock::default(),
        }
    }
}
impl<AsyncFile: io::AsyncFileWriter> Committer<AsyncFile> {
    pub fn pending_grain_reservation(&self, grain_id: GrainId) -> Option<GrainReservation> {
        let pending_reservations = self.pending_reservations.read();
        pending_reservations.get(&grain_id).copied()
    }

    pub fn enqueue_commit(
        &self,
        batch: impl Iterator<Item = GrainBatchOperation>,
        async_writes: impl Iterator<Item = AsyncFile>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: Option<GrainMutexGuard>,
        database: Arc<DatabaseState<AsyncFile::Manager>>,
    ) -> PendingCommit<AsyncFile::Manager> {
        let mut pending_reservations = self.pending_reservations.write();
        let mut state = self.state.lock();
        let batch_id = Self::batch_commit(
            batch,
            async_writes,
            checkpoint_to,
            new_embedded_header,
            &mut pending_reservations,
            &mut state,
        );

        PendingCommit { database, batch_id }
    }

    fn batch_commit(
        batch: impl Iterator<Item = GrainBatchOperation>,
        async_writes: impl Iterator<Item = AsyncFile>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: Option<GrainMutexGuard>,
        pending_reservations: &mut HashMap<GrainId, GrainReservation>,
        state: &mut CommitBatches<AsyncFile>,
    ) -> GrainBatchId {
        let batch_id = state.batch_id;
        state.batch_id.0 += 1;
        pending_reservations.reserve(batch.size_hint().0);
        let batch = GrainBatch {
            batch_id,
            checkpoint_to,
            grains: batch
                .map(|op| {
                    if let GrainBatchOperation::Allocate(reservation) = &op {
                        pending_reservations.insert(reservation.grain_id, *reservation);
                    }
                    op
                })
                .collect(),
            async_writers: async_writes.collect(),
        };
        state.pending_batches.push(batch);
        // Take the new header value, and release the guard by consuming it with
        // map.
        if let Some(new_embedded_header) = new_embedded_header {
            // TODO this code needs to archive grains, but it's far enough in
            // the process it's kind of annoying to do. Ideally, if the new
            // header isn't used, it shouldn't be included in the batch in the
            // first place.
            state.new_embedded_header = EmbeddedHeaderUpdate::Replace(new_embedded_header.get());
        }

        batch_id
    }

    pub fn commit(
        &self,
        batch: impl Iterator<Item = GrainBatchOperation>,
        async_writes: impl Iterator<Item = AsyncFile>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: Option<GrainMutexGuard>,
        database: &DatabaseState<AsyncFile::Manager>,
    ) -> io::Result<CheckpointGuard> {
        let mut pending_reservations = self.pending_reservations.write();
        let mut state = self.state.lock();
        let batch_id = Self::batch_commit(
            batch,
            async_writes,
            checkpoint_to,
            new_embedded_header,
            &mut pending_reservations,
            &mut state,
        );
        drop(pending_reservations);
        self.commit_or_wait(batch_id, state, database)
    }

    pub fn wait_for_batch(
        &self,
        pending_commit: &PendingCommit<AsyncFile::Manager>,
    ) -> io::Result<CheckpointGuard> {
        self.commit_or_wait(
            pending_commit.batch_id,
            self.state.lock(),
            &pending_commit.database,
        )
    }

    fn commit_or_wait(
        &self,
        batch_id: GrainBatchId,
        mut state: MutexGuard<'_, CommitBatches<AsyncFile>>,
        database: &DatabaseState<AsyncFile::Manager>,
    ) -> io::Result<CheckpointGuard> {
        loop {
            // Check if our batch has been written.
            if let Some(committed_batch_id) = state.committed_batches.remove(&batch_id) {
                return Ok(committed_batch_id);
            } else if state.committing {
                // Wait for a commit to notify that a batch has been committed.
                self.commit_sync.wait(&mut state);
            } else {
                // No other thread has taken over committing, so this thread
                // should.
                let (last_batch_id, checkpoint_to, new_embedded_header, mut grains, async_writes) =
                    state.become_commit_thread();
                // Release the mutex, allowing other threads to queue batches
                // while we write and sync the file.
                drop(state);

                let checkpoint_to = checkpoint_to.and_then(|checkpoint_to| {
                    database
                        .checkpoint_protector
                        .safe_to_checkpoint_batch_id(checkpoint_to)
                });

                let mut file = database.file_manager.write(&database.path)?;
                let new_batch_id = Self::commit_batches(
                    &mut grains,
                    async_writes,
                    checkpoint_to,
                    new_embedded_header,
                    database,
                    &mut file,
                )?;
                drop(file);

                let new_batch_guard = database.checkpoint_protector.guard(new_batch_id);

                // Re-acquire the state to update the knowledge of committed batches
                let mut state = self.state.lock();
                for id in (state.committed_batch_id.0 + 1)..=last_batch_id.0 {
                    if id != batch_id.0 {
                        state
                            .committed_batches
                            .insert(GrainBatchId(id), new_batch_guard.clone());
                    }
                }
                state.committed_batch_id = last_batch_id;
                state.committing = false;
                drop(state);

                // Even if we didn't commit multiple, another thread could still
                // be waiting.
                self.commit_sync.notify_all();

                // Publish freed grains to the atlas
                let mut atlas = database.atlas.lock();
                atlas.free_grains(&grains)?;
                drop(atlas);

                let mut pending_reservations = self.pending_reservations.write();
                for reservation in grains.into_iter().filter_map(|op| {
                    if let GrainBatchOperation::Allocate(reservation) = op {
                        Some(reservation)
                    } else {
                        None
                    }
                }) {
                    pending_reservations.remove(&reservation.grain_id);
                }
                drop(pending_reservations);

                return Ok(new_batch_guard);
            }
        }
    }

    fn commit_batches(
        grains: &mut Vec<GrainBatchOperation>,
        pending_async_writers: Vec<AsyncFile>,
        checkpoint_to: Option<BatchId>,
        new_embedded_header: EmbeddedHeaderUpdate,
        database: &DatabaseState<AsyncFile::Manager>,
        file: &mut <AsyncFile::Manager as io::FileManager>::File,
    ) -> io::Result<BatchId> {
        let mut disk_state = database.disk_state.lock();

        let committing_batch = disk_state.header.batch.next();

        // Gather info about new basins and strata that the atlas has allocated.
        let mut modifications =
            Self::gather_modifications(committing_batch, database, &mut disk_state)?;

        let freed_log_pages = if let Some(checkpoint_to) = checkpoint_to {
            disk_state.header.checkpoint = checkpoint_to;
            let mut log = database.log.write();
            let pages_to_remove = log.checkpoint(checkpoint_to);

            for loaded_page in &pages_to_remove {
                for entry in &loaded_page.page.entries {
                    if entry.position == 0 {
                        break;
                    }

                    let entry =
                        CommitLogEntry::load_from(entry, true, file, &database.buffer_allocator)?;
                    for archive_change in entry
                        .grain_changes
                        .iter()
                        .filter(|change| matches!(change.operation, GrainOperation::Archive))
                    {
                        grains.push(GrainBatchOperation::Free(*archive_change));
                    }
                }
            }

            Some(pages_to_remove)
        } else {
            None
        };

        // Sort the grains to ensure we can work with a single grain map at a
        // time.
        grains.sort_by_key(GrainBatchOperation::grain_id);

        let (mut modified_pages, log_entry) = Self::update_grain_map_pages(
            grains,
            &mut modifications,
            database,
            &mut disk_state,
            file,
        )?;

        disk_state.header.log_offset = Self::write_commit_log(
            &log_entry,
            committing_batch,
            new_embedded_header,
            database,
            file,
        )?;

        for (offset, grain_map_page) in &mut modified_pages {
            grain_map_page.write_to(*offset, committing_batch, file, &database.buffer_allocator)?;
        }

        Self::write_modified(
            &mut modifications,
            committing_batch,
            &mut disk_state,
            &database.buffer_allocator,
            file,
        )?;

        disk_state.header.batch = committing_batch;
        disk_state.header.write_to(
            if disk_state.first_header_is_current {
                PAGE_SIZE_U64
            } else {
                0
            },
            file,
            &database.buffer_allocator,
        )?;

        for mut async_writer in pending_async_writers {
            async_writer.wait()?;
        }

        file.synchronize()?;

        disk_state.first_header_is_current = !disk_state.first_header_is_current;

        // Publish the updated grain maps
        database
            .grain_map_page_cache
            .update_pages(modified_pages.into_iter());

        // Publish the batch id
        // TODO does this need to be SeqCst?
        database
            .current_batch
            .store(committing_batch.0, Ordering::SeqCst);

        // Mark the regions of any free log pages as free.
        for removed_log_page_offset in freed_log_pages
            .into_iter()
            .flatten()
            .filter_map(|page| page.offset)
        {
            database.file_allocations.set(
                removed_log_page_offset..removed_log_page_offset + PAGE_SIZE_U64,
                Allocation::Free,
            );
        }

        // Publish the new embedded header
        if let EmbeddedHeaderUpdate::Replace(new_header) = new_embedded_header {
            database.embedded_header.publish(new_header);
        }

        Ok(committing_batch)
    }

    fn write_modified<File: io::WriteIoBuffer>(
        modifications: &mut CommitModifications,
        committing_batch: BatchId,
        disk_state: &mut DiskState,
        allocator: &Allocator,
        file: &mut File,
    ) -> io::Result<()> {
        for (basin_index, stratum_index, grain_map_index) in modifications.grain_maps.drain() {
            let grain_map = &mut disk_state.basins[basin_index].strata[stratum_index].grain_maps
                [grain_map_index];
            grain_map.map.written_at = committing_batch;
            let offset = grain_map.offset_to_write_at();
            grain_map.map.write_to(offset, allocator, file)?;
            grain_map.first_is_current = !grain_map.first_is_current;
        }

        for basin_index in modifications.modified_basins.drain(..) {
            disk_state.header.basins[basin_index].last_written_at = committing_batch;
            let mut offset = disk_state.header.basins[basin_index].file_offset;

            let basin = &mut disk_state.basins[basin_index];
            if basin.first_is_current {
                offset += PAGE_SIZE_U64;
            }

            basin.header.written_at = committing_batch;
            basin.header.write_to(offset, file, allocator)?;
            basin.first_is_current = !basin.first_is_current;
        }

        Ok(())
    }

    fn gather_modifications<Manager: io::FileManager>(
        committing_batch: BatchId,
        database: &DatabaseState<Manager>,
        disk_state: &mut DiskState,
    ) -> io::Result<CommitModifications> {
        let atlas = database.atlas.lock();
        let mut modified_basins = Vec::<usize>::new();
        let mut modified_grain_maps = HashSet::<(usize, usize, usize)>::new();
        let mut new_pages = HashSet::<u64>::new();
        for (basin_index, basin_atlas) in atlas.basins.iter().enumerate() {
            let mut changed = false;
            let basin = if basin_index < disk_state.basins.len() {
                todo_if!(
                    disk_state.header.basins[basin_index].file_offset != basin_atlas.location,
                    "need to handle relocation https://github.com/khonsulabs/sediment/issues/12"
                );

                &mut disk_state.basins[basin_index]
            } else {
                changed = true;
                disk_state.basins.push(BasinState::default());
                disk_state.header.basins.push(BasinIndex {
                    last_written_at: committing_batch,
                    file_offset: basin_atlas.location,
                });
                disk_state.basins.last_mut().unwrap()
            };

            for (stratum_index, stratum_atlas) in basin_atlas.strata.iter().enumerate() {
                todo_if!(stratum_atlas.grain_maps.len() > 1, "need to support multiple grain maps https://github.com/khonsulabs/sediment/issues/11");

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
                        )?],
                    });
                    modified_grain_maps.insert((basin_index, stratum_index, 0));
                    basin.strata.last_mut().unwrap()
                };

                todo_if!(
                    stratum_atlas.grain_maps[0].offset != stratum.grain_maps[0].offset,
                    "need to handle relocation https://github.com/khonsulabs/sediment/issues/12"
                );
            }

            if changed {
                modified_basins.push(basin_index);
            }
        }
        // Free the atlas, allowing writes to the database resume.
        drop(atlas);

        Ok(CommitModifications {
            modified_basins,
            grain_maps: modified_grain_maps,
            new_pages,
        })
    }

    fn update_grain_map_pages<File: io::File>(
        grains: &[GrainBatchOperation],
        modifications: &mut CommitModifications,
        database: &DatabaseState<File::Manager>,
        disk_state: &mut DiskState,
        file: &mut File,
    ) -> io::Result<(Vec<(u64, LoadedGrainMapPage)>, CommitLogEntry)> {
        let mut modified_pages = Vec::<(u64, LoadedGrainMapPage)>::new();
        let mut grain_changes = Vec::with_capacity(grains.len());
        for operation in grains {
            let basin_index = usize::from(operation.grain_id().basin_index());
            let stratum_index = usize::from(operation.grain_id().stratum_index());
            let basin = &mut disk_state.basins[basin_index];
            let stratum_info = &basin.header.strata[stratum_index];
            let stratum = &mut basin.strata[stratum_index];

            let grain_index = operation.grain_id().grain_index();
            let grains_per_map = stratum_info.grains_per_map();
            let grain_map_index = usize::try_from(grain_index / grains_per_map).to_io()?;

            // At this point we have enough information to handle the Archive
            // operation. For the other two operations, we need the grain count,
            // and the data comes from two different locations.
            let (grain_count, allocation_state, log_op) = match operation {
                GrainBatchOperation::Allocate(reservation) => (
                    u8::try_from((reservation.length + 8).ceil_div_by(stratum_info.grain_length()))
                        .to_io()?,
                    true,
                    GrainOperation::Allocate {
                        crc: reservation
                            .crc
                            .expect("reservation committed without being written to"),
                    },
                ),
                GrainBatchOperation::Free(change) => (change.count, false, GrainOperation::Free),
                GrainBatchOperation::Archive { grain_id, count } => {
                    // Archiving a grain doesn't actually change these pages, we
                    // just need the archive entry in the commit log. The
                    // allocation states will be updated when the log entry
                    // containing the archive command is checkpointed.
                    modifications
                        .grain_maps
                        .insert((basin_index, stratum_index, grain_map_index));
                    grain_changes.push(GrainChange {
                        operation: GrainOperation::Archive,
                        start: *grain_id,
                        count: *count,
                    });
                    continue;
                }
            };

            let local_grain_index = usize::try_from(grain_index % grains_per_map).to_io()?;
            todo_if!(local_grain_index >= GrainMapPage::GRAINS, "need to handle multiple grain map pages https://github.com/khonsulabs/sediment/issues/11");
            let grain_map_page_offset =
                stratum.grain_maps[grain_map_index].offset + stratum_info.total_header_length();

            let loaded_grain_map_page = match modified_pages.last_mut() {
                Some((offset, grain_map_page)) if offset == &grain_map_page_offset => {
                    grain_map_page
                }
                _ => {
                    let page = if modifications.new_pages.remove(&grain_map_page_offset) {
                        // A new page, no need to load from disk.
                        LoadedGrainMapPage::default()
                    } else {
                        database.grain_map_page_cache.fetch(
                            grain_map_page_offset,
                            disk_state.header.batch,
                            file,
                            &database.buffer_allocator,
                        )?
                    };
                    modified_pages.push((grain_map_page_offset, page));

                    &mut modified_pages.last_mut().unwrap().1
                }
            };
            stratum.grain_maps[grain_map_index].map.allocation_state
                [local_grain_index..local_grain_index + usize::from(grain_count)]
                .fill(allocation_state);

            if allocation_state {
                for (allocation_index, local_grain_index) in
                    (local_grain_index..local_grain_index + usize::from(grain_count)).enumerate()
                {
                    let grain =
                        &mut loaded_grain_map_page.page.consecutive_allocations[local_grain_index];
                    assert!(*grain == 0);
                    *grain = grain_count - u8::try_from(allocation_index).unwrap();
                }
            } else {
                for index in local_grain_index..local_grain_index + usize::from(grain_count) {
                    let grain = &mut loaded_grain_map_page.page.consecutive_allocations[index];
                    assert!(
                        *grain != 0,
                        "grain not allocated {}:{}/{grain_count}",
                        operation.grain_id(),
                        index - local_grain_index
                    );
                    *grain = 0;
                }
            }
            modifications
                .grain_maps
                .insert((basin_index, stratum_index, grain_map_index));
            grain_changes.push(GrainChange {
                operation: log_op,
                start: operation.grain_id(),
                count: grain_count,
            });
        }

        for new_page_offset in modifications.new_pages.drain() {
            modified_pages.push((new_page_offset, LoadedGrainMapPage::default()));
        }

        Ok((modified_pages, CommitLogEntry { grain_changes }))
    }

    fn write_commit_log<Manager: io::FileManager>(
        log_entry: &CommitLogEntry,
        committing_batch: BatchId,
        new_embedded_header: EmbeddedHeaderUpdate,
        database: &DatabaseState<Manager>,
        file: &mut Manager::File,
    ) -> io::Result<u64> {
        // Write the commit log for these changes.
        let mut buffer = Buffer::with_capacity(PAGE_SIZE, database.buffer_allocator.clone());
        log_entry.serialize_into(&mut buffer)?;
        let log_entry_crc = crc(&buffer);
        let log_entry_len = u32::try_from(buffer.len()).to_io()?;
        // pad the entry to the next page size
        buffer.set_len(buffer.len().round_to_multiple_of(PAGE_SIZE).unwrap());
        let log_entry_offset = database
            .file_allocations
            .allocate(u64::try_from(buffer.len()).to_io()?, file)?;
        file.write_all(buffer, log_entry_offset).0?;

        let embedded_header = if let EmbeddedHeaderUpdate::Replace(new_header) = new_embedded_header
        {
            new_header
        } else {
            database.embedded_header.current()
        };

        // Point to the new entry from the commit log.
        let mut log = database.log.write();
        log.push(
            committing_batch,
            LogEntryIndex {
                position: log_entry_offset,
                length: log_entry_len,
                crc: log_entry_crc,
                embedded_header,
            },
        );
        log.write_to(&database.file_allocations, &database.buffer_allocator, file)?;
        let offset = log.position().unwrap();
        drop(log);
        Ok(offset)
    }
}

#[derive(Debug)]
struct GrainBatch<File: io::AsyncFileWriter> {
    pub batch_id: GrainBatchId,
    pub checkpoint_to: Option<BatchId>,
    pub grains: Vec<GrainBatchOperation>,
    pub async_writers: Vec<File>,
}

#[derive(Debug, Clone, Copy)]
pub enum GrainBatchOperation {
    Allocate(GrainReservation),
    Archive { grain_id: GrainId, count: u8 },
    Free(GrainChange),
}

impl GrainBatchOperation {
    pub const fn grain_id(&self) -> GrainId {
        match self {
            GrainBatchOperation::Allocate(reservation) => reservation.grain_id,
            GrainBatchOperation::Free(change) => change.start,
            GrainBatchOperation::Archive { grain_id, .. } => *grain_id,
        }
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
struct GrainBatchId(u64);

#[derive(Debug)]
struct CommitBatches<AsyncFile: io::AsyncFileWriter> {
    committing: bool,
    batch_id: GrainBatchId,
    committed_batch_id: GrainBatchId,
    committed_batches: HashMap<GrainBatchId, CheckpointGuard>,
    pending_batches: Vec<GrainBatch<AsyncFile>>,
    new_embedded_header: EmbeddedHeaderUpdate,
}

impl<AsyncFile> CommitBatches<AsyncFile>
where
    AsyncFile: io::AsyncFileWriter,
{
    fn become_commit_thread(
        &mut self,
    ) -> (
        GrainBatchId,
        Option<BatchId>,
        EmbeddedHeaderUpdate,
        Vec<GrainBatchOperation>,
        Vec<AsyncFile>,
    ) {
        self.committing = true;
        let mut grains =
            Vec::with_capacity(self.pending_batches.iter().map(|b| b.grains.len()).sum());
        let mut async_writes = Vec::with_capacity(
            self.pending_batches
                .iter()
                .map(|b| b.async_writers.len())
                .sum(),
        );
        let mut latest_batch_id = self.pending_batches.last().unwrap().batch_id;
        let mut checkpoint_to = None;
        for mut batch in self.pending_batches.drain(..) {
            grains.append(&mut batch.grains);
            async_writes.append(&mut batch.async_writers);
            latest_batch_id = batch.batch_id;
            if let Some(batch_checkpoint) = batch.checkpoint_to {
                if checkpoint_to.map_or(true, |checkpoint_to| checkpoint_to < batch_checkpoint) {
                    checkpoint_to = Some(batch_checkpoint);
                }
            }
        }
        (
            latest_batch_id,
            checkpoint_to,
            std::mem::take(&mut self.new_embedded_header),
            grains,
            async_writes,
        )
    }
}

impl<File: io::AsyncFileWriter> Default for CommitBatches<File> {
    fn default() -> Self {
        Self {
            committing: false,
            batch_id: GrainBatchId(1),
            committed_batch_id: GrainBatchId(0),
            committed_batches: HashMap::new(),
            pending_batches: Vec::new(),
            new_embedded_header: EmbeddedHeaderUpdate::None,
        }
    }
}

struct CommitModifications {
    modified_basins: Vec<usize>,
    grain_maps: HashSet<(usize, usize, usize)>,
    new_pages: HashSet<u64>,
}

pub struct PendingCommit<Manager>
where
    Manager: io::FileManager,
{
    database: Arc<DatabaseState<Manager>>,
    batch_id: GrainBatchId,
}

impl<Manager> PendingCommit<Manager>
where
    Manager: io::FileManager,
{
    pub fn commit(&self) -> io::Result<CheckpointGuard> {
        self.database.committer.wait_for_batch(self)
    }

    #[must_use]
    pub fn path_id(&self) -> &PathId {
        &self.database.path
    }
}
