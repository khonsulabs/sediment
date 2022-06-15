use crate::{
    database::{allocations::FileAllocations, log::CommitLog, page_cache::PageCache},
    format::{
        crc, Allocation, Basin, BasinIndex, BatchId, CommitLogEntry, GrainChange, GrainMap,
        GrainMapPage, GrainOperation, Header, StratumIndex, PAGE_SIZE, PAGE_SIZE_U64,
    },
    io::{self, ext::ToIoResult},
};

/// Manages the current disk state.
///
/// This structure is only mutated and accessed during:
///
/// - Initial database load. `Atlas::recover` will read the database state and
///   recover the most recent state that can be verified. This is either the
///   most recent or the previous.
/// - During commit. `Committer::commit_batches` will update this state as new
///   changes are made to the basins and file headers.
#[derive(Debug)]
pub struct DiskState {
    pub first_header_is_current: bool,
    pub header: Header,
    pub basins: Vec<BasinState>,
}

impl DiskState {
    pub fn recover<File: io::File>(
        file: &mut File,
        scratch: &mut Vec<u8>,
        page_cache: &PageCache,
    ) -> io::Result<(Self, CommitLog, FileAllocations)> {
        let (first_header, second_header) = if file.is_empty()? {
            // Initialize the an empty database.
            let header = Header::default();
            header.write_to(0, true, file, scratch)?;
            header.write_to(PAGE_SIZE_U64, true, file, scratch)?;
            file.synchronize()?;
            (Ok(header.clone()), Ok(header))
        } else {
            let mut buffer = std::mem::take(scratch);
            buffer.resize(PAGE_SIZE * 2, 0);
            let (result, buffer) = file.read_exact(buffer, 0);
            *scratch = buffer;
            result?;
            let first_header = Header::deserialize_from(&scratch[..PAGE_SIZE], true);
            let second_header = Header::deserialize_from(&scratch[PAGE_SIZE..], true);

            (first_header, second_header)
        };

        let first_batch_id = first_header.as_ref().map(|header| header.batch).ok();
        let seond_batch_id = second_header.as_ref().map(|header| header.batch).ok();
        let (current, previous, current_is_first) = match (first_batch_id, seond_batch_id) {
            (Some(first_batch_id), Some(seond_batch_id)) if first_batch_id > seond_batch_id => {
                (first_header, second_header, true)
            }
            // When either header has an error, put the error header first. This
            // ensures the error messages below will report the failure in
            // loading the header before attempting to restore from the header
            // that did parse.
            (Some(_), Some(_) | None) => (second_header, first_header, false),
            (None, Some(_) | None) => (first_header, second_header, true),
        };

        match current.and_then(|header| {
            Self::load_from_header(header, current_is_first, file, scratch, page_cache)
        }) {
            Ok(loaded) => Ok(loaded),
            Err(current_header_error) => {
                eprintln!(
                    "Error loading current header: {current_header_error}. Attempting rollback."
                );
                match previous
                .and_then(|header| Self::load_from_header(header, !current_is_first, file, scratch, page_cache)) {
                    Ok(loaded) => {
                        // TODO Zero out all data from the next header

                        // TODO offer callback to allow custom rollback logic.
                        eprintln!("Successfully recovered previous state.");
                        Ok(loaded)
                    }
                    Err(previous_header_error) => {
                        Err(io::invalid_data_error(format!("Unrecoverable database. Error loading previous header: {previous_header_error}.")))
                    }
                }
            }
        }
    }

    fn load_from_header<File: io::File>(
        header: Header,
        is_first_header: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
        page_cache: &PageCache,
    ) -> io::Result<(Self, CommitLog, FileAllocations)> {
        let file_allocations = FileAllocations::new(file.len()?);
        // Allocate the file header
        file_allocations.set(0..PAGE_SIZE_U64 * 2, Allocation::Allocated);

        let mut basins = Vec::new();
        for basin in &header.basins {
            basins.push(Self::load_basin_state(
                basin,
                &header,
                &file_allocations,
                file,
                scratch,
            )?);
        }

        let log = if header.log_offset > 0 {
            CommitLog::read_from(
                header.log_offset,
                file,
                scratch,
                header.checkpoint,
                &file_allocations,
            )?
        } else {
            CommitLog::default()
        };

        let state = DiskState {
            first_header_is_current: is_first_header,
            header,
            basins,
        };
        Self::validate_last_log_entry(&log, &state, file, scratch, page_cache)?;

        Ok((state, log, file_allocations))
    }

    fn load_stratum_state<File: io::File>(
        stratum: &StratumIndex,
        header: &Header,
        file_allocations: &FileAllocations,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<StratumState> {
        assert_eq!(stratum.grain_map_count, 1, "need to support multiple maps");
        file_allocations.set(
            stratum.grain_map_location..stratum.grain_map_location + stratum.grain_map_length(),
            Allocation::Allocated,
        );

        let header_length = GrainMap::header_length_for_grain_count(stratum.grains_per_map());

        let first_map = GrainMap::read_from(
            file,
            stratum.grain_map_location,
            stratum.grains_per_map(),
            scratch,
            true,
        );
        let second_map = GrainMap::read_from(
            file,
            stratum.grain_map_location + header_length,
            stratum.grains_per_map(),
            scratch,
            true,
        );

        let (map, first_is_current) = match (first_map, second_map) {
            (Ok(first_map), Ok(second_map)) => {
                if first_map.written_at > second_map.written_at
                    && first_map.written_at <= header.batch
                {
                    (first_map, true)
                } else if second_map.written_at <= header.batch {
                    (second_map, false)
                } else {
                    return Err(io::invalid_data_error(
                        "GrainMap error: neither batch is valid",
                    ));
                }
            }
            (Ok(first_map), _) if first_map.written_at <= header.batch => (first_map, true),
            (_, Ok(second_map)) if second_map.written_at <= header.batch => (second_map, false),
            (Err(err), _) | (_, Err(err)) => {
                return Err(err);
            }
        };

        Ok(StratumState {
            grain_maps: vec![GrainMapState {
                first_is_current,
                offset: stratum.grain_map_location,
                header_length,
                map,
            }],
        })
    }

    fn load_basin_state<File: io::File>(
        basin: &BasinIndex,
        header: &Header,
        file_allocations: &FileAllocations,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<BasinState> {
        file_allocations.set(
            basin.file_offset..basin.file_offset + PAGE_SIZE_U64 * 2,
            Allocation::Allocated,
        );

        let mut buffer = std::mem::take(scratch);
        buffer.resize(PAGE_SIZE * 2, 0);
        let (result, buffer) = file.read_exact(buffer, basin.file_offset);
        *scratch = buffer;
        result?;

        let first_basin = Basin::deserialize_from(&scratch[..PAGE_SIZE], true);
        let second_basin = Basin::deserialize_from(&scratch[PAGE_SIZE..], true);

        let (basin_header, first_is_current) = match (first_basin, second_basin) {
            (Ok(first_basin), _) if first_basin.written_at == basin.last_written_at => {
                (first_basin, true)
            }
            (_, Ok(second_basin)) if second_basin.written_at == basin.last_written_at => {
                (second_basin, false)
            }
            (Ok(first_basin), Ok(second_basin)) => {
                return Err(io::invalid_data_error(format!(
                    "neither basin matches expected batch. First: {}, Second: {}, Expected: {}",
                    first_basin.written_at, second_basin.written_at, basin.last_written_at
                )))
            }
            (Err(err), _) | (_, Err(err)) => {
                return Err(err);
            }
        };

        let mut strata = Vec::with_capacity(basin_header.strata.len());
        for stratum in &basin_header.strata {
            strata.push(Self::load_stratum_state(
                stratum,
                header,
                file_allocations,
                file,
                scratch,
            )?);
        }
        Ok(BasinState {
            first_is_current,
            header: basin_header,
            strata,
        })
    }

    fn validate_last_log_entry<File: io::File>(
        log: &CommitLog,
        state: &DiskState,
        file: &mut File,
        scratch: &mut Vec<u8>,
        page_cache: &PageCache,
    ) -> io::Result<()> {
        let (batch_id, entry_index) = if let Some(entry) = log.most_recent_entry() {
            entry
        } else {
            // No commit log entries, nothing to validate.
            return Ok(());
        };

        let entry = CommitLogEntry::load_from(entry_index, true, file, scratch)?;

        for change in &entry.grain_changes {
            Self::validate_grain_change(change, batch_id, state, file, scratch, page_cache)?;
        }

        Ok(())
    }

    fn validate_grain_change<File: io::File>(
        change: &GrainChange,
        batch_id: BatchId,
        state: &DiskState,
        file: &mut File,
        scratch: &mut Vec<u8>,
        page_cache: &PageCache,
    ) -> io::Result<()> {
        let basin = state
            .basins
            .get(usize::from(change.start.basin_index()))
            .ok_or_else(|| io::invalid_data_error("log validation failed: invalid basin index"))?;
        let stratum_info = basin
            .header
            .strata
            .get(usize::from(change.start.stratum_index()))
            .ok_or_else(|| {
                io::invalid_data_error("log validation failed: invalid stratum index")
            })?;
        let stratum = basin
            .strata
            .get(usize::from(change.start.stratum_index()))
            .ok_or_else(|| {
                io::invalid_data_error("log validation failed: invalid stratum index")
            })?;
        let grain_map_index = change.start.grain_index() / stratum_info.grains_per_map();
        let grain_map_local_index = change.start.grain_index() % stratum_info.grains_per_map();
        let grain_map = stratum
            .grain_maps
            .get(usize::try_from(grain_map_index).to_io()?)
            .ok_or_else(|| io::invalid_data_error("log validation failed: grain map not found"))?;
        if grain_map.map.written_at != batch_id {
            return Err(io::invalid_data_error(
                "log validation failed: grain map not updated",
            ));
        }

        // Verify the grain map is updated. This is a consistency check, and can
        // only fail from faulty logic.
        let should_be_allocated = match change.operation {
            GrainOperation::Allocate | GrainOperation::Archive => true,
            GrainOperation::Free => false,
        };
        for index in grain_map_local_index..grain_map_local_index + u64::from(change.count) {
            let allocated = grain_map.map.allocation_state[usize::try_from(index).to_io()?];
            if allocated != should_be_allocated {
                return Err(io::invalid_data_error(
                    "log validation failed: grain map allocation state inconsistency",
                ));
            }
        }

        let grain_map_page_index = grain_map_local_index / 170;
        let local_grain_index = grain_map_local_index % 170;
        let grain_map_offset = grain_map.offset;
        let page_offset =
            grain_map_offset + stratum_info.header_length() + PAGE_SIZE_U64 * grain_map_page_index;
        let grain_data_offset = grain_map_offset
            + stratum_info.grain_map_data_offset()
            + local_grain_index * u64::from(stratum_info.grain_length());
        let grain_map_page = page_cache.fetch(page_offset, true, file, scratch)?;

        let local_grain_index = usize::try_from(local_grain_index).to_io()?;
        for index in local_grain_index..local_grain_index + usize::try_from(change.count).to_io()? {
            Self::validate_grain_info_state(change, index, batch_id, &grain_map_page)?;
        }

        // Verify the data's CRC to ensure the data is fully present.
        let mut buffer = std::mem::take(scratch);
        buffer.resize(
            usize::try_from(grain_map_page.grains[local_grain_index].length).to_io()?,
            0,
        );
        let (result, buffer) = file.read_exact(buffer, grain_data_offset);
        *scratch = buffer;
        result?;

        if grain_map_page.grains[local_grain_index].crc != crc(scratch) {
            return Err(io::invalid_data_error(
                "log validation failed: grain crc check failed",
            ));
        }

        Ok(())
    }

    fn validate_grain_info_state(
        change: &GrainChange,
        local_index: usize,
        batch_id: BatchId,
        grain_map_page: &GrainMapPage,
    ) -> io::Result<()> {
        match change.operation {
            GrainOperation::Allocate => {
                if grain_map_page.grains[local_index].checked_allocated_at() != Some(batch_id)
                    || grain_map_page.grains[local_index]
                        .checked_archived_at()
                        .is_some()
                {
                    return Err(io::invalid_data_error(
                        "log validation failed: grain not allocated",
                    ));
                }
            }
            GrainOperation::Archive => {
                if grain_map_page.grains[local_index].checked_archived_at() != Some(batch_id)
                    || grain_map_page.grains[local_index]
                        .checked_allocated_at()
                        .is_none()
                {
                    return Err(io::invalid_data_error(
                        "log validation failed: grain not archived",
                    ));
                }
            }
            GrainOperation::Free => {
                if grain_map_page.grains[local_index]
                    .checked_allocated_at()
                    .is_some()
                    || grain_map_page.grains[local_index]
                        .checked_archived_at()
                        .is_some()
                {
                    return Err(io::invalid_data_error(
                        "log validation failed: grain not freed",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BasinState {
    pub first_is_current: bool,
    pub header: Basin,
    pub strata: Vec<StratumState>,
}

#[derive(Debug)]
pub struct StratumState {
    pub grain_maps: Vec<GrainMapState>,
}

#[derive(Debug)]
pub struct GrainMapState {
    pub first_is_current: bool,
    pub offset: u64,
    header_length: u64,
    pub map: GrainMap,
}
impl GrainMapState {
    pub fn new(offset: u64, grain_count: u64) -> io::Result<Self> {
        Ok(Self {
            offset,
            first_is_current: false,
            header_length: GrainMap::header_length_for_grain_count(grain_count),
            map: GrainMap::new(grain_count)?,
        })
    }

    pub fn offset_to_write_at(&self) -> u64 {
        if self.first_is_current {
            self.offset + self.header_length
        } else {
            self.offset
        }
    }
}
