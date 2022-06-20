use crate::{
    database::{allocations::FileAllocations, log::CommitLog, page_cache::PageCache, GrainData},
    format::{
        Allocation, Basin, BasinIndex, BatchId, CommitLogEntry, GrainChange, GrainMap,
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
    ) -> io::Result<RecoveredState> {
        let (first_header, second_header) = if file.is_empty()? {
            // Initialize the an empty database.
            let header = Header::default();
            header.write_to(0, file, scratch)?;
            header.write_to(PAGE_SIZE_U64, file, scratch)?;
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
            Ok((state, log, file_allocations)) => Ok(RecoveredState {
                recovered_from_error: None,
                disk_state: state,
                log,
                file_allocations,
            }),
            Err(first_header_error) => {
                match previous.and_then(|header| {
                    Self::load_from_header(header, !current_is_first, file, scratch, page_cache)
                }) {

                    Ok((mut state, log, file_allocations)) => {
                        state.overwrite_rolled_back_commit(file, scratch, page_cache)?;

                        Ok(RecoveredState {
                        recovered_from_error: Some(first_header_error),
                        disk_state: state,
                        log,
                        file_allocations,
                    })
                },
                    Err(error) => Err(io::invalid_data_error(format!(
                        "Unrecoverable database. Error loading first header: {first_header_error}. Error loading previous header: {error}."
                    ))),
                }
            }
        }
    }

    /// Overwrites all headers that aren't referred to by the current commit.
    ///
    /// To allow the `BatchId` to be sequential and not need to keep track of
    /// `BatchId`s that were rolled back due to inconsitent disk state, we must
    /// remove all references to the bad data.
    ///
    /// The problem is that with a single fsync, we don't know what we don't
    /// know if we have a valid log entry. The easiest solution is to validate
    /// each header, and if any fail to parse or reference a batch ID greater
    /// than the current state, we overwrite the bad copy with the current copy.
    ///
    /// The current implementation is dumb and overwrites more data than
    /// neccessary -- it doesn't perform the check in the previous paragraph,
    /// instead opting to overwrite all inactive headers. Despite this, this
    /// approach will likely be good enough for a long time, as there are
    /// benefits to doing this overwrite: It helps protect against bit rot for
    /// the pages that aren't being updated on a regular basis.
    fn overwrite_rolled_back_commit<File: io::File>(
        &mut self,
        file: &mut File,
        scratch: &mut Vec<u8>,
        page_cache: &PageCache,
    ) -> io::Result<()> {
        self.header.write_to(
            if self.first_header_is_current {
                PAGE_SIZE_U64
            } else {
                0
            },
            file,
            scratch,
        )?;

        for (basin_index, basin) in self.basins.iter().enumerate() {
            basin.header.write_to(
                self.header.basins[basin_index].file_offset
                    + if basin.first_is_current {
                        PAGE_SIZE_U64
                    } else {
                        0
                    },
                file,
            )?;

            for (stratum_index, stratum) in basin.strata.iter().enumerate() {
                for grain_map in &stratum.grain_maps {
                    grain_map.map.write_to(
                        basin.header.strata[stratum_index].grain_map_location
                            + if basin.first_is_current {
                                basin.header.strata[stratum_index].header_length()
                            } else {
                                0
                            },
                        file,
                    )?;

                    for page in 0..basin.header.strata[stratum_index].grain_map_pages() {
                        let offset = basin.header.strata[stratum_index].grain_map_location
                            + basin.header.strata[stratum_index].total_header_length()
                            + PAGE_SIZE_U64 * 2 * page;
                        let mut loaded_page =
                            page_cache.fetch(offset, self.header.batch, file, scratch)?;
                        loaded_page.write_to(offset, self.header.batch, file)?;
                    }
                }
            }
        }

        Ok(())
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
        if let Some((batch_id, entry_index)) = log.most_recent_entry() {
            let entry = CommitLogEntry::load_from(entry_index, true, file, scratch)?;

            for change in &entry.grain_changes {
                Self::validate_grain_change(change, batch_id, state, file, scratch, page_cache)?;
            }
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
            GrainOperation::Allocate { .. } | GrainOperation::Archive => true,
            GrainOperation::Free => false,
        };
        let check_crc = if let GrainOperation::Allocate { crc } = change.operation {
            Some(crc)
        } else {
            None
        };
        for index in grain_map_local_index..grain_map_local_index + u64::from(change.count) {
            let allocated = grain_map.map.allocation_state[usize::try_from(index).to_io()?];
            if allocated != should_be_allocated {
                return Err(io::invalid_data_error(
                    "log validation failed: grain map allocation state inconsistency",
                ));
            }
        }

        let grain_map_page_index = grain_map_local_index / GrainMapPage::GRAINS_U64;
        let local_grain_index = grain_map_local_index % GrainMapPage::GRAINS_U64;
        let grain_map_offset = grain_map.offset;
        let page_offset = grain_map_offset
            + stratum_info.total_header_length()
            + PAGE_SIZE_U64 * grain_map_page_index;
        let grain_data_offset = grain_map_offset
            + stratum_info.grain_map_data_offset()
            + local_grain_index * u64::from(stratum_info.grain_length());
        let loaded_page = page_cache.fetch(page_offset, state.header.batch, file, scratch)?;

        let local_grain_index = usize::try_from(local_grain_index).to_io()?;

        if let GrainOperation::Free = change.operation {
            for index in 0..usize::from(change.count) {
                if loaded_page.page.consecutive_allocations[local_grain_index + index] != 0 {
                    return Err(io::invalid_data_error(
                        "log validation failed: grain not free",
                    ));
                }
            }
        } else {
            for (index, expected_value) in
                (0..usize::from(change.count)).zip((1..=change.count).rev())
            {
                if loaded_page.page.consecutive_allocations[local_grain_index + index]
                    != expected_value
                {
                    return Err(io::invalid_data_error(
                        "log validation failed: grain allocation state mismatch",
                    ));
                }
            }
        }

        // If this grain was allocated, ensure that the CRC is the CRC we are
        // expecting, and that the data validates the CRC.
        if let Some(check_crc) = check_crc {
            // Verify the data's CRC to ensure the data is fully present.
            let mut buffer = std::mem::take(scratch);
            let total_length = usize::try_from(
                u64::from(loaded_page.page.consecutive_allocations[local_grain_index])
                    * u64::from(stratum_info.grain_length()),
            )
            .to_io()?;
            buffer.resize(total_length, 0);
            let (result, buffer) = file.read_exact(buffer, grain_data_offset);
            result?;

            let grain_data = GrainData::from_bytes(buffer);
            if !grain_data.is_crc_valid() || grain_data.crc != check_crc {
                return Err(io::invalid_data_error(
                    "log validation failed: grain crc check failed",
                ));
            }
            *scratch = grain_data.data;
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

#[derive(Debug)]
pub struct RecoveredState {
    pub recovered_from_error: Option<std::io::Error>,
    pub disk_state: DiskState,
    pub log: CommitLog,
    pub file_allocations: FileAllocations,
}
