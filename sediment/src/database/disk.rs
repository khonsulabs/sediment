use crate::{
    database::{allocations::FileAllocations, log::CommitLog},
    format::{Allocation, Basin, GrainMap, Header, PAGE_SIZE, PAGE_SIZE_U64},
    io,
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

        let first_sequence = first_header.as_ref().map(|header| header.sequence).ok();
        let second_sequence = second_header.as_ref().map(|header| header.sequence).ok();
        let (current, previous, current_is_first) = match (first_sequence, second_sequence) {
            (Some(first_sequence), Some(second_sequence)) if first_sequence > second_sequence => {
                (first_header, second_header, true)
            }
            (Some(_), Some(_)) => (second_header, first_header, false),
            // When either header has an error, put the error header first. This
            // ensures the error messages below will report the failure in
            // loading the header before attempting to restore from the header
            // that did parse.
            (Some(_), None) => (second_header, first_header, false),
            (None, Some(_)) | (None, None) => (first_header, second_header, true),
        };

        match current
            .and_then(|header| Self::load_from_header(header, current_is_first, file, scratch))
        {
            Ok(loaded) => Ok(loaded),
            Err(current_header_error) => {
                eprintln!(
                    "Error loading current header: {current_header_error}. Attempting rollback."
                );
                match previous
                .and_then(|header| Self::load_from_header(header, !current_is_first, file, scratch)) {
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
    ) -> io::Result<(Self, CommitLog, FileAllocations)> {
        let file_allocations = FileAllocations::new(file.len()?);
        // Allocate the file header
        file_allocations.set(0..PAGE_SIZE_U64 * 2, Allocation::Allocated);

        let mut basins = Vec::new();
        for basin in &header.basins {
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
                (Ok(first_basin), _) if first_basin.sequence == basin.sequence_id => {
                    (first_basin, true)
                }
                (_, Ok(second_basin)) if second_basin.sequence == basin.sequence_id => {
                    (second_basin, false)
                }
                (Ok(first_basin), Ok(second_basin)) => {
                    return Err(io::invalid_data_error(format!(
                        "neither basin matches expected batch. First: {}, Second: {}, Expected: {}",
                        first_basin.sequence, second_basin.sequence, basin.sequence_id
                    )))
                }
                (Err(err), _) | (_, Err(err)) => {
                    return Err(err);
                }
            };

            let mut strata = Vec::with_capacity(basin_header.strata.len());
            for stratum in &basin_header.strata {
                assert_eq!(stratum.grain_map_count, 1, "need to support multiple maps");
                file_allocations.set(
                    stratum.grain_map_location
                        ..stratum.grain_map_location + stratum.grain_map_length(),
                    Allocation::Allocated,
                );

                let header_length =
                    GrainMap::header_length_for_grain_count(stratum.grains_per_map());

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
                        if first_map.sequence > second_map.sequence
                            && first_map.sequence <= header.sequence
                        {
                            (first_map, true)
                        } else if second_map.sequence <= header.sequence {
                            (second_map, false)
                        } else {
                            return Err(io::invalid_data_error(
                                "GrainMap error: neither batch is valid",
                            ));
                        }
                    }
                    (Ok(first_map), _) if first_map.sequence <= header.sequence => {
                        (first_map, true)
                    }
                    (_, Ok(second_map)) if second_map.sequence <= header.sequence => {
                        (second_map, false)
                    }
                    (Err(err), _) | (_, Err(err)) => {
                        return Err(err);
                    }
                };

                strata.push(StratumState {
                    grain_maps: vec![GrainMapState {
                        first_is_current,
                        offset: stratum.grain_map_location,
                        header_length,
                        map,
                    }],
                });
            }
            basins.push(BasinState {
                first_is_current,
                header: basin_header,
                strata,
            });
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

        Ok((
            DiskState {
                first_header_is_current: is_first_header,
                header,
                basins,
            },
            log,
            file_allocations,
        ))
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
    pub fn new(offset: u64, grain_count: u64) -> Self {
        Self {
            offset,
            first_is_current: false,
            header_length: GrainMap::header_length_for_grain_count(grain_count),
            map: GrainMap::new(grain_count),
        }
    }

    pub fn offset_to_write_at(&self) -> u64 {
        if self.first_is_current {
            self.offset + self.header_length
        } else {
            self.offset
        }
    }
}
