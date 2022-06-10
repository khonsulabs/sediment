use crate::{
    database::log::CommitLog,
    format::{BasinHeader, BatchId, FileHeader, GrainMapHeader},
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
    pub header: FileHeader,
    pub basins: Vec<BasinState>,
}

impl DiskState {
    pub fn recover<File: io::File>(
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<(Self, CommitLog)> {
        let header = if file.is_empty()? {
            // Initialize the an empty database.
            let mut header = FileHeader::default();
            header.flush_to_file(BatchId::first(), file, scratch)?;
            file.synchronize()?;
            header
        } else {
            FileHeader::read_from(file, scratch)?
        };

        let mut basins = Vec::new();
        for basin in &header.current().basins {
            let header = BasinHeader::read_from(file, basin.file_offset, &mut Vec::new(), true)?;
            let mut strata = Vec::with_capacity(header.current().strata.len());
            for stratum in &header.current().strata {
                assert_eq!(stratum.grain_map_count, 1, "need to support multiple maps");

                let grain_map = GrainMapHeader::read_from(
                    file,
                    stratum.grain_map_location,
                    stratum.grains_per_map(),
                    scratch,
                )?;

                strata.push(StratumState {
                    grain_maps: vec![grain_map],
                });
            }
            basins.push(BasinState { header, strata });
        }

        let log = if header.current().log_offset > 0 {
            CommitLog::read_from(
                header.current().log_offset,
                file,
                scratch,
                header.current().checkpoint,
            )?
        } else {
            CommitLog::default()
        };

        Ok((DiskState { header, basins }, log))
    }
}

#[derive(Debug)]
pub struct BasinState {
    pub header: BasinHeader,
    pub strata: Vec<StratumState>,
}

#[derive(Debug)]
pub struct StratumState {
    pub grain_maps: Vec<GrainMapHeader>,
}
