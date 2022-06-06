use crate::{
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
    pub fn recover<File: io::File>(file: &mut File, scratch: &mut Vec<u8>) -> io::Result<Self> {
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
            let mut stratum = Vec::with_capacity(header.current().stratum.len());
            for strata in &header.current().stratum {
                assert_eq!(strata.grain_map_count, 1, "need to support multiple maps");

                let grain_map = GrainMapHeader::read_from(
                    file,
                    strata.grain_map_location,
                    strata.grains_per_map(),
                    scratch,
                )?;

                stratum.push(StrataState {
                    grain_maps: vec![grain_map],
                });
            }
            basins.push(BasinState { header, stratum });
        }

        Ok(DiskState { header, basins })
    }
}

#[derive(Debug)]
pub struct BasinState {
    pub header: BasinHeader,
    pub stratum: Vec<StrataState>,
}

#[derive(Debug)]
pub struct StrataState {
    pub grain_maps: Vec<GrainMapHeader>,
}
