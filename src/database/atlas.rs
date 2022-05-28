use std::{collections::HashMap, sync::Arc};

use lrumap::LruHashMap;

use crate::{
    database::GrainReservation,
    format::{
        Basin, BasinHeader, BasinIndex, GrainId, GrainInfo, GrainMapPage, Header, SequenceId,
    },
    io,
    ranges::Ranges,
};

#[derive(Debug)]
pub struct Atlas {
    file_allocations: Ranges<FileAllocation>,
    basins: Vec<BasinAtlas>,
    grain_map_cache: LruHashMap<u64, Arc<GrainMapPage>>,
}

impl Atlas {
    pub fn load_from<File: io::File>(file: &mut File, header: &Header) -> io::Result<Self> {
        let mut file_allocations = Ranges::new(FileAllocation::Free, Some(file.len()?));
        file_allocations.set(..8192, FileAllocation::Header);

        let mut basins = Vec::new();
        for basin in &header.basins {
            file_allocations.set(
                basin.basin_location()..basin.basin_location() + 8192,
                FileAllocation::Basin,
            );

            let header =
                BasinHeader::read_from(file, basin.basin_location(), &mut Vec::new(), true)?;
            let mut stratum = Vec::new();
            // TODO read stratum
            let header = basins.push(BasinAtlas {
                location: basin.basin_location(),
                header,
                stratum,
            });
        }

        Ok(Self {
            file_allocations,
            basins,
            grain_map_cache: LruHashMap::new(64),
        })
    }

    pub fn reserve_grain(&mut self, length: u64) -> io::Result<GrainReservation> {
        let mut best_grain_id = Option::<GrainId>::None;

        for (index, basin) in self.basins.iter_mut().enumerate() {
            todo!("scan the basin")
        }

        if best_grain_id.is_none() {
            best_grain_id = Some(self.allocate_new_grain_map(length)?);
        }

        todo!("Update the atlas")
    }

    fn allocate_new_grain_map(&mut self, length: u64) -> io::Result<GrainId> {
        // Attempt to allocate a new strata in an existing basin
        for (basin_index, basin) in self.basins.iter_mut().enumerate() {
            if basin.stratum.len() < 255 {
                todo!("allocate a new stratum in an existing basin")
            }
        }

        // Attempt to allocate a new basin
        if self.basins.len() < 255 {
            todo!("allocate a new basin")
        }

        todo!("Extend an existing grain map")
    }

    fn allocate(&mut self, length: u64, kind: FileAllocation) -> io::Result<u64> {
        todo!("allocate new disk space")
    }
}

#[derive(Debug)]
pub struct BasinAtlas {
    location: u64,
    header: BasinHeader,
    stratum: Vec<StrataAtlas>,
}

#[derive(Debug)]
pub struct StrataAtlas {
    allocations: Vec<Ranges<GrainAllocation>>,
}

/// The lifecycle of a grain.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum GrainAllocation {
    /// The grain's data is considered unallocated.
    ///
    /// The only valid state transition is to Reserved.
    Free,

    /// The grain is reserved for writing to. A grain can only be reserved by
    /// one writer at a time.
    ///
    /// If the write is committed, the state transitions to Assigned. If the
    /// write is abandoned, the state transitions to Free.
    Reserved,

    /// The grain has data written to it. It can be read from in this state or
    /// marked for deletion.
    ///
    /// If the grain is mared for deletion, the state transitions to
    /// PendingArchive.
    Assigned,

    /// The grain map will be updated on the next commit to mark this grain as
    /// archived.
    PendingArchive,

    /// This grain previously had data stored in it. The grain will be freed
    /// once the database is checkpointed.
    ///
    /// Once the database is checkpointed beyond the sequence ID for the
    /// archival operation, the state transitions to Free.
    Archived,
}

impl Default for GrainAllocation {
    fn default() -> Self {
        Self::Free
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum FileAllocation {
    Free,
    Header,
    Basin,
    Strata,
    Grains,
}

impl Default for FileAllocation {
    fn default() -> Self {
        Self::Free
    }
}
