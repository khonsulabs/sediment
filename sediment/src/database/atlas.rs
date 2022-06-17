use crate::{
    database::{
        allocations::FileAllocations, disk::DiskState, page_cache::PageCache, GrainReservation,
    },
    format::{Allocation, Basin, BatchId, GrainId, GrainMapPage, StratumIndex, PAGE_SIZE_U64},
    io::{self, ext::ToIoResult},
    ranges::Ranges,
    todo_if,
    utils::{Multiples, RangeLength},
};

/// Controls the in-memory representation of grain locations and allocations.
///
/// All state contained by this type is ephemeral. A fresh state will be
/// reloaded each time the database is opened.
#[derive(Debug)]
pub struct Atlas {
    pub basins: Vec<BasinAtlas>,
}

impl Atlas {
    pub fn from_state(state: &DiskState) -> Self {
        let mut basins = Vec::new();
        for (basin_index, basin) in state.header.basins.iter().enumerate() {
            let basin_state = &state.basins[basin_index];

            let mut strata = Vec::with_capacity(basin_state.strata.len());

            for (stratum_state, stratum_index) in basin_state
                .strata
                .iter()
                .zip(basin_state.header.strata.iter())
            {
                let grain_maps = stratum_state
                    .grain_maps
                    .iter()
                    .map(|grain_map| GrainMapAtlas {
                        new: false,
                        offset: stratum_index.grain_map_location,
                        allocations: grain_map
                            .map
                            .allocation_state
                            .iter()
                            .map(|bit| {
                                if *bit {
                                    Allocation::Allocated
                                } else {
                                    Allocation::Free
                                }
                            })
                            .collect(),
                    })
                    .collect::<Vec<_>>();

                let (free_grains, allocated_grains) = grain_maps.iter().fold((0, 0), |acc, map| {
                    map.allocations
                        .iter()
                        .fold(acc, |(free, allocated), (range, tag)| match tag {
                            Allocation::Free => (free + range.len(), allocated),
                            Allocation::Allocated => (free, allocated + range.len()),
                        })
                });

                todo_if!(grain_maps.len() > 1, "need to handle multiple map offsets https://github.com/khonsulabs/sediment/issues/11");

                strata.push(StratumAtlas {
                    free_grains,
                    allocated_grains,
                    grain_length: stratum_index.grain_length(),
                    grains_per_map: stratum_index.grains_per_map(),
                    grain_map_header_length: stratum_index.header_length(),
                    grain_maps,
                });
            }

            basins.push(BasinAtlas {
                location: basin.file_offset,
                header: basin_state.header.clone(),
                strata,
            });
        }

        Self { basins }
    }

    pub fn reserve_grain<File: io::File>(
        &mut self,
        length: u32,
        file: &mut File,
        file_allocations: &FileAllocations,
    ) -> io::Result<GrainReservation> {
        let mut best_grain_id = Option::<GrainId>::None;

        for (basin_index, basin) in self.basins.iter_mut().enumerate() {
            for (stratum_index, stratum) in basin.strata.iter_mut().enumerate() {
                let grains_needed = (length + 8).ceil_div_by(stratum.grain_length);
                if grains_needed > 4 {
                    continue;
                }

                if let Some(grain_index) = stratum.grain_maps.iter().enumerate().find_map(
                    |(grain_map_index, grain_map)| {
                        let grain_map_index = u64::try_from(grain_map_index).unwrap();
                        grain_map
                            .allocations
                            .iter()
                            .find_map(|(range, allocation)| match (range.len(), allocation) {
                                (free_grains, Allocation::Free)
                                    if free_grains >= u64::from(grains_needed) =>
                                {
                                    Some(*range.start() + grain_map_index * stratum.grains_per_map)
                                }
                                _ => None,
                            })
                    },
                ) {
                    best_grain_id = Some(GrainId::from_parts(
                        basin_index,
                        stratum_index,
                        grain_index,
                    )?);
                }
            }
        }

        let grain_id = if let Some(grain_id) = best_grain_id {
            grain_id
        } else {
            self.allocate_new_grain_map(length, file, file_allocations)?
        };

        let offset = self
            .map_stratum_for_grain(grain_id, |stratum| -> io::Result<u64> {
                let grains_needed = (length + 8)
                    .round_to_multiple_of(stratum.grain_length)
                    .expect("practically impossible to overflow");

                let (offset, grain_map_index) =
                    stratum.offset_and_index_of_grain_data(grain_id.grain_index())?;
                let local_grain_index = grain_id.grain_index() % stratum.grains_per_map;

                stratum.grain_maps[grain_map_index].allocations.set(
                    local_grain_index..local_grain_index + u64::from(grains_needed),
                    Allocation::Allocated,
                );
                Ok(offset)
            })
            .expect("reserved bad grain")?;

        Ok(GrainReservation {
            grain_id,
            offset,
            length,
        })
    }

    pub fn length_of_grain<File: io::File>(
        &mut self,
        grain_id: GrainId,
        current_batch: BatchId,
        page_cache: &PageCache,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<Option<(u64, u64)>> {
        let (offset, page_local_index, grain_map_page_offset, grain_length) = match self
            .map_stratum_for_grain(grain_id, |stratum| -> io::Result<(u64, usize, u64, u32)> {
                let (offset, grain_map_index) =
                    stratum.offset_and_index_of_grain_data(grain_id.grain_index())?;
                let page = grain_id.grain_index() / GrainMapPage::GRAINS_U64;
                let page_local_index =
                    usize::try_from(grain_id.grain_index() % GrainMapPage::GRAINS_U64).unwrap();

                let grain_map_offset = stratum.grain_maps[grain_map_index].offset;
                let page_offset =
                    grain_map_offset + stratum.grain_map_header_length + PAGE_SIZE_U64 * page;

                Ok((offset, page_local_index, page_offset, stratum.grain_length))
            }) {
            Some(Ok(result)) => result,
            Some(Err(err)) => return Err(err),
            None => return Ok(None),
        };

        let consecutive_grains = page_cache.fetch_grain_length(
            grain_map_page_offset,
            page_local_index,
            current_batch,
            file,
            scratch,
        )?;

        Ok(Some((
            offset,
            u64::from(consecutive_grains) * u64::from(grain_length),
        )))
    }

    fn map_stratum_for_grain<F: FnOnce(&mut StratumAtlas) -> R, R>(
        &mut self,
        grain_id: GrainId,
        map: F,
    ) -> Option<R> {
        let basin = self.basins.get_mut(usize::from(grain_id.basin_index()))?;
        let stratum = basin
            .strata
            .get_mut(usize::from(grain_id.stratum_index()))?;
        Some(map(stratum))
    }

    fn allocate_new_grain_map<File: io::File>(
        &mut self,
        length: u32,
        file: &mut File,
        file_allocations: &FileAllocations,
    ) -> io::Result<GrainId> {
        // TODO we shouldn't always pick the largest size. We should support
        // using multiple grains on an initial allocation.
        let ideal_grain_length = (length + 8).min(2_u32.pow(24)).next_power_of_two();
        let grain_length_exp = u8::try_from(ideal_grain_length.trailing_zeros()).unwrap();

        // Attempt to allocate a new stratum in an existing basin
        if let Some(basin_index) = self
            .basins
            .iter_mut()
            .enumerate()
            .find_map(|(index, basin)| (basin.strata.len() < 254).then(|| index))
        {
            let grain_count_exp = 0; // TODO change this. it should increment until the maximum each time we create a new map for a specific size.
            let mut stratum = StratumIndex {
                grain_map_count: 1,
                grain_count_exp,
                grain_length_exp,
                grain_map_location: 0,
            };
            stratum.grain_map_location =
                file_allocations.allocate(stratum.grain_map_length(), file)?;

            let allocations = Ranges::new(Allocation::Free, Some(stratum.grains_per_map()));

            let basin = &mut self.basins[basin_index];
            let stratum_index = basin.strata.len();
            let grain_id = GrainId::from_parts(basin_index, stratum_index, 0)?;
            basin.strata.push(StratumAtlas {
                free_grains: stratum.grains_per_map(),
                allocated_grains: 0,
                grain_length: stratum.grain_length(),
                grains_per_map: stratum.grains_per_map(),
                grain_maps: vec![GrainMapAtlas {
                    new: true,
                    offset: stratum.grain_map_location,
                    allocations,
                }],
                grain_map_header_length: stratum.header_length(),
            });
            basin.header.strata.push(stratum);
            return Ok(grain_id);
        }

        // Attempt to allocate a new basin
        if self.basins.len() < 254 {
            let location = file_allocations.allocate(PAGE_SIZE_U64 * 2, file)?;
            self.basins.push(BasinAtlas::new(location));
            // Recurse. Now that we have a new basin, the previous loop will be
            // able to allocate a strata.
            return self.allocate_new_grain_map(length, file, file_allocations);
        }

        todo!("grow an existing grain map or add an additional grain map to a stratum https://github.com/khonsulabs/sediment/issues/11")
    }

    pub fn forget_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<()> {
        for reservation in reservations {
            if let Some(Err(err)) =
                self.map_stratum_for_grain(reservation.grain_id, |strata| -> io::Result<()> {
                    let grains_needed = reservation
                        .length
                        .round_to_multiple_of(strata.grain_length)
                        .expect("practically impossible to overflow");

                    let (start_grain, grain_map_index) =
                        strata.grain_map_index(reservation.grain_id.grain_index())?;
                    let end_grain = start_grain + u64::from(grains_needed);
                    strata.grain_maps[grain_map_index]
                        .allocations
                        .set(start_grain..end_grain, Allocation::Free);
                    Ok(())
                })
            {
                return Err(err);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct BasinAtlas {
    pub location: u64,
    pub header: Basin,
    pub strata: Vec<StratumAtlas>,
}

impl BasinAtlas {
    pub fn new(location: u64) -> Self {
        Self {
            location,
            header: Basin::default(),
            strata: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct StratumAtlas {
    pub free_grains: u64,
    pub allocated_grains: u64,
    pub grain_length: u32,
    pub grains_per_map: u64,
    pub grain_map_header_length: u64,
    pub grain_maps: Vec<GrainMapAtlas>,
}

impl StratumAtlas {
    pub fn grain_count_exp(&self) -> u8 {
        u8::try_from((self.grains_per_map / GrainMapPage::GRAINS_U64).trailing_zeros()).unwrap()
    }

    pub fn grain_length_exp(&self) -> u8 {
        u8::try_from(self.grain_length.trailing_zeros()).unwrap()
    }

    pub fn grain_map_index(&self, grain_index: u64) -> io::Result<(u64, usize)> {
        let grain_map_index = grain_index / self.grains_per_map;
        let grain_map_index_usize = usize::try_from(grain_map_index).to_io()?;

        let local_grain_index = grain_index % self.grains_per_map;

        Ok((local_grain_index, grain_map_index_usize))
    }

    pub fn offset_and_index_of_grain_data(&self, grain_index: u64) -> io::Result<(u64, usize)> {
        let (local_grain_index, grain_map_index) = self.grain_map_index(grain_index)?;

        let offset = self
            .grain_maps
            .get(grain_map_index)
            .expect("bad grain")
            .offset
            + self.grain_map_header_length
            + u64::try_from(self.grain_maps.len()).unwrap() * PAGE_SIZE_U64 * 2
            + u64::from(self.grain_length) * local_grain_index;

        Ok((offset, grain_map_index))
    }
}

#[derive(Debug)]
pub struct GrainMapAtlas {
    pub new: bool,
    pub offset: u64,
    pub allocations: Ranges<Allocation>,
}
