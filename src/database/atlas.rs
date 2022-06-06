use std::cmp::Ordering;

use crate::{
    database::{disk::DiskState, GrainReservation},
    format::{Allocation, BasinHeader, GrainId, StrataIndex, PAGE_SIZE_U64},
    io::{self, ext::ToIoResult},
    ranges::{Ranges, Span},
    todo_if,
    utils::RoundToMultiple,
};

/// Controls the in-memory representation of grain locations and allocations.
///
/// All state contained by this type is ephemeral. A fresh state will be
/// reloaded each time the database is opened.
#[derive(Debug)]
pub struct Atlas {
    file_allocations: Ranges<Allocation>,
    pub basins: Vec<BasinAtlas>,
}

impl Atlas {
    pub fn from_state<File: io::File>(state: &DiskState, file: &mut File) -> io::Result<Self> {
        let mut file_allocations = Ranges::new(Allocation::Free, Some(file.len()?));
        // Allocate the file header
        file_allocations.set(..PAGE_SIZE_U64 * 2, Allocation::Allocated);

        let mut basins = Vec::new();
        for (basin_index, basin) in state.header.current().basins.iter().enumerate() {
            file_allocations.set(
                basin.file_offset..basin.file_offset + PAGE_SIZE_U64 * 2,
                Allocation::Allocated,
            );
            let basin_state = &state.basins[basin_index];

            let mut stratum = Vec::with_capacity(basin_state.stratum.len());

            for (strata_state, strata_index) in basin_state
                .stratum
                .iter()
                .zip(basin_state.header.current().stratum.iter())
            {
                let grain_maps = strata_state
                    .grain_maps
                    .iter()
                    .map(|grain_map| {
                        file_allocations.set(
                            strata_index.grain_map_location
                                ..strata_index.grain_map_location + strata_index.grain_map_length(),
                            Allocation::Allocated,
                        );
                        GrainMapAtlas {
                            new: false,
                            offset: strata_index.grain_map_location,
                            allocations: grain_map
                                .current()
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
                        }
                    })
                    .collect::<Vec<_>>();

                let (free_grains, allocated_grains) = grain_maps.iter().fold((0, 0), |acc, map| {
                    map.allocations
                        .iter()
                        .fold(acc, |(free, allocated), (range, tag)| match tag {
                            Allocation::Free => (free + range.end() - range.start(), allocated),
                            Allocation::Allocated => {
                                (free, allocated + range.end() - range.start())
                            }
                        })
                });

                todo_if!(grain_maps.len() <= 1, "need to handle multiple map offsets");

                stratum.push(StrataAtlas {
                    free_grains,
                    allocated_grains,
                    grain_length: strata_index.grain_length(),
                    grains_per_map: strata_index.grains_per_map(),
                    grain_map_header_length: strata_index.header_length(),
                    grain_maps,
                });
            }

            basins.push(BasinAtlas {
                location: basin.file_offset,
                header: basin_state.header.clone(),
                stratum,
            });
        }

        Ok(Self {
            file_allocations,
            basins,
        })
    }

    pub fn reserve_grain<File: io::File>(
        &mut self,
        length: u32,
        file: &mut File,
    ) -> io::Result<GrainReservation> {
        let mut best_grain_id = Option::<GrainId>::None;

        for (basin_index, basin) in self.basins.iter_mut().enumerate() {
            for (strata_index, strata) in basin.stratum.iter_mut().enumerate() {
                let grains_needed = length
                    .round_to_multiple_of(strata.grain_length)
                    .expect("value too large");

                if let Some(grain_index) =
                    strata
                        .grain_maps
                        .iter()
                        .enumerate()
                        .find_map(|(grain_map_index, grain_map)| {
                            let grain_map_index = u64::try_from(grain_map_index).unwrap();
                            grain_map
                                .allocations
                                .iter()
                                .find_map(|(range, allocation)| {
                                    match (range.end() - range.start(), allocation) {
                                        (free_grains, Allocation::Free)
                                            if free_grains >= u64::from(grains_needed) =>
                                        {
                                            Some(
                                                *range.start()
                                                    + grain_map_index * strata.grains_per_map,
                                            )
                                        }
                                        _ => None,
                                    }
                                })
                        })
                {
                    best_grain_id =
                        Some(GrainId::from_parts(basin_index, strata_index, grain_index)?);
                }
            }
        }

        let grain_id = if let Some(grain_id) = best_grain_id {
            grain_id
        } else {
            self.allocate_new_grain_map(length, file)?
        };

        let offset = self.map_strata_for_grain(grain_id, |strata| -> io::Result<u64> {
            let grains_needed = length
                .round_to_multiple_of(strata.grain_length)
                .expect("practically impossible to overflow");

            let (offset, grain_map_index) =
                strata.offset_and_index_of_grain_data(grain_id.grain_index())?;

            strata.grain_maps[grain_map_index]
                .allocations
                .set(..u64::from(grains_needed), Allocation::Allocated);
            Ok(offset)
        })?;

        Ok(GrainReservation {
            grain_id,
            offset,
            length,
            crc: 0,
        })
    }

    fn map_strata_for_grain<F: FnOnce(&mut StrataAtlas) -> R, R>(
        &mut self,
        grain_id: GrainId,
        map: F,
    ) -> R {
        let basin = self
            .basins
            .get_mut(usize::from(grain_id.basin_index()))
            .expect("reserved bad grain");
        let strata = basin
            .stratum
            .get_mut(usize::from(grain_id.strata_index()))
            .expect("reserved bad grain");
        map(strata)
    }

    fn allocate_new_grain_map<File: io::File>(
        &mut self,
        length: u32,
        file: &mut File,
    ) -> io::Result<GrainId> {
        // TODO we shouldn't always pick the largest size. We should support
        // using multiple grains on an initial allocation.
        let ideal_grain_length = length.min(2_u32.pow(24)).next_power_of_two();
        let grain_length_exp = u8::try_from(ideal_grain_length.trailing_zeros()).unwrap();

        // Attempt to allocate a new strata in an existing basin
        if let Some(basin_index) = self
            .basins
            .iter_mut()
            .enumerate()
            .find_map(|(index, basin)| (basin.stratum.len() < 255).then(|| index))
        {
            let grain_count_exp = 1; // TODO change this. it should increment until the maximum each time we create a new map for a specific size.
            let mut strata = StrataIndex {
                grain_map_count: 1,
                grain_count_exp,
                grain_length_exp,
                grain_map_location: 0,
            };
            let grain_map_header_length = strata.header_length();
            strata.grain_map_location =
                self.allocate(grain_map_header_length + strata.grain_map_length(), file)?;

            let allocations = Ranges::new(Allocation::Free, Some(strata.grains_per_map()));

            let basin = &mut self.basins[basin_index];
            let strata_index = basin.stratum.len();
            let grain_id = GrainId::from_parts(basin_index, strata_index, 0)?;
            basin.stratum.push(StrataAtlas {
                free_grains: strata.grains_per_map(),
                allocated_grains: 0,
                grain_length: strata.grain_length(),
                grains_per_map: strata.grains_per_map(),
                grain_maps: vec![GrainMapAtlas {
                    new: true,
                    offset: strata.grain_map_location,
                    allocations,
                }],
                grain_map_header_length,
            });
            basin.header.next_mut().stratum.push(strata);
            return Ok(grain_id);
        }

        // Attempt to allocate a new basin
        if self.basins.len() < 254 {
            let location = dbg!(self.allocate(PAGE_SIZE_U64 * 2, file))?;
            self.basins.push(BasinAtlas::new(location));
            // Recurse. Now that we have a new basin, the previous loop will be
            // able to allocate a strata.
            return self.allocate_new_grain_map(length, file);
        }

        todo!("grow an existing grain map or add an additional grain map to a strata")
    }

    fn allocate<File: io::File>(&mut self, length: u64, file: &mut File) -> io::Result<u64> {
        let mut best_allocation = None;
        for (range, _) in self
            .file_allocations
            .iter()
            .filter(|(_, allocation)| matches!(allocation, Allocation::Free))
        {
            let available_amount = range.end().saturating_sub(*range.start());
            let comparison = available_amount.cmp(&length);
            if matches!(comparison, Ordering::Greater | Ordering::Equal) {
                best_allocation = Some((available_amount, range));
                if matches!(comparison, Ordering::Equal) {
                    break;
                }
            }
        }

        if let Some((_, range)) = best_allocation {
            let location = *range.start();
            self.file_allocations.set(range, Allocation::Allocated);
            return Ok(location);
        }

        let current_length = self.file_allocations.maximum().unwrap() + 1;
        let start = match self.file_allocations.last() {
            Some(Span {
                tag: Allocation::Free,
                start,
            }) => {
                // Reuse the free space already at the tail of the file
                let start = *start;
                let space_to_use = current_length - start;
                self.file_allocations.set(start.., Allocation::Allocated);
                // Allocate whatever we still need
                let new_amount = length - space_to_use;
                self.file_allocations
                    .extend_by(new_amount, Allocation::Allocated);
                start
            }
            _ => {
                self.file_allocations
                    .extend_by(length, Allocation::Allocated);
                current_length
            }
        };

        file.set_length(start + length)?;

        Ok(start)
    }

    // pub fn commit_reservations<File: io::File>(
    //     &mut self,
    //     reservations: impl Iterator<Item = GrainReservation>,
    //     file: &mut File,
    // ) -> io::Result<SequenceId> {
    //     let new_sequence = self.header.current().sequence.next();
    //     // TODO these reservations need to be able to be batched with other
    //     // writers.

    //     // Rewrite all grain maps affected by the reservations.

    //     Ok(new_sequence)
    // }

    pub fn forget_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<()> {
        for reservation in reservations {
            self.map_strata_for_grain(reservation.grain_id, |strata| -> io::Result<()> {
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
            })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct BasinAtlas {
    pub location: u64,
    pub header: BasinHeader,
    pub stratum: Vec<StrataAtlas>,
}

impl BasinAtlas {
    pub fn new(location: u64) -> Self {
        Self {
            location,
            header: BasinHeader::default(),
            stratum: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct StrataAtlas {
    pub free_grains: u64,
    pub allocated_grains: u64,
    pub grain_length: u32,
    pub grains_per_map: u64,
    pub grain_map_header_length: u64,
    pub grain_maps: Vec<GrainMapAtlas>,
}

impl StrataAtlas {
    pub fn grain_count_exp(&self) -> u8 {
        u8::try_from((self.grains_per_map / 170).trailing_zeros()).unwrap()
    }

    pub fn grain_length_exp(&self) -> u8 {
        u8::try_from(self.grain_length.trailing_zeros()).unwrap()
    }

    pub fn offset_of_grain_data(&self, grain_index: u64) -> io::Result<u64> {
        Ok(self.offset_and_index_of_grain_data(grain_index)?.0)
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
