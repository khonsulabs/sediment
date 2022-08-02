use std::collections::VecDeque;

use parking_lot::{Condvar, Mutex, MutexGuard};
use rebytes::Allocator;
use tinyvec::ArrayVec;

use crate::{
    database::{
        allocations::{FileAllocationStatistics, FileAllocations},
        committer::GrainBatchOperation,
        disk::DiskState,
        page_cache::PageCache,
        GrainReservation,
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
    data: Mutex<AtlasData>,
    grain_map_allocation_sync: Condvar,
}

impl Atlas {
    pub fn from_state(state: &DiskState) -> Self {
        Self {
            data: Mutex::new(AtlasData::from_state(state)),
            grain_map_allocation_sync: Condvar::new(),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, AtlasData> {
        self.data.lock()
    }

    pub fn reserve_grain<File: io::File>(
        &self,
        length: u32,
        page_cache: &PageCache,
        file: &mut File,
        file_allocations: &FileAllocations,
    ) -> io::Result<GrainReservation> {
        let mut data = self.data.lock();
        let mut allocation_mask = 0;
        let result = AtlasData::reserve_grain(
            &mut data,
            length,
            &self.grain_map_allocation_sync,
            page_cache,
            file,
            file_allocations,
            &mut allocation_mask,
        );
        // If we allocated, we need to notify any waiting threads that we're
        // done allocating. We do this at this level to ensure we can drop the
        // Mutex before we notify on the Condvar, hopefully yielding smoother
        // wakeup behavior.
        if allocation_mask != 0 {
            data.grain_map_sizes_being_allocated ^= allocation_mask;
            drop(data);
            self.grain_map_allocation_sync.notify_all();
        }
        result
    }
}

#[derive(Debug)]
pub struct AtlasData {
    pub basins: Vec<BasinAtlas>,
    pub strata_by_size: Vec<StrataByLength>,
    grain_map_sizes_being_allocated: u32,
}

impl AtlasData {
    pub fn from_state(state: &DiskState) -> Self {
        let mut basins = Vec::with_capacity(255);
        let mut strata_by_size = vec![StrataByLength::default(); 32];
        for (basin_index, basin) in state.header.basins.iter().enumerate() {
            let basin_state = &state.basins[basin_index];

            let mut strata = Vec::with_capacity(basin_state.strata.len());

            for (stratum_state, stratum_index) in basin_state
                .strata
                .iter()
                .zip(basin_state.header.strata.iter())
            {
                let mut maximum_allocation_size_across_grain_maps = 0;
                let grain_maps = stratum_state
                    .grain_maps
                    .iter()
                    .map(|grain_map| {
                        let mut longest_free = None;
                        let mut counting_free = 0_usize;
                        let allocations = grain_map
                            .map
                            .allocation_state
                            .iter()
                            .map(|bit| {
                                if bit {
                                    if longest_free.is_none()
                                        || longest_free.unwrap() < counting_free
                                    {
                                        longest_free = Some(counting_free);
                                    }
                                    counting_free = 0;
                                    Allocation::Allocated
                                } else {
                                    counting_free += 1;
                                    Allocation::Free
                                }
                            })
                            .collect();

                        let maximum_allocation_size = u8::try_from(counting_free).unwrap_or(255);
                        maximum_allocation_size_across_grain_maps =
                            maximum_allocation_size_across_grain_maps.max(maximum_allocation_size);
                        GrainMapAtlas {
                            new: false,
                            offset: stratum_index.grain_map_location,
                            allocations,
                            maximum_allocation_size,
                        }
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

                let by_size = &mut strata_by_size[usize::from(stratum_index.grain_length_exp)];
                by_size.maximum_allocation_size = by_size
                    .maximum_allocation_size
                    .max(maximum_allocation_size_across_grain_maps);
                by_size.indicies.push_front((basin_index, strata.len()));
                strata.push(StratumAtlas {
                    free_grains,
                    allocated_grains,
                    grain_length: stratum_index.grain_length(),
                    grains_per_map: stratum_index.grains_per_map(),
                    grain_map_header_length: stratum_index.total_header_length(),
                    grain_maps,
                });
            }

            basins.push(BasinAtlas {
                location: basin.file_offset,
                header: basin_state.header.clone(),
                strata,
            });
        }

        Self {
            basins,
            strata_by_size,
            grain_map_sizes_being_allocated: 0,
        }
    }

    pub fn reserve_grain<File: io::File>(
        data: &mut MutexGuard<'_, Self>,
        length: u32,
        grain_map_allocation_sync: &Condvar,
        page_cache: &PageCache,
        file: &mut File,
        file_allocations: &FileAllocations,
        thread_allocation_mask: &mut u32,
    ) -> io::Result<GrainReservation> {
        #[derive(Default, Ord, Eq, PartialOrd, PartialEq)]
        struct BestGrainSize {
            leftover_bytes: u32,
            grain_size: u8,
            grains_needed: u8,
        }
        let mut best_grain_id = Option::<GrainId>::None;

        // Determine the minimum and maximum grain sizes we will consider.
        let total_length = length + 8;
        let leading_zeroes = total_length.leading_zeros();
        let maximum_grain_size =
            u8::try_from((32 - leading_zeroes).max(1)).expect("will always be able to fit");
        let minimum_grain_size =
            u8::try_from((32 - (total_length.ceil_div_by(255)).leading_zeros()).max(1))
                .expect("will always be able to fit");

        // Create an ordered list of the grain sizes to consider based on how well packed the data is.
        let mut best_grain_sizes = ArrayVec::<[BestGrainSize; 32]>::default();
        {
            for grain_size in minimum_grain_size..=maximum_grain_size {
                let grain_size_in_bytes = 2_u32.pow(u32::from(grain_size));
                let grains_needed =
                    u8::try_from(total_length.ceil_div_by(grain_size_in_bytes)).unwrap_or(255);
                let allocated_length = u32::from(grains_needed) * grain_size_in_bytes;
                if data.strata_by_size[usize::from(grain_size)].maximum_allocation_size
                    < grains_needed
                {
                    // No strata of this size have enough room for this allocation.
                    continue;
                }
                let leftover_bytes = allocated_length - length;
                let entry = BestGrainSize {
                    leftover_bytes,
                    grain_size,
                    grains_needed,
                };
                let insert_at = best_grain_sizes
                    .binary_search_by(|best| best.cmp(&entry))
                    .map_or_else(|e| e, |i| i);
                best_grain_sizes.insert(insert_at, entry);
            }
        }
        'outer: for BestGrainSize {
            grain_size,
            grains_needed,
            ..
        } in best_grain_sizes
        {
            // Because data is a MutexGuard, accessing strata_by_size causes an
            // immutable borrow that prevents mutable borrows. By forcing the
            // resolution to &mut Self, the borrow checker can resolve the
            // borrows to the individual fields rather than using the mutex
            // guard's lifetime.
            let data = &mut **data;
            let strata_by_size = &mut data.strata_by_size[usize::from(grain_size)];
            let mut strata_to_consider = strata_by_size.indicies.len();
            while strata_to_consider > 0 {
                let (basin_index, stratum_index) =
                    strata_by_size.indicies.front().expect("loop precondition");

                let basin = &mut data.basins[*basin_index];
                let stratum = &mut basin.strata[*stratum_index];
                if let Some(grain_index) = stratum.grain_maps.iter_mut().enumerate().find_map(
                    |(grain_map_index, grain_map)| {
                        if grain_map.maximum_allocation_size >= grains_needed {
                            let grain_map_index = u64::try_from(grain_map_index).unwrap();
                            let grain_index = grain_map.allocations.iter().find_map(
                                |(range, allocation)| match (range.len(), allocation) {
                                    (free_grains, Allocation::Free)
                                        if free_grains >= u64::from(grains_needed) =>
                                    {
                                        Some(
                                            *range.start()
                                                + grain_map_index * stratum.grains_per_map,
                                        )
                                    }
                                    _ => None,
                                },
                            );
                            if grain_index.is_none() {
                                grain_map.maximum_allocation_size = grains_needed - 1;
                            }
                            grain_index
                        } else {
                            None
                        }
                    },
                ) {
                    best_grain_id = Some(
                        GrainId::from_parts(*basin_index, *stratum_index, grain_index)
                            .expect("values should never be out of range"),
                    );
                    break 'outer;
                }

                strata_by_size.indicies.rotate_left(1);
                strata_to_consider -= 1;
            }

            // No strata contained this allocation
            strata_by_size.maximum_allocation_size = grains_needed - 1;
        }

        let grain_id = if let Some(grain_id) = best_grain_id {
            grain_id
        } else {
            Self::allocate_new_grain_map(
                data,
                length,
                grain_map_allocation_sync,
                page_cache,
                file,
                file_allocations,
                thread_allocation_mask,
            )?
        };

        let (offset, grain_count) = data
            .map_stratum_for_grain(grain_id, |stratum| -> io::Result<(u64, u8)> {
                let grains_needed =
                    u8::try_from((length + 8).ceil_div_by(stratum.grain_length)).to_io()?;

                let (offset, grain_map_index) =
                    stratum.offset_and_index_of_grain_data(grain_id.grain_index())?;
                let local_grain_index = grain_id.grain_index() % stratum.grains_per_map;

                stratum.grain_maps[grain_map_index].allocations.set(
                    local_grain_index..local_grain_index + u64::from(grains_needed),
                    Allocation::Allocated,
                );
                Ok((offset, grains_needed))
            })
            .expect("reserved bad grain")?;

        Ok(GrainReservation {
            grain_id,
            offset,
            length,
            crc: None,
            grain_count,
        })
    }

    pub fn grain_allocation_info<File: io::File>(
        &mut self,
        grain_id: GrainId,
        current_batch: BatchId,
        page_cache: &PageCache,
        file: &mut File,
        allocator: &Allocator,
    ) -> io::Result<Option<GrainAllocationInfo>> {
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
            allocator,
        )?;

        Ok(Some(GrainAllocationInfo {
            offset,
            allocated_length: u64::from(consecutive_grains) * u64::from(grain_length),
            count: consecutive_grains,
        }))
    }

    pub fn free_grains(&mut self, grains: &[GrainBatchOperation]) -> io::Result<()> {
        for op in grains {
            if let GrainBatchOperation::Free(change) = op {
                self.map_stratum_for_grain(change.start, |stratum| -> io::Result<()> {
                    let (_, grain_map_index) =
                        stratum.offset_and_index_of_grain_data(change.start.grain_index())?;

                    let map_local_index = change.start.grain_index() % stratum.grains_per_map;

                    // Reset the allocation limit.
                    stratum.grain_maps[grain_map_index].maximum_allocation_size = 255;
                    // Set the region as free.
                    stratum.grain_maps[grain_map_index].allocations.set(
                        map_local_index..map_local_index + u64::from(change.count),
                        Allocation::Free,
                    );
                    Ok(())
                })
                .transpose()
                .map(|_| ())?;
            }
        }
        Ok(())
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

    #[allow(clippy::cast_precision_loss)]
    fn ideal_grain_map_grain_length_exp(length: u32, file_info: &FileAllocationStatistics) -> u8 {
        let length_with_header = length.checked_add(8).expect("length too large");
        let leading_zeroes = length_with_header.leading_zeros();
        let maximum_grain_length =
            u8::try_from((32 - leading_zeroes).max(1)).expect("will always be able to fit");
        let minimum_grain_length =
            u8::try_from((32 - (length_with_header.ceil_div_by(255)).leading_zeros()).max(1))
                .expect("will always be able to fit");

        let total_file_length = (file_info.allocated_space + file_info.free_space) as f64;
        let mut ideal_grain_length = minimum_grain_length;
        let mut ideal_grain_length_overhead = u32::MAX;
        for length_exp in (minimum_grain_length..=maximum_grain_length).rev() {
            let grain_length = 2_u32.pow(u32::from(length_exp));
            let total_allocation = length_with_header
                .round_to_multiple_of(grain_length)
                .unwrap();
            let percent_growth_of_file =
                (u64::from(grain_length) * GrainMapPage::GRAINS_U64) as f64 / total_file_length;
            let overhead = total_allocation - length_with_header;
            if overhead < ideal_grain_length_overhead && percent_growth_of_file < 0.0625 {
                ideal_grain_length = length_exp;
                ideal_grain_length_overhead = overhead;
            }
        }

        ideal_grain_length
    }

    fn allocate_new_grain_map<File: io::File>(
        data: &mut MutexGuard<'_, Self>,
        length: u32,
        grain_map_allocation_sync: &Condvar,
        page_cache: &PageCache,
        file: &mut File,
        file_allocations: &FileAllocations,
        thread_allocation_mask: &mut u32,
    ) -> io::Result<GrainId> {
        let grain_length_exp =
            Self::ideal_grain_map_grain_length_exp(length, &file_allocations.statistics());
        let grain_length_bitmask = 1 << u32::from(grain_length_exp);

        while data.grain_map_sizes_being_allocated & grain_length_bitmask == 1 {
            // TODO once we support multiple grain maps this needs to be updated.
            let existing_strata_count = data.strata_by_size[usize::from(grain_length_exp)]
                .indicies
                .len();
            grain_map_allocation_sync.wait(data);
            if data.strata_by_size[usize::from(grain_length_exp)]
                .indicies
                .len()
                != existing_strata_count
            {
                // A new stratum of this length has been added. Let's assume we can find an allocation in this stratum.

                return Self::allocate_new_grain_map(
                    data,
                    length,
                    grain_map_allocation_sync,
                    page_cache,
                    file,
                    file_allocations,
                    thread_allocation_mask,
                );
            }
        }

        // Signal to other threads that we're allocating already. Before we
        // change the file length, we will release the mutex allowing other
        // allocations to happen while the file allocation happens.
        data.grain_map_sizes_being_allocated |= grain_length_bitmask;
        *thread_allocation_mask |= grain_length_bitmask;

        // Attempt to allocate a new stratum in an existing basin
        if let Some(basin_index) = data
            .basins
            .iter_mut()
            .enumerate()
            .find_map(|(index, basin)| (basin.strata.len() < 254).then(|| index))
        {
            return Self::allocate_new_grain_map_in_basin(
                basin_index,
                grain_length_exp,
                data,
                page_cache,
                file,
                file_allocations,
            );
        }

        // Attempt to allocate a new basin
        if data.basins.len() < 254 {
            let location = file_allocations.allocate(PAGE_SIZE_U64 * 2, file)?;
            let basin_index = data.basins.len();
            data.basins.push(BasinAtlas::new(location));
            return Self::allocate_new_grain_map_in_basin(
                basin_index,
                grain_length_exp,
                data,
                page_cache,
                file,
                file_allocations,
            );
        }

        todo!("grow an existing grain map or add an additional grain map to a stratum https://github.com/khonsulabs/sediment/issues/11")
    }

    fn allocate_new_grain_map_in_basin<File: io::File>(
        basin_index: usize,
        grain_length_exp: u8,
        data: &mut MutexGuard<'_, Self>,
        page_cache: &PageCache,
        file: &mut File,
        file_allocations: &FileAllocations,
    ) -> io::Result<GrainId> {
        let grain_count_exp = 0; // TODO change this. it should increment until the maximum each time we create a new map for a specific size.
        let mut stratum = StratumIndex {
            grain_map_count: 1,
            grain_count_exp,
            grain_length_exp,
            grain_map_location: 0,
        };
        stratum.grain_map_location = file_allocations.allocate(stratum.grain_map_length(), file)?;

        // TODO once we can allocate more than one page, we need to register each page.
        page_cache.register_new_page(stratum.grain_map_location + stratum.total_header_length());

        let allocations = Ranges::new(Allocation::Free, Some(stratum.grains_per_map()));

        let basin = &mut data.basins[basin_index];
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
                maximum_allocation_size: 255,
            }],
            grain_map_header_length: stratum.total_header_length(),
        });
        let stratum_index = basin.header.strata.len();
        basin.header.strata.push(stratum);
        let by_size = &mut data.strata_by_size[usize::from(grain_length_exp)];
        by_size.indicies.push_front((basin_index, stratum_index));
        by_size.maximum_allocation_size = 255;
        Ok(grain_id)
    }

    pub fn forget_reservations(
        &mut self,
        reservations: impl Iterator<Item = GrainReservation>,
    ) -> io::Result<()> {
        for reservation in reservations {
            if let Some(Err(err)) =
                self.map_stratum_for_grain(reservation.grain_id, |strata| -> io::Result<()> {
                    let grains_needed = (reservation.length + 8)
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
            strata: Vec::with_capacity(255),
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
    maximum_allocation_size: u8,
}

#[derive(Debug)]
pub struct GrainAllocationInfo {
    pub offset: u64,
    pub count: u8,
    pub allocated_length: u64,
}

#[derive(Debug, Default, Clone)]
pub struct StrataByLength {
    maximum_allocation_size: u8,
    indicies: VecDeque<(usize, usize)>,
}
