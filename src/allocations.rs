use crate::format::{GrainAllocationStatus, GrainIndex, LocalGrainId, StratumHeader};

#[derive(Debug)]
pub struct FreeLocations {
    free_locations: Vec<LocationInfo>,
    grains_free: u16,
}

impl Default for FreeLocations {
    fn default() -> Self {
        Self {
            free_locations: vec![LocationInfo {
                offset: GrainIndex::new(0).expect("0 is valid"),
                length: 16_372,
            }],
            grains_free: 16_372,
        }
    }
}

impl FreeLocations {
    pub fn from_stratum(stratum: &StratumHeader) -> Self {
        let mut free_locations = Vec::new();

        let mut index = 0;
        while index < 16_372 {
            let index_status = stratum.grains[index];
            let count = index_status.count();
            let free = matches!(
                index_status.status().expect("invalid header"),
                GrainAllocationStatus::Free
            );

            if free {
                // See how many free grains in a row we have.
                let free_until = if let Some(next_allocated_index) = stratum.grains[index + 1..]
                    .iter()
                    .enumerate()
                    .find_map(|(index, info)| {
                        matches!(
                            info.status().expect("invalid header"),
                            GrainAllocationStatus::Allocated | GrainAllocationStatus::Archived
                        )
                        .then_some(index)
                    }) {
                    next_allocated_index
                } else {
                    16_372
                };

                insert_location_info(
                    &mut free_locations,
                    LocationInfo {
                        offset: GrainIndex::new(index as u16).expect("valid index"),
                        length: (free_until - index) as u16,
                    },
                );

                index = free_until;
            } else {
                index += usize::from(count);
            }
        }

        let grains_free = free_locations.iter().map(|loc| loc.length).sum::<u16>();
        Self {
            free_locations,
            grains_free,
        }
    }

    pub fn allocate(&mut self, number_of_grains: u8) -> Option<LocalGrainId> {
        let number_of_grains_u16 = u16::from(number_of_grains);
        if number_of_grains_u16 <= self.grains_free {
            for (index, location) in self.free_locations.iter().enumerate() {
                if location.length >= number_of_grains_u16 {
                    self.grains_free -= number_of_grains_u16;
                    // This position can be split. If the new length will cause the
                    // location to shift position, we'll remove and reinsert it.
                    let new_length = location.length - number_of_grains_u16;
                    let grain_index = location.offset;

                    if new_length == 0 {
                        self.free_locations.remove(index);
                    } else {
                        self.free_locations[index].offset += number_of_grains;
                        self.free_locations[index].length = new_length;

                        self.check_if_location_needs_to_move(index);
                    }

                    return Some(
                        LocalGrainId::from_parts(grain_index, number_of_grains)
                            .expect("invalid grain count"),
                    );
                }
            }
        }

        None
    }

    fn check_if_location_needs_to_move(&mut self, index: usize) {
        let info = &self.free_locations[index];
        if index > 0 && self.free_locations[index - 1].length > info.length {
            // Needs to move below
            if index == 1 || (index > 1 && self.free_locations[index - 2].length < info.length) {
                // A simple swap of index and index - 1 is enough
                self.free_locations.swap(index - 1, index);
            } else {
                // We need to move this row more than a single location
                let info = self.free_locations.remove(index);
                insert_location_info(&mut self.free_locations, info);
            }
        } else if index + 1 < self.free_locations.len()
            && self.free_locations[index + 1].length < info.length
        {
            // Needs to move above
            if index + 2 == self.free_locations.len()
                || (index + 2 < self.free_locations.len()
                    && self.free_locations[index + 1].length > info.length)
            {
                // A simple swap of index and index - 1 is enough
                self.free_locations.swap(index, index + 1);
            } else {
                // We need to move this row more than a single location
                let info = self.free_locations.remove(index);
                insert_location_info(&mut self.free_locations, info);
            }
        }
    }

    #[must_use]
    pub fn allocate_grain(&mut self, grain_id: LocalGrainId) -> bool {
        let start = grain_id.grain_index();
        let count = u16::from(grain_id.grain_count());
        let end = start.as_u16() + count;

        for (index, info) in self.free_locations.iter_mut().enumerate() {
            let location_end = info.offset.as_u16() + info.length;
            if location_end >= start.as_u16() && info.offset.as_u16() <= end {
                // TODO allocate from this chunk
                if info.offset == start {
                    // Allocate from the start
                    info.offset = GrainIndex::new(end).expect("valid index");
                    info.length -= count;
                    if info.length > 0 {
                        self.check_if_location_needs_to_move(index);
                    } else {
                        // The location is now empty.
                        self.free_locations.remove(index);
                    }
                } else if location_end == end {
                    // Allocate from the end
                    info.length -= count;

                    // We can assume a non-zero length because otherwise
                    // info.offset would have been equal to start.
                    self.check_if_location_needs_to_move(index);
                } else {
                    // Split this into two
                    let remaining_start_grains = start.as_u16() - info.offset.as_u16();
                    let remaining_end_grains = location_end - end;

                    info.length = remaining_start_grains;
                    self.check_if_location_needs_to_move(index);

                    // Add the new region for the tail end of the split.
                    insert_location_info(
                        &mut self.free_locations,
                        LocationInfo {
                            offset: GrainIndex::new(end).expect("valid index"),
                            length: remaining_end_grains,
                        },
                    );
                }
                self.grains_free -= count;
                return true;
            }
        }

        false
    }

    pub fn free_grain(&mut self, grain_id: LocalGrainId) {
        let start = grain_id.grain_index();
        let count = u16::from(grain_id.grain_count());
        let end = start.as_u16() + count;

        self.grains_free += count;

        // First, attempt to find a grain whose end matches the start or whose
        // start matches the end.
        for (index, info) in self.free_locations.iter_mut().enumerate() {
            let location_end = info.offset.as_u16() + info.length;
            // Check for any overlap of the two regions
            if info.offset.as_u16() <= end && location_end >= start.as_u16() {
                // If we don't match the end or start, this is an invalid
                // operation.
                if info.offset.as_u16() == end {
                    // Prepend the free grains
                    info.offset = start;
                    info.length += count;
                    self.scan_for_merge_with_start(index, start.as_u16());
                } else if location_end == start.as_u16() {
                    // Append the free grains
                    info.length += count;
                    self.scan_for_merge_with_end(index, end);
                } else {
                    unreachable!("invalid free operation: grain was already partially free")
                }

                return;
            }
        }

        // If we couldn't find an existing location to latch onto, we need to
        // insert it.
        insert_location_info(
            &mut self.free_locations,
            LocationInfo {
                offset: start,
                length: count,
            },
        )
    }

    fn scan_for_merge_with_start(&mut self, possible_merge_index: usize, start: u16) {
        for index in possible_merge_index + 1..self.free_locations.len() {
            let info = &self.free_locations[index];

            if info.offset.as_u16() + info.length == start {
                let new_start = info.offset;
                let count = info.length;
                self.free_locations.remove(index);
                self.free_locations[possible_merge_index].offset = new_start;
                self.free_locations[possible_merge_index].length += count;
                return;
            }
        }
    }

    fn scan_for_merge_with_end(&mut self, possible_merge_index: usize, end: u16) {
        for index in possible_merge_index + 1..self.free_locations.len() {
            let info = &self.free_locations[index];

            if info.offset.as_u16() == end {
                let count = info.length;
                self.free_locations.remove(index);
                self.free_locations[possible_merge_index].length += count;
                return;
            }
        }
    }

    pub const fn is_full(&self) -> bool {
        self.grains_free == 0
    }
}

fn insert_location_info(free_locations: &mut Vec<LocationInfo>, info: LocationInfo) {
    let insert_at = free_locations
        .binary_search_by(|loc| loc.length.cmp(&info.length))
        .map_or_else(|a| a, |a| a);
    free_locations.insert(insert_at, info);
}

#[derive(Debug)]
struct LocationInfo {
    offset: GrainIndex,
    length: u16,
}

#[test]
fn basics() {
    let mut allocator = FreeLocations::default();
    let first = allocator.allocate(16).expect("failed to allocate");
    println!("Allocated {first}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 1);
    let second = allocator.allocate(16).expect("failed to allocate");
    println!("Allocated {second}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 1);
    allocator.free_grain(first);
    println!("Freed {first}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    let reused_a = allocator.allocate(8).expect("failed to allocate");
    println!("Allocated {reused_a}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    let reused_b = allocator.allocate(8).expect("failed to allocate");
    println!("Allocated {reused_b}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 1);
    assert_eq!(allocator.grains_free, 16_372 - 32);

    // Free the grains in order such that the second free ends up joining the
    // two existing nodes into one.
    allocator.free_grain(reused_b);
    println!("Freed {reused_b}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    allocator.free_grain(second);
    println!("Freed {second}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 1);
    allocator.free_grain(reused_a);
    println!("Freed {reused_a}: {allocator:?}");
    assert_eq!(allocator.grains_free, 16_372);

    // Re-allocate using these grain ids
    assert!(allocator.allocate_grain(second));
    println!("Allocated {second}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    assert!(allocator.allocate_grain(reused_a));
    println!("Allocated {reused_a}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    assert!(allocator.allocate_grain(reused_b));
    println!("Allocated {reused_b}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 1);
    assert_eq!(allocator.grains_free, 16_372 - 32);

    // Free in a different order this time.
    allocator.free_grain(reused_a);
    println!("Freed {reused_a}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    allocator.free_grain(second);
    println!("Freed {second}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 2);
    allocator.free_grain(reused_b);
    println!("Freed {reused_b}: {allocator:?}");
    assert_eq!(allocator.free_locations.len(), 1);
    assert_eq!(allocator.grains_free, 16_372);
}
