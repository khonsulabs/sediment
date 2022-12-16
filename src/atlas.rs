use std::{
    collections::{HashMap, VecDeque},
    io::Read,
    sync::{Mutex, PoisonError},
};

use okaywal::{ChunkReader, LogPosition, WriteAheadLog};
use tinyvec::ArrayVec;

use crate::{
    allocations::FreeLocations,
    basinmap::BasinMap,
    format::{BasinId, GrainId, StratumHeader, StratumId},
    store::{BasinState, Store},
    Result,
};

#[derive(Debug)]
pub struct Atlas {
    data: Mutex<Data>,
}

impl Atlas {
    pub fn new(store: &Store) -> Self {
        let disk_state = store.lock();

        let mut basins = BasinMap::new();

        for (basin_id, basin) in &disk_state.basins {
            basins[basin_id] = Some(Basin::from(basin));
        }

        Self {
            data: Mutex::new(Data {
                basins,
                uncheckpointed_grains: HashMap::new(),
            }),
        }
    }

    pub fn find(&self, grain: GrainId, wal: &WriteAheadLog) -> Result<Option<GrainReader>> {
        let data = self.data.lock().map_or_else(PoisonError::into_inner, |a| a);
        match data.uncheckpointed_grains.get(&grain) {
            Some(UncheckpointedGrain::PendingCommit) => Ok(None),
            Some(UncheckpointedGrain::InWal(location)) => {
                let location = *location;
                let mut chunk_reader = wal.read_at(location)?;
                // We hold onto the data lock until after we read from the wal
                // to ensure a checkpoint doesn't happen before we start the
                // read operation.
                drop(data);

                // Skip over the WalChunk info.
                chunk_reader.read_exact(&mut [0; 9])?;

                Ok(Some(GrainReader::InWal(chunk_reader)))
            }
            None => todo!("find grain in stratum"),
        }
    }

    pub fn reserve(&self, length: u32) -> Result<GrainId> {
        // First, determine what basins have been allocated, and within those,
        // which ones are the best fit (least amount of wasted space). For
        // example, storing a 80 byte value as 2 64 byte grains vs 3 32 byte
        // grains would waste 48 bytes in one case and waste 0 bytes in the
        // other.
        let length_with_grain_info = length.checked_add(16).expect("grain too large"); // TODO error
        let mut data = self.data.lock().map_or_else(PoisonError::into_inner, |a| a);
        // Accessing fields through MutexGuard's DerefMut causes issues with the
        // borrow checker extending the lifetime of the borrow across both
        // basins and uncheckpointed_grains. So, we perform the DerefMut to get
        // the Data pointer first, allowing the borrow checker to see that the
        // mutable accesses are unique.
        let data = &mut *data;
        let mut eligible_basins = ArrayVec::<[(BasinId, u32, bool, bool, u32); 8]>::new();
        for basin in 0..=7 {
            let basin_id = BasinId::new(basin).expect("valid basin id");
            let grain_size = basin_id.grain_stripe_bytes();
            let number_of_grains_needed =
                if let Some(padded_length) = length_with_grain_info.checked_add(grain_size - 1) {
                    padded_length / grain_size
                } else {
                    todo!("handle large grains")
                };
            let extra_bytes = number_of_grains_needed * grain_size - length;

            eligible_basins.push((
                basin_id,
                number_of_grains_needed,
                number_of_grains_needed <= 63,
                data.basins[basin_id].is_some(),
                extra_bytes,
            ));
        }

        eligible_basins.sort_by(|a, b| b.4.cmp(&a.4));

        // Now we have a list of basins to consider.
        for (basin_id, number_of_grains_needed, _, _, _) in eligible_basins
            .iter()
            .filter(|(_, _, can_fit, is_allocated, _)| *can_fit && *is_allocated)
        {
            let basin = data.basins[*basin_id]
                .as_mut()
                .expect("filter should prevent none");

            let mut free_stata = basin.free_strata.iter_mut();
            while let Some(stratum_id) = free_stata.next() {
                let stratum = basin
                    .strata
                    .get_mut(stratum_id.as_usize())
                    .expect("strata should be allocated");
                if let Ok(grain_id) = allocate_grain_within_stratum(
                    stratum,
                    &mut data.uncheckpointed_grains,
                    *basin_id,
                    stratum_id,
                    *number_of_grains_needed as u8,
                ) {
                    return Ok(grain_id);
                } else if stratum.allocations.is_full() {
                    free_stata.remove_current();
                }
            }
        }

        // We couldn't find an existing stratum that was able to fit the
        // allocation. Create a new one.
        let (basin_id, number_of_grains_needed, _, is_allocated, _) = eligible_basins
            .first()
            .expect("at least one basin should fit");
        if !*is_allocated {
            data.basins[*basin_id] = Some(Basin::default());
        }
        let basin = data.basins[*basin_id].as_mut().expect("just allocated");
        let new_id = StratumId::new(basin.strata.len() as u64).expect("valid stratum id");
        basin.strata.push(Stratum::default());
        basin.free_strata.push(new_id);
        Ok(allocate_grain_within_stratum(
            basin.strata.last_mut().expect("just pushed"),
            &mut data.uncheckpointed_grains,
            *basin_id,
            new_id,
            *number_of_grains_needed as u8,
        )
        .expect("empty stratum should have room"))
    }

    pub fn note_grains_written(
        &self,
        written_grains: impl IntoIterator<Item = (GrainId, LogPosition)>,
    ) {
        let mut data = self.data.lock().map_or_else(PoisonError::into_inner, |a| a);
        for (grain, log_position) in written_grains {
            data.uncheckpointed_grains
                .insert(grain, UncheckpointedGrain::InWal(log_position));
        }
    }

    pub fn rollback_grains(&self, written_grains: impl IntoIterator<Item = GrainId>) {
        let mut data = self.data.lock().map_or_else(PoisonError::into_inner, |a| a);
        for grain in written_grains {
            data.uncheckpointed_grains.remove(&grain);
            let basin = data.basins[grain.basin_id()]
                .as_mut()
                .expect("basin missing");
            let stratum = basin
                .strata
                .get_mut(grain.stratum_id().as_usize())
                .expect("stratum missing");

            stratum.allocations.free_grain(grain.local_grain_id());
        }
    }
}

#[derive(Debug)]
struct Data {
    basins: BasinMap<Basin>,
    uncheckpointed_grains: HashMap<GrainId, UncheckpointedGrain>,
}

fn allocate_grain_within_stratum(
    stratum: &mut Stratum,
    uncheckpointed_grains: &mut HashMap<GrainId, UncheckpointedGrain>,

    basin_id: BasinId,
    stratum_id: StratumId,
    number_of_grains_needed: u8,
) -> Result<GrainId, ()> {
    if let Some(index) = stratum.allocations.allocate(number_of_grains_needed) {
        let id = GrainId::new(basin_id, stratum_id, index);
        uncheckpointed_grains.insert(id, UncheckpointedGrain::PendingCommit);
        Ok(id)
    } else {
        Err(())
    }
}

#[derive(Debug, Default)]
struct Basin {
    strata: Vec<Stratum>,
    free_strata: StratumIdRing,
}

impl<'a> From<&'a BasinState> for Basin {
    fn from(state: &'a BasinState) -> Self {
        let mut strata = Vec::new();
        let mut free_strata = StratumIdRing::default();
        for stratum in &state.stratum {
            let stratum = Stratum::from(&stratum.active);

            if !stratum.allocations.is_full() {
                free_strata.push(StratumId::new(strata.len() as u64).expect("valid stratum id"));
            }

            strata.push(stratum);
        }

        Self {
            strata,
            free_strata,
        }
    }
}

#[derive(Debug, Default)]
struct Stratum {
    allocations: FreeLocations,
}

impl<'a> From<&'a StratumHeader> for Stratum {
    fn from(state: &'a StratumHeader) -> Self {
        let allocations = FreeLocations::from_stratum(state);

        Self { allocations }
    }
}

#[derive(Debug)]
pub enum GrainReader {
    InWal(ChunkReader),
}

impl GrainReader {
    pub const fn bytes_remaining(&self) -> u32 {
        match self {
            GrainReader::InWal(reader) => reader.bytes_remaining(),
        }
    }
}

impl Read for GrainReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            GrainReader::InWal(reader) => reader.read(buf),
        }
    }
}

#[derive(Debug)]
enum UncheckpointedGrain {
    PendingCommit,
    InWal(LogPosition),
}

#[derive(Debug)]
enum GrainPosition {
    InWal(LogPosition),
    InStratum,
}

#[derive(Debug, Default)]
struct StratumIdRing(VecDeque<StratumId>);

impl StratumIdRing {
    pub fn push(&mut self, id: StratumId) {
        self.0.push_back(id);
    }

    pub fn iter_mut(&mut self) -> StratumIdIter<'_> {
        StratumIdIter {
            ring: self,
            iterated: 0,
        }
    }
}

struct StratumIdIter<'a> {
    ring: &'a mut StratumIdRing,
    iterated: usize,
}

impl<'a> Iterator for StratumIdIter<'a> {
    type Item = StratumId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterated == self.ring.0.len() {
            None
        } else {
            // Cycle the ring, moving the front to the end. We keep track of how
            // many times we've iterated to ensure we don't return the same id
            // twice.
            self.iterated += 1;
            self.ring.0.rotate_left(1);
            self.ring.0.front().copied()
        }
    }
}

impl<'a> StratumIdIter<'a> {
    /// Removes the current id from the ring.
    ///
    /// # Panics
    ///
    /// Panics if `Iterator::next()` wasn't called at least once before calling
    /// this function.
    pub fn remove_current(&mut self) {
        assert!(self.iterated > 0);
        self.ring.0.pop_front();
        self.iterated -= 1;
    }
}
