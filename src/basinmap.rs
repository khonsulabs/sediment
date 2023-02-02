use std::ops::{Index, IndexMut};

use crate::format::BasinId;

#[derive(Debug)]
pub struct BasinMap<T> {
    basins: [Option<T>; 8],
}

impl<T> BasinMap<T> {
    pub const fn new() -> Self {
        Self {
            basins: [None, None, None, None, None, None, None, None],
        }
    }

    pub fn get_or_insert_with(&mut self, index: BasinId, default: impl FnOnce() -> T) -> &mut T {
        if self[index].is_none() {
            self[index] = Some(default());
        }

        self[index].as_mut().expect("always initialized above")
    }

    pub fn get_or_default(&mut self, index: BasinId) -> &mut T
    where
        T: Default,
    {
        self.get_or_insert_with(index, T::default)
    }
}

impl<T> Default for BasinMap<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Index<BasinId> for BasinMap<T> {
    type Output = Option<T>;

    fn index(&self, index: BasinId) -> &Self::Output {
        &self.basins[usize::from(index.index())]
    }
}

impl<T> IndexMut<BasinId> for BasinMap<T> {
    fn index_mut(&mut self, index: BasinId) -> &mut Self::Output {
        &mut self.basins[usize::from(index.index())]
    }
}

impl<'a, T> IntoIterator for &'a BasinMap<T> {
    type IntoIter = Iter<'a, T>;
    type Item = (BasinId, &'a T);

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            map: self,
            id: Some(BasinId::MIN),
        }
    }
}

#[derive(Debug)]
pub struct Iter<'a, T> {
    map: &'a BasinMap<T>,
    id: Option<BasinId>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (BasinId, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(id) = self.id {
            let next_id = id.next();
            let basin_contents = &self.map[id];
            self.id = next_id;
            // If the basin had something, return it. Otherwise, look at the
            // next position.
            if let Some(contents) = basin_contents {
                return Some((id, contents));
            }
        }

        None
    }
}
