use std::{collections::HashMap, sync::Arc};

use lrumap::LruHashMap;
use parking_lot::{Mutex, RwLock};
use rebytes::{Allocator, Buffer};

use crate::{
    format::{BatchId, GrainMapPage, PAGE_SIZE, PAGE_SIZE_U64},
    io,
};

#[derive(Debug)]
pub struct PageCache {
    data: Mutex<Data>,
}

#[derive(Debug)]
struct Data {
    cache: LruHashMap<u64, CacheEntry>,
    new_pages: HashMap<u64, LoadedGrainMapPage>,
    first_is_active: HashMap<u64, bool>,
}

impl PageCache {
    pub fn map<'a, File: io::File, F: FnOnce(&mut LoadedGrainMapPage) -> R, R>(
        &'a self,
        page_offset: u64,
        current_batch: BatchId,
        file: &mut File,
        allocator: &Allocator,
        map: F,
    ) -> io::Result<R> {
        let mut data = self.data.lock();
        match data.cache.get_mut(&page_offset) {
            Some(CacheEntry::Loading(sync)) => {
                let sync = sync.clone();
                drop(data);
                let _guard = sync.read();
                // Once we acquire the read, the page should now be available.
                self.map(page_offset, current_batch, file, allocator, map)
            }
            Some(CacheEntry::Loaded(page)) => Ok(map(page)),
            None => {
                if let Some(page) = data.new_pages.get_mut(&page_offset) {
                    return Ok(map(page));
                }

                let sync = Arc::new(RwLock::new(()));
                data.cache
                    .push(page_offset, CacheEntry::Loading(sync.clone()));
                let page_guard = sync.write();
                let first_is_active = data.first_is_active.get(&page_offset).copied();
                drop(data);

                let mut loaded_page = if let Some(first_is_active) = first_is_active {
                    LoadedGrainMapPage::read_specific_page_from(
                        page_offset,
                        first_is_active,
                        file,
                        allocator,
                    )?
                } else {
                    LoadedGrainMapPage::read_from(page_offset, current_batch, file, allocator)?
                };
                if loaded_page.page.written_at > current_batch {
                    return Err(io::invalid_data_error("both grain map pages are invalid"));
                }
                let result = map(&mut loaded_page);

                let mut data = self.data.lock();
                let is_first = loaded_page.is_first;
                data.cache
                    .push(page_offset, CacheEntry::Loaded(loaded_page));

                // If we didn't have state stored for this page about which
                // header is active, insert it now.
                if first_is_active.is_none() {
                    data.first_is_active.insert(page_offset, is_first);
                }

                drop(data);

                // Now that the cache is updated, unblock any threads waiting on
                // this page.
                drop(page_guard);

                Ok(result)
            }
        }
    }

    pub fn fetch<'a, File: io::File>(
        &'a self,
        page_offset: u64,
        current_batch: BatchId,
        file: &mut File,
        allocator: &Allocator,
    ) -> io::Result<LoadedGrainMapPage> {
        self.map(page_offset, current_batch, file, allocator, |page| {
            page.clone()
        })
    }

    pub fn fetch_grain_length<'a, File: io::File>(
        &'a self,
        page_offset: u64,
        local_grain_index: usize,
        current_batch: BatchId,
        file: &mut File,
        allocator: &Allocator,
    ) -> io::Result<u8> {
        assert!(local_grain_index < GrainMapPage::GRAINS);

        self.map(page_offset, current_batch, file, allocator, |loaded_page| {
            let first_byte = local_grain_index
                .checked_sub(1)
                .map(|previous_index| loaded_page.page.consecutive_allocations[previous_index]);
            let grains_allocated = loaded_page.page.consecutive_allocations[local_grain_index];
            if first_byte.map_or(false, |first_byte| first_byte > grains_allocated) {
                // The previous byte contained a larger number, which means this
                // `GrainId` points at the middle of an allocation.
                return Err(io::invalid_data_error(format!(
                    "invalid GrainId - points inside of another allocation {} - {grains_allocated}",
                    first_byte.unwrap()
                )));
            }

            Ok(grains_allocated)
        })?
    }

    pub fn update_pages(&self, pages: impl Iterator<Item = (u64, LoadedGrainMapPage)>) {
        let mut data = self.data.lock();
        for (offset, page) in pages {
            *data.first_is_active.entry(offset).or_default() = page.is_first;
            data.cache.push(offset, CacheEntry::Loaded(page));
            data.new_pages.remove(&offset);
        }
    }

    pub fn register_new_page(&self, offset: u64) {
        let mut data = self.data.lock();
        data.new_pages.insert(offset, LoadedGrainMapPage::default());
    }
}

impl Default for PageCache {
    fn default() -> Self {
        Self {
            data: Mutex::new(Data {
                cache: LruHashMap::new(4096),
                new_pages: HashMap::new(),
                first_is_active: HashMap::new(),
            }),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum CacheEntry {
    Loading(Arc<RwLock<()>>),
    Loaded(LoadedGrainMapPage),
}

#[derive(Debug, Clone, Default)]
pub struct LoadedGrainMapPage {
    pub is_first: bool,
    pub page: GrainMapPage,
}

impl LoadedGrainMapPage {
    pub fn write_to<File: io::WriteIoBuffer>(
        &mut self,
        offset: u64,
        new_batch: BatchId,
        file: &mut File,
        allocator: &Allocator,
    ) -> io::Result<()> {
        let page_offset = if self.is_first {
            offset + PAGE_SIZE_U64
        } else {
            offset
        };

        self.page.written_at = new_batch;
        self.page.write_to(page_offset, file, allocator)?;
        self.is_first = !self.is_first;

        Ok(())
    }

    pub fn read_from<File: io::File>(
        page_offset: u64,
        current_batch: BatchId,
        file: &mut File,
        allocator: &Allocator,
    ) -> io::Result<Self> {
        let buffer = Buffer::with_len(PAGE_SIZE * 2, allocator.clone());

        let (result, buffer) = file.read_exact(buffer, page_offset);
        result?;

        let first_page = GrainMapPage::deserialize(&buffer[..PAGE_SIZE], true);
        let second_page = GrainMapPage::deserialize(&buffer[PAGE_SIZE..], true);

        match (first_page, second_page) {
            (Ok(first_page), Ok(second_page)) => {
                if first_page.written_at > second_page.written_at
                    && first_page.written_at <= current_batch
                {
                    Ok(LoadedGrainMapPage {
                        page: first_page,
                        is_first: true,
                    })
                } else if second_page.written_at > first_page.written_at
                    && second_page.written_at <= current_batch
                {
                    Ok(LoadedGrainMapPage {
                        page: second_page,
                        is_first: false,
                    })
                } else {
                    Err(io::invalid_data_error(
                        "neither grain map has a valid batch",
                    ))
                }
            }
            (_, Ok(second_page)) => Ok(LoadedGrainMapPage {
                page: second_page,
                is_first: false,
            }),
            (Ok(first_page), _) => Ok(LoadedGrainMapPage {
                page: first_page,
                is_first: true,
            }),
            (Err(err), _) => Err(err),
        }
    }

    pub fn read_specific_page_from<File: io::File>(
        page_offset: u64,
        first_page: bool,
        file: &mut File,
        allocator: &Allocator,
    ) -> io::Result<Self> {
        let buffer = Buffer::with_len(PAGE_SIZE, allocator.clone());
        let (result, buffer) = file.read_exact(
            buffer,
            page_offset + (!first_page).then(|| 4096).unwrap_or_default(),
        );
        result?;

        GrainMapPage::deserialize(&buffer, true).map(|page| LoadedGrainMapPage {
            page,
            is_first: first_page,
        })
    }
}
