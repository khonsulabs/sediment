use std::sync::Arc;

use lrumap::LruHashMap;
use parking_lot::{Mutex, RwLock};

use crate::{
    format::{GrainInfo, GrainMapPage, PAGE_SIZE},
    io,
};

#[derive(Debug)]
pub struct PageCache {
    cache: Mutex<LruHashMap<u64, CacheEntry>>,
}

impl PageCache {
    pub fn map<'a, File: io::File, F: FnOnce(&mut GrainMapPage) -> R, R>(
        &'a self,
        page_offset: u64,
        check_crc: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
        map: F,
    ) -> io::Result<R> {
        let mut cache = self.cache.lock();
        match cache.get_mut(&page_offset) {
            Some(CacheEntry::Loading(sync)) => {
                let sync = sync.clone();
                drop(cache);
                let _guard = sync.read();
                // Once we acquire the read, the page should now be available.
                self.map(page_offset, check_crc, file, scratch, map)
            }
            Some(CacheEntry::Loaded(page)) => Ok(map(page)),
            None => {
                let sync = Arc::new(RwLock::new(()));
                cache.push(page_offset, CacheEntry::Loading(sync.clone()));
                let page_guard = sync.write();
                drop(cache);

                let mut buffer = std::mem::take(scratch);
                buffer.resize(PAGE_SIZE, 0);
                let (result, buffer) = file.read_exact(buffer, page_offset);
                result?;

                let mut page = GrainMapPage::deserialize(&buffer, check_crc)?;
                *scratch = buffer;
                let result = map(&mut page);

                let mut cache = self.cache.lock();
                cache.push(page_offset, CacheEntry::Loaded(page));
                drop(cache);

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
        check_crc: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<GrainMapPage> {
        self.map(page_offset, check_crc, file, scratch, |page| page.clone())
    }

    pub fn fetch_grain_info<'a, File: io::File>(
        &'a self,
        page_offset: u64,
        local_grain_index: usize,
        check_crc: bool,
        file: &mut File,
        scratch: &mut Vec<u8>,
    ) -> io::Result<GrainInfo> {
        self.map(page_offset, check_crc, file, scratch, |page| {
            page.grains
                .get(local_grain_index)
                .copied()
                .unwrap_or_default()
        })
    }
    pub fn update_pages(&self, pages: impl Iterator<Item = (u64, GrainMapPage)>) {
        let mut cache = self.cache.lock();
        for (offset, page) in pages {
            cache.push(offset, CacheEntry::Loaded(page));
        }
    }
}

impl Default for PageCache {
    fn default() -> Self {
        Self {
            cache: Mutex::new(LruHashMap::new(4096)),
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum CacheEntry {
    Loading(Arc<RwLock<()>>),
    Loaded(GrainMapPage),
}
