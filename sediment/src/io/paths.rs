use std::{
    borrow::Borrow,
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

#[derive(Clone, Debug)]
pub struct PathId {
    pub id: u64,
    pub path: Arc<PathBuf>,
}

impl std::hash::Hash for PathId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl Eq for PathId {}

impl PartialEq for PathId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id || self.path == other.path
    }
}

impl PartialEq<Path> for PathId {
    fn eq(&self, other: &Path) -> bool {
        &**self.path == other
    }
}

impl Borrow<Path> for PathId {
    fn borrow(&self) -> &Path {
        &self.path
    }
}

impl AsRef<Path> for PathId {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

/// Converts between paths and unique IDs.
#[derive(Default, Clone, Debug)]
pub struct PathIds {
    file_id_counter: Arc<AtomicU64>,
    file_ids: Arc<RwLock<HashSet<PathId>>>,
}

impl PathIds {
    fn file_id_for_path(&self, path: &Path, insert_if_not_found: bool) -> Option<PathId> {
        let file_ids = self.file_ids.upgradable_read();
        if let Some(id) = file_ids.get(path) {
            Some(id.clone())
        } else if insert_if_not_found {
            let mut file_ids = RwLockUpgradableReadGuard::upgrade(file_ids);
            // Assume that in the optimal flow, multiple threads aren't asking
            // to open the same path for the first time.
            let new_id = PathId {
                path: Arc::new(path.to_path_buf()),
                id: self.file_id_counter.fetch_add(1, Ordering::SeqCst),
            };
            if file_ids.insert(new_id.clone()) {
                Some(new_id)
            } else {
                file_ids.get(path).cloned()
            }
        } else {
            None
        }
    }

    #[must_use]
    pub fn get(&self, path: &Path) -> Option<PathId> {
        self.file_id_for_path(path, false)
    }

    #[must_use]
    pub fn get_or_insert(&self, path: &Path) -> PathId {
        self.file_id_for_path(path, true)
            .expect("should always return a path")
    }

    #[must_use]
    pub fn paths_with_prefix(&self, path: &Path) -> Vec<PathId> {
        let files = self.file_ids.read();
        files
            .iter()
            .filter(|p| p.path.starts_with(path))
            .cloned()
            .collect()
    }

    // fn remove_file_id_for_path(&self, path: impl IntoPathId) -> Option<PathId> {
    //     let mut file_ids = self.file_ids.write();
    //     if path.id().is_some() {
    //         file_ids.take(&path.into_path_id())
    //     } else {
    //         file_ids.take(path.path())
    //     }
    // }

    // fn recreate_file_id_for_path(&self, path: impl IntoPathId) -> Option<RecreatedFile<'_>> {
    //     let existing = path.into_path_id();
    //     let mut file_ids = self.file_ids.write();
    //     let new_id = self.file_id_counter.fetch_add(1, Ordering::SeqCst);
    //     let new_id = PathId {
    //         path: existing.path,
    //         id: Some(new_id),
    //     };
    //     file_ids
    //         .replace(new_id.clone())
    //         .map(|old_id| RecreatedFile {
    //             previous_id: old_id,
    //             new_id,
    //             _guard: file_ids,
    //         })
    // }

    // fn remove_file_ids_for_path_prefix(&self, path: &Path) -> Vec<PathId> {
    //     let mut file_ids = self.file_ids.write();
    //     let mut ids_to_remove = Vec::new();
    //     let mut paths_to_remove = Vec::new();
    //     for id in file_ids.iter() {
    //         if id.path().starts_with(path) {
    //             paths_to_remove.push(id.clone());
    //             ids_to_remove.push(id.clone());
    //         }
    //     }

    //     for path in paths_to_remove {
    //         file_ids.remove(&path);
    //     }

    //     ids_to_remove
    // }
}

/// A file that has had its contents replaced. While this value exists, all
/// other threads will be blocked from interacting with the [`PathIds`]
/// structure. Only hold onto this value for short periods of time.
#[derive(Debug)]
pub struct RecreatedFile<'a> {
    /// The file's previous id.
    pub previous_id: PathId,
    /// The file's new id.
    pub new_id: PathId,
    _guard: RwLockWriteGuard<'a, HashSet<PathId>>,
}
