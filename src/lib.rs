use std::{
    io::{self, Read},
    path::Path,
    sync::Arc,
};

use okaywal::WriteAheadLog;

use crate::{
    atlas::{Atlas, GrainReader},
    format::GrainId,
    store::Store,
};

pub use transaction::Transaction;

mod allocations;
mod atlas;
mod basinmap;
pub mod format;
mod store;
mod transaction;
mod util;
mod wal;

pub type Result<T, E = io::Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Database {
    data: Arc<Data>,
    wal: WriteAheadLog,
}

impl Database {
    pub fn recover<AsRefPath: AsRef<Path>>(directory: AsRefPath) -> Result<Self> {
        // Opening the store restores the database to the last fully committed
        // state. Each commit happens when the write ahead log is checkpointed.
        let store = Store::recover(directory.as_ref())?;
        let atlas = Atlas::new(&store);
        let data = Arc::new(Data { store, atlas });
        // Recover any transactions from the write ahead log that haven't been
        // checkpointed to the store already.
        let wal = WriteAheadLog::recover(directory, wal::Manager::new(&data))?;

        Ok(Self { data, wal })
    }

    pub fn begin_transaction(&self) -> Result<Transaction<'_>> {
        let wal_entry = self.wal.begin_entry()?;

        Ok(Transaction::new(self, wal_entry))
    }

    pub fn get(&self, grain: GrainId) -> Result<Option<GrainReader>> {
        self.data.atlas.find(grain, &self.wal)
    }
}

impl Eq for Database {}

impl PartialEq for Database {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
}

#[derive(Debug)]
struct Data {
    store: Store,
    atlas: Atlas,
}

#[test]
fn basic() {
    let path = Path::new("test");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }

    let db = Database::recover(path).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    tx.commit().unwrap();

    let mut reader = db.get(grain).unwrap().expect("grain not found");
    let mut contents = Vec::new();
    reader.read_to_end(&mut contents).unwrap();
    assert_eq!(contents, b"hello, world");

    std::fs::remove_dir_all(path).unwrap();
}
