use std::path::Path;

use sediment::{database::Database, io::fs::StdFile};
use timings::Timings;

fn main() {
    let (measurements, stats) = Timings::new();

    measure_sediment(&measurements);
    #[cfg(feature = "marble")]
    marble::measure(&measurements);
    measure_sqlite(&measurements);

    drop(measurements);

    let stats = stats.join().unwrap();
    timings::print_table_summaries(&stats).unwrap();
}

fn measure_sediment(measurements: &Timings<&'static str>) {
    let path = Path::new(".bench-suite.sediment");
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }

    let mut sediment = Database::<StdFile>::open(path).unwrap();

    let data = vec![0; 4096];
    for _ in 0_u128..5 {
        let measurement = measurements.begin("sediment", "insert 16b");
        sediment.write(&data).unwrap();
        measurement.finish();
    }

    drop(sediment);
    std::fs::remove_file(path).unwrap();
}

fn measure_sqlite(measurements: &Timings<&'static str>) {
    let path = Path::new("./bench-suite.sqlite");
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let mut sqlite = rusqlite::Connection::open(path).unwrap();
    // On macOS with built-in SQLite versions, despite the name and the SQLite
    // documentation, this pragma makes SQLite use `fcntl(_, F_BARRIER_FSYNC,
    // _)`. There's not a good practical way to make rusqlite's access of SQLite
    // on macOS to use `F_FULLFSYNC`, which skews benchmarks heavily in favor of
    // SQLite when not enabling this feature.
    //
    // Enabling this feature reduces the durability guarantees, which breaks
    // ACID compliance. Unless performance is critical on macOS or you know that
    // ACID compliance is not important for your application, this feature
    // should be left disabled.
    //
    // <https://bonsaidb.io/blog/acid-on-apple/>
    // <https://www.sqlite.org/pragma.html#pragma_fullfsync>
    #[cfg(feature = "fbarrier-fsync")]
    sqlite.pragma_update(None, "fullfsync", "on").unwrap();

    sqlite
        .execute("create table blobs (value BLOB)", [])
        .unwrap();

    for i in 0_u128..5 {
        let measurement = measurements.begin("sqlite", "insert 16b");
        let tx = sqlite.transaction().unwrap();
        tx.execute("insert into blobs (value) values ($1)", [&i.to_le_bytes()])
            .unwrap();
        tx.commit().unwrap();
        measurement.finish();
    }
    drop(sqlite);

    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "marble")]
mod marble {
    use super::*;
    use ::marble::{Marble, ObjectId};

    pub fn measure(measurements: &Timings<&'static str>) {
        let path = Path::new("./bench-suite.marble");
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        let marble = Marble::open(path).unwrap();

        for i in 0_u128..1000 {
            let measurement = measurements.begin("marble", "insert 16b");
            marble
                .write_batch([(ObjectId::new(i as u64 + 1).unwrap(), Some(i.to_le_bytes()))])
                .unwrap();
            marble.maintenance().unwrap();
            measurement.finish();
        }

        drop(marble);
        std::fs::remove_dir_all(path).unwrap();
    }
}