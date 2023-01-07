use std::{num::NonZeroUsize, ops::Range, path::Path, sync::Arc};

use rand::{prelude::StdRng, Rng, SeedableRng};
use sediment::{format::TransactionId, Database};
use timings::{Benchmark, BenchmarkImplementation, Label, LabeledTimings, Timings};

const ITERS: u128 = 100;
const INSERTS_PER_BATCH: usize = 20;

fn main() {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        if cfg!(feature = "sqlite") && !cfg!(feature = "fbarrier-fsync") {
            eprintln!("SQLite bundled in macOS uses F_BARRIERFSYNC instead of F_FULLFSYNC, which means it does not provide ACID guarantees. Enable feature `fbarrier-fsync` to configure Sediment to use the same synchronization primitive. See <https://bonsaidb.io/blog/acid-on-apple/> for more information.");
        }

        if cfg!(feature = "rocksdb") {
            if cfg!(feature = "fbarrier-fsync") {
                eprintln!("RocksDB prior to 7.3.1 only utilizes fdatasync. As of writing this, RocksDB does not support F_BARRIERFSYNC. The current version used by the rocksdb crate is 7.1.2.");
                eprintln!("rocksdb crate's built version: <https://github.com/rust-rocksdb/rust-rocksdb/blob/master/librocksdb-sys/build_version.cc#L11>");
                eprintln!("ACID on Apple: <https://bonsaidb.io/blog/acid-on-apple/>");
            } else {
                eprintln!("RocksDB does not use F_FULLFSYNC until version 7.3.1. The current version used by the rocksdb crate is 7.1.2.");
                eprintln!("rocksdb crate's built version: <https://github.com/rust-rocksdb/rust-rocksdb/blob/master/librocksdb-sys/build_version.cc#L11>");
            }
        }
    }

    let measurements = Timings::default();

    let source = vec![0; 4096];
    let mut ranges = Vec::new();
    let mut rng = StdRng::from_seed([0; 32]);
    for _ in 0..ITERS {
        let mut batch = Vec::with_capacity(rng.gen_range(1..INSERTS_PER_BATCH));
        for _ in 0..batch.capacity() {
            let start = rng.gen_range(0..source.len());
            let end = rng.gen_range(start..source.len());
            batch.push(start..end);
        }
        ranges.push(batch);
    }

    let threads = std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(4)
        .max(4);

    let mut benchmark = Benchmark::for_config(Arc::new(ThreadedInsertsData { source, ranges }))
        .with_each_number_of_threads([threads * 4, threads * 2, threads, 1]);

    #[cfg(feature = "sqlite")]
    {
        benchmark = benchmark.with::<SqliteThreadedInserts>();
    }
    #[cfg(feature = "rocksdb")]
    {
        benchmark = benchmark.with::<self::rocksdb::ThreadedInserts>();
    }

    benchmark = benchmark.with::<SedimentThreadedInserts>();

    benchmark.run(&measurements).unwrap();
    // return;

    measure_sediment(&measurements);
    #[cfg(feature = "marble")]
    marble::measure(&measurements);
    #[cfg(feature = "sqlite")]
    measure_sqlite(&measurements);
    #[cfg(feature = "rocksdb")]
    self::rocksdb::measure(&measurements);

    let stats = measurements.wait_for_stats();
    timings::print_table_summaries(&stats).unwrap();
}

fn measure_sediment(measurements: &Timings<String>) {
    let path = Path::new(".bench-suite.sediment");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }

    let sediment = Database::recover(path).unwrap();
    let mut checkpoint_to = TransactionId::default();
    for i in 0_u128..ITERS {
        let measurement = measurements.begin("sediment", String::from("insert 16b"));
        let mut session = sediment.begin_transaction().unwrap();
        session.write(&i.to_le_bytes()).unwrap();
        session.checkpoint_to(checkpoint_to).unwrap();
        checkpoint_to = session.commit().unwrap();
        measurement.finish();
    }

    sediment.shutdown().unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[cfg(feature = "sqlite")]
fn measure_sqlite(measurements: &Timings<String>) {
    let path = Path::new("./bench-suite.sqlite");
    if path.exists() {
        std::fs::remove_file(path).unwrap();
    }
    let mut sqlite = initialize_sqlite(path);

    for i in 0_u128..ITERS {
        let measurement = measurements.begin("sqlite", String::from("insert 16b"));
        let tx = sqlite.transaction().unwrap();
        tx.execute("insert into blobs (value) values ($1)", [&i.to_le_bytes()])
            .unwrap();
        tx.commit().unwrap();
        measurement.finish();
    }
    drop(sqlite);

    std::fs::remove_file(path).unwrap();
}

#[cfg(feature = "sqlite")]
fn initialize_sqlite(path: &Path) -> rusqlite::Connection {
    let sqlite = rusqlite::Connection::open(path).unwrap();
    sqlite
        .busy_timeout(std::time::Duration::from_secs(3600))
        .unwrap();

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
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
        sqlite.pragma_update(None, "fullfsync", "on").unwrap();
    }

    sqlite
        .execute("create table if not exists blobs (value BLOB)", [])
        .unwrap();
    sqlite
}

#[cfg(feature = "marble")]
mod marble {
    use super::*;

    pub fn measure(measurements: &Timings<String>) {
        let path = Path::new("./bench-suite.marble");
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        let marble = ::marble::open(path).unwrap();

        for i in 0_u128..ITERS {
            let measurement = measurements.begin("marble", String::from("insert 16b"));
            marble
                .write_batch([(i as u64 + 1, Some(i.to_le_bytes()))])
                .unwrap();
            marble.maintenance().unwrap();
            measurement.finish();
        }

        drop(marble);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[derive(Debug)]
pub struct ThreadedInsertsData {
    source: Vec<u8>,
    ranges: Vec<Vec<Range<usize>>>,
}

#[derive(Debug, Clone)]
pub struct SedimentThreadedInserts {
    db: Database,
    number_of_threads: usize,
    data: Arc<ThreadedInsertsData>,
}

impl BenchmarkImplementation<String, Arc<ThreadedInsertsData>, ()> for SedimentThreadedInserts {
    type SharedConfig = Self;

    fn initialize_shared_config(
        number_of_threads: usize,
        config: &Arc<ThreadedInsertsData>,
    ) -> Result<Self::SharedConfig, ()> {
        Ok(Self {
            db: Database::recover(".threaded-inserts.sediment").unwrap(),
            number_of_threads,
            data: config.clone(),
        })
    }

    fn reset(_shutting_down: bool) -> Result<(), ()> {
        let path = Path::new(".threaded-inserts.sediment");
        if path.exists() {
            println!("Cleaning up.");
            std::fs::remove_dir_all(path).unwrap();
        }
        Ok(())
    }

    fn initialize(_number_of_threads: usize, config: Self) -> Result<Self, ()> {
        Ok(config)
    }

    fn measure(&mut self, measurements: &LabeledTimings<String>) -> Result<(), ()> {
        let mut checkpoint_to = TransactionId::default();
        for batch in &self.data.ranges {
            let measurement =
                measurements.begin(format!("{}-threads-inserts", self.number_of_threads));
            let mut session = self.db.begin_transaction().unwrap();
            for range in batch {
                session.write(&self.data.source[range.clone()]).unwrap();
            }
            session.checkpoint_to(checkpoint_to).unwrap();
            checkpoint_to = session.commit().unwrap();
            //session.commit().unwrap();
            measurement.finish();
        }

        // dbg!(self.db.statistics());

        Ok(())
    }

    fn label(_number_of_threads: usize, _config: &Arc<ThreadedInsertsData>) -> Label {
        Label::from("sediment")
    }
}

#[cfg(feature = "sqlite")]
#[derive(Debug)]
pub struct SqliteThreadedInserts {
    number_of_threads: usize,
    data: Arc<ThreadedInsertsData>,
}

#[cfg(feature = "sqlite")]
impl BenchmarkImplementation<String, Arc<ThreadedInsertsData>, ()> for SqliteThreadedInserts {
    type SharedConfig = Arc<ThreadedInsertsData>;

    fn initialize_shared_config(
        _number_of_threads: usize,
        config: &Arc<ThreadedInsertsData>,
    ) -> Result<Self::SharedConfig, ()> {
        Ok(config.clone())
    }

    fn initialize(number_of_threads: usize, config: Arc<ThreadedInsertsData>) -> Result<Self, ()> {
        Ok(Self {
            number_of_threads,
            data: config,
        })
    }

    fn measure(&mut self, measurements: &LabeledTimings<String>) -> Result<(), ()> {
        let path = Path::new(".threaded-inserts.sqlite3");
        let mut db = initialize_sqlite(path);

        for batch in &self.data.ranges {
            let measurement =
                measurements.begin(format!("{}-threads-inserts", self.number_of_threads));
            let tx = db.transaction().unwrap();
            for range in batch {
                tx.execute(
                    "insert into blobs (value) values ($1)",
                    [&self.data.source[range.clone()]],
                )
                .unwrap();
            }
            tx.commit().unwrap();
            measurement.finish();
        }

        Ok(())
    }

    fn reset(_shutting_down: bool) -> Result<(), ()> {
        let path = Path::new(".threaded-inserts.sqlite3");
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
        Ok(())
    }

    fn label(_number_of_threads: usize, _config: &Arc<ThreadedInsertsData>) -> Label {
        Label::from("sqlite")
    }
}

#[cfg(feature = "rocksdb")]
mod rocksdb {
    use std::{
        path::Path,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
    };

    use rocksdb::{DBWithThreadMode, MultiThreaded, WriteBatch, WriteOptions, DB};
    use timings::{BenchmarkImplementation, LabeledTimings, Timings};

    use super::ITERS;
    use crate::ThreadedInsertsData;

    pub fn measure(measurements: &Timings<String>) {
        let path = Path::new("./bench-suite.rocksdb");
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        let db = DB::open_default(path).unwrap();
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);

        for i in 0_u128..ITERS {
            let measurement = measurements.begin("rocksdb", String::from("insert 16b"));

            db.put_opt(i.to_be_bytes(), i.to_le_bytes(), &write_opts)
                .unwrap();
            measurement.finish();
        }

        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }

    pub struct ThreadedInserts {
        number_of_threads: usize,
        config: ThreadedInsertsConfig,
    }

    #[derive(Clone)]
    pub struct ThreadedInsertsConfig {
        db: Arc<rocksdb::DBWithThreadMode<MultiThreaded>>,
        unique_id_counter: Arc<AtomicU64>,
        data: Arc<ThreadedInsertsData>,
    }

    impl BenchmarkImplementation<String, Arc<ThreadedInsertsData>, ()> for ThreadedInserts {
        type SharedConfig = ThreadedInsertsConfig;

        fn initialize_shared_config(
            _number_of_threads: usize,
            config: &Arc<ThreadedInsertsData>,
        ) -> Result<Self::SharedConfig, ()> {
            let path = Path::new("./.threaded-inserts.rocksdb");
            let db = DBWithThreadMode::<MultiThreaded>::open_default(path).unwrap();
            Ok(ThreadedInsertsConfig {
                db: Arc::new(db),
                unique_id_counter: Arc::default(),
                data: config.clone(),
            })
        }

        fn initialize(number_of_threads: usize, config: ThreadedInsertsConfig) -> Result<Self, ()> {
            Ok(Self {
                number_of_threads,
                config,
            })
        }

        #[allow(clippy::unnecessary_to_owned)] // TODO submit PR against rocksdb to allow ?Sized
        fn measure(&mut self, measurements: &LabeledTimings<String>) -> Result<(), ()> {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            for batch in &self.config.data.ranges {
                let measurement =
                    measurements.begin(format!("{}-threads-inserts", self.number_of_threads));
                let mut write_batch = WriteBatch::default();

                for range in batch {
                    let unique_id = self.config.unique_id_counter.fetch_add(1, Ordering::SeqCst);
                    write_batch.put(
                        unique_id.to_be_bytes(),
                        &self.config.data.source[range.clone()].to_vec(),
                    );
                }
                self.config.db.write_opt(write_batch, &write_opts).unwrap();
                measurement.finish();
            }

            Ok(())
        }

        fn reset(shutting_down: bool) -> Result<(), ()> {
            let path = Path::new("./.threaded-inserts.rocksdb");
            if path.exists() {
                std::fs::remove_dir_all(path).unwrap();
            }
            if !shutting_down {
                std::fs::create_dir(path).unwrap();
            }
            Ok(())
        }

        fn label(_number_of_threads: usize, _config: &Arc<ThreadedInsertsData>) -> timings::Label {
            "rocksdb".into()
        }
    }
}
