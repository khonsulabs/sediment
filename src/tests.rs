use std::fs;
use std::io::Read;
use std::path::Path;

use crate::config::Config;
use crate::format::TransactionId;
use crate::Database;

#[test]
fn basic() {
    let path = Path::new("test");
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }

    let db = Database::recover(path).unwrap();
    assert!(db.embedded_header().unwrap().is_none());
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    println!("Wrote {grain:?}");
    tx.set_embedded_header(Some(grain)).unwrap();
    assert!(db.read(grain).unwrap().is_none());
    let tx_id = tx.commit().unwrap();
    assert_eq!(db.embedded_header().unwrap(), Some(grain));

    let verify = |db: &Database| {
        let mut reader = db.read(grain).unwrap().expect("grain not found");
        assert_eq!(reader.length(), 12);
        assert_eq!(reader.bytes_remaining(), reader.length());
        let mut read_contents = [0; 12];
        reader.read_exact(&mut read_contents[..6]).unwrap();
        assert_eq!(reader.bytes_remaining(), 6);
        reader.read_exact(&mut read_contents[6..]).unwrap();
        assert_eq!(reader.bytes_remaining(), 0);
        assert_eq!(&read_contents[..], b"hello, world");

        assert_eq!(reader.read(&mut read_contents).unwrap(), 0);

        let commit = db.commit_log_head().unwrap().expect("commit log missing");
        assert_eq!(commit.transaction_id, tx_id);
        assert_eq!(commit.new_grains.len(), 1);
        assert_eq!(commit.new_grains[0].id, grain);
        assert_eq!(commit.embedded_header_data, Some(grain));
        assert!(commit.freed_grains.is_empty());
        assert!(commit.archived_grains.is_empty());
        assert!(commit.next_entry(db).unwrap().is_none());
    };

    verify(&db);

    // Close the database and reopen it. Since this has a default WAL
    // configuration, this transaction will be recovered from the WAL, unlike a
    // lot of the other unit tests.
    db.shutdown().unwrap();
    let db = Database::recover(path).unwrap();

    verify(&db);
    db.shutdown().unwrap();

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_checkpoint() {
    let path = Path::new(".test-checkpoint");
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }

    // Configure the WAL to checkpoint after 10 bytes -- "hello, world" is 12.
    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    assert!(db.read(grain).unwrap().is_none());
    tx.commit().unwrap();
    db.shutdown().unwrap();

    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();
    let contents = db
        .read(grain)
        .unwrap()
        .expect("grain not found")
        .read_all_data()
        .unwrap();
    assert_eq!(contents, b"hello, world");

    db.shutdown().unwrap();

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn wal_checkpoint_loop() {
    let path = Path::new(".test-checkpoint-loop");
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }

    // Configure the WAL to checkpoint after 10 bytes -- "hello, world" is 12.
    let mut grains_written = Vec::new();
    for i in 0_usize..10 {
        println!("{i}");
        let db = Config::for_directory(path)
            .configure_wal(|wal| wal.checkpoint_after_bytes(10))
            .recover()
            .unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let grain = dbg!(tx.write(&i.to_be_bytes()).unwrap());
        assert!(db.read(grain).unwrap().is_none());
        grains_written.push(grain);
        tx.commit().unwrap();

        for (index, grain) in grains_written.iter().enumerate() {
            dbg!(grain);
            let contents = db
                .read(*grain)
                .unwrap()
                .expect("grain not found")
                .read_all_data()
                .unwrap();
            assert_eq!(contents, &index.to_be_bytes());
        }

        db.shutdown().unwrap();
    }

    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();
    for (index, grain) in grains_written.iter().enumerate() {
        let contents = db
            .read(*grain)
            .unwrap()
            .expect("grain not found")
            .read_all_data()
            .unwrap();
        assert_eq!(contents, &index.to_be_bytes());
    }

    // Verify the commit log is correct. The commit log head will contain the
    // addition of the most recent grain, and we should be able to iterate
    // backwards and find each grain in each entry.
    let mut grains_to_read = grains_written.iter().rev();
    let mut current_commit_log_entry = db.commit_log_head().unwrap();
    while let Some(commit_log_entry) = current_commit_log_entry {
        let expected_grain = grains_to_read.next().expect("too many commit log entries");
        assert_eq!(&commit_log_entry.new_grains[0].id, expected_grain);
        current_commit_log_entry = commit_log_entry.next_entry(&db).unwrap();
    }

    db.shutdown().unwrap();

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn sediment_checkpoint_loop() {
    let path = Path::new(".test-sediment-checkpoint-loop");
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }

    // Configure the WAL to checkpoint after 10 bytes -- "hello, world" is 12.
    let mut grains_written = Vec::new();
    let mut headers_written = Vec::new();
    let mut tx_id = TransactionId::default();
    for i in 0_usize..10 {
        let db = Config::for_directory(path)
            .configure_wal(|wal| wal.checkpoint_after_bytes(10))
            .recover()
            .unwrap();
        let mut tx = db.begin_transaction().unwrap();
        let new_grain = tx.write(&i.to_be_bytes()).unwrap();
        if let Some(last_grain) = grains_written.last() {
            tx.archive(*last_grain).unwrap();
        }
        grains_written.push(new_grain);
        // The old headers are automatically archived.
        let new_header = tx.write(&i.to_be_bytes()).unwrap();
        tx.set_embedded_header(Some(new_header)).unwrap();
        headers_written.push(new_header);

        tx.checkpoint_to(tx_id).unwrap();
        tx_id = tx.commit().unwrap();

        db.shutdown().unwrap();
    }

    let db = Config::for_directory(path)
        .configure_wal(|wal| wal.checkpoint_after_bytes(10))
        .recover()
        .unwrap();

    // Because we close and reopen the database so often, we may not actually
    // have finished the sediment checkpoint yet. This thread sleep gives it
    // time to complete if it was run upon recovery.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Because we archived all grains except the last one, we should only be able to read the last grain
    for (index, (grain, header)) in grains_written.iter().zip(&headers_written).enumerate() {
        let result = db.read(*grain).unwrap();
        let header_result = db.read(*header).unwrap();
        if index >= grains_written.len() - 2 {
            let contents = result.expect("grain not found").read_all_data().unwrap();
            assert_eq!(contents, &index.to_be_bytes());
            let contents = header_result
                .expect("grain not found")
                .read_all_data()
                .unwrap();
            assert_eq!(contents, &index.to_be_bytes());
        } else if let Some(grain) = result.or(header_result) {
            // Because grain IDs can be reused, we may have "lucked" out and
            // stumbled upon another written grain. If we get an error reading
            // the data or the contents aren't what we expect, this is a passed
            // check.
            if let Ok(contents) = grain.read_all_data() {
                assert_ne!(contents, &index.to_be_bytes());
            }
        } else {
            // None means the grain couldn't be read.
        }
    }

    db.shutdown().unwrap();

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn rollback() {
    let path = Path::new("rollback");
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }

    let db = Database::recover(path).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    let grain = tx.write(b"hello, world").unwrap();
    println!("Wrote {grain:?}");
    tx.set_embedded_header(Some(grain)).unwrap();
    assert!(db.read(grain).unwrap().is_none());
    drop(tx);

    // Ensure we still didn't get it published.
    assert!(db.read(grain).unwrap().is_none());

    // Trying again, we should get the same grain id back.
    let mut tx = db.begin_transaction().unwrap();
    assert_eq!(tx.write(b"hello, world").unwrap(), grain);
    drop(tx);

    db.shutdown().unwrap();

    fs::remove_dir_all(path).unwrap();
}
