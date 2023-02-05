use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::Path;

use crate::config::Config;
use crate::format::{
    BasinAndStratum, Duplicable, IndexHeader, StratumHeader, StratumId, TransactionId,
};
use crate::{Database, Error};

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
    tx.rollback().unwrap();

    db.shutdown().unwrap();

    fs::remove_dir_all(path).unwrap();
}

#[derive(Clone)]
enum WriteCommand<'a> {
    Write {
        target: Target,
        offset: u64,
        bytes: &'a [u8],
    },
    RemoveStratum(Option<StratumId>),
    RemoveIndex,
    DoNothing,
}

#[derive(Clone)]
enum Target {
    Grain,
    Stratum,
    Index,
}

#[test]
fn last_write_rollback() {
    #[track_caller]
    fn test_write_after(commands: &[WriteCommand], expect_error: bool) {
        let path = Path::new("last-write");
        if path.exists() {
            fs::remove_dir_all(path).unwrap();
        }

        let mut written_grains = Vec::new();
        let mut rolled_back_grains = Vec::new();
        let mut commands = commands.iter();
        let mut index = 0_usize;
        loop {
            let db = Config::for_directory(path)
                .configure_wal(|wal| wal.checkpoint_after_bytes(10))
                .recover();
            let db = match db {
                Ok(db) => db,
                Err(_) if commands.len() == 0 && expect_error => break,
                Err(err) => unreachable!("error when not expected: {err}"),
            };

            for (grain_id, expected_data) in &written_grains {
                assert_eq!(
                    &db.read(*grain_id)
                        .unwrap()
                        .expect("grain missing")
                        .read_all_data()
                        .unwrap(),
                    expected_data
                )
            }

            for (grain_id, expected_data) in &rolled_back_grains {
                if let Some(reader) = db.read(*grain_id).unwrap() {
                    // The grain id can be reused, but the contents shouldn't
                    // match. Note that this rollback required forcibly changing
                    // bits after the transaction was written. In a normal crash
                    // or power outage scenario, the grain id wouldn't have been
                    // returned until the data is fully synced to disk.
                    assert_ne!(&reader.read_all_data().unwrap(), expected_data);
                }
            }

            let mut tx = db.begin_transaction().unwrap();
            let data = index
                .to_be_bytes()
                .into_iter()
                .cycle()
                .take(2000)
                .collect::<Vec<_>>();
            index += 1;
            let grain_id = dbg!(tx.write(&data).unwrap());
            assert_eq!(grain_id.grain_count(), 63);
            tx.commit().unwrap();

            db.shutdown().unwrap();

            match commands.next() {
                Some(WriteCommand::Write {
                    target,
                    offset,
                    bytes,
                }) => {
                    let mut file = match target {
                        Target::Grain | Target::Stratum => OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(path.join(grain_id.basin_and_stratum().to_string()))
                            .unwrap(),
                        Target::Index => OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(path.join("index"))
                            .unwrap(),
                    };
                    let position = match target {
                        Target::Grain => grain_id.file_position() + *offset,
                        Target::Stratum | Target::Index => *offset,
                    };
                    file.seek(std::io::SeekFrom::Start(position)).unwrap();
                    file.write_all(bytes).unwrap();
                    rolled_back_grains.push((grain_id, data));
                }
                Some(WriteCommand::RemoveStratum(Some(stratum))) => {
                    let id = BasinAndStratum::from_parts(grain_id.basin_id(), *stratum);
                    std::fs::remove_file(path.join(id.to_string())).unwrap();
                }
                Some(WriteCommand::RemoveStratum(None)) => {
                    std::fs::remove_file(path.join(grain_id.basin_and_stratum().to_string()))
                        .unwrap();
                }
                Some(WriteCommand::RemoveIndex) => {
                    std::fs::remove_file(path.join("index")).unwrap();
                }
                Some(WriteCommand::DoNothing) => written_grains.push((grain_id, data)),
                None if expect_error => unreachable!("expected error but no error was encountered"),
                None => break,
            }
        }

        fs::remove_dir_all(path).unwrap();
    }

    // Test removing the stratum after it's been created. This simulates a file
    // being written but the directory metadata not being synchronized, causing
    // the file's record to be entirely lost.
    test_write_after(&[WriteCommand::RemoveStratum(None)], false);
    // Test overwriting the headers with 0 -- an edge case where the file record
    // was synced but the headers weren't.
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Stratum,
            offset: 0,
            bytes: &[1; 16_384],
        }],
        false,
    );
    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Stratum,
                offset: StratumHeader::BYTES,
                bytes: &[1; 16_384],
            },
        ],
        false,
    );
    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Stratum,
                offset: 0,
                bytes: &[1; 16_384 * 2],
            },
        ],
        true,
    );
    // Test overwriting the header with a valid but incorrect header. This
    // shouldn't ever happen in practice, because recovery is supposed to
    // overwrite the bad headers.
    let mut valid_header = StratumHeader::default();
    let mut valid_header_bytes = Vec::new();
    valid_header.transaction_id = TransactionId::from(1);
    valid_header.write_to(&mut valid_header_bytes).unwrap();
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Stratum,
            offset: 0,
            bytes: &valid_header_bytes,
        }],
        false,
    );
    valid_header.transaction_id = TransactionId::from(2);
    valid_header_bytes.clear();
    valid_header.write_to(&mut valid_header_bytes).unwrap();
    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Stratum,
                offset: StratumHeader::BYTES,
                bytes: &valid_header_bytes,
            },
        ],
        false,
    );
    // Test overwriting both headers with crc-valid, but not actually valid,
    // headers.
    valid_header.write_to(&mut valid_header_bytes).unwrap();
    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Stratum,
                offset: 0,
                bytes: &valid_header_bytes,
            },
        ],
        true,
    );

    // Test overwriting a grain's transaction ID in both the first and second
    // headers.
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Grain,
            offset: 0,
            bytes: &[0xFF],
        }],
        false,
    );

    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Grain,
                offset: 0,
                bytes: &[0xFF],
            },
        ],
        false,
    );

    // Test mutating the grain data, causing its CRC to fail to validate.
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Grain,
            offset: 13,
            bytes: &[0xFF],
        }],
        false,
    );

    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Grain,
                offset: 13,
                bytes: &[0xFF],
            },
        ],
        false,
    );

    // Test overwriting the stratum header.
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Stratum,
            offset: 0,
            bytes: &[0xFF],
        }],
        false,
    );

    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Stratum,
                offset: StratumHeader::BYTES,
                bytes: &[0xFF],
            },
        ],
        false,
    );

    // Test mucking with the index file
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Index,
            offset: 0,
            bytes: &[0xFF],
        }],
        false,
    );

    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Index,
                offset: IndexHeader::BYTES,
                bytes: &[0xFF],
            },
        ],
        false,
    );

    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Index,
                offset: IndexHeader::BYTES,
                bytes: &[0xFF],
            },
        ],
        false,
    );

    // Test writing a "valid" index header, but with broken data.
    let mut index_header = IndexHeader {
        // Point the commit log to an invalid id.
        transaction_id: TransactionId::from(1),
        commit_log_head: "71fffffffffe-fffff".parse().ok(),
        ..IndexHeader::default()
    };
    let mut index_header_bytes = Vec::new();
    index_header.write_to(&mut index_header_bytes).unwrap();
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Index,
            offset: 0,
            bytes: &index_header_bytes,
        }],
        false,
    );
    index_header.transaction_id = TransactionId::from(2);
    index_header_bytes.clear();
    index_header.write_to(&mut index_header_bytes).unwrap();
    test_write_after(
        &[
            WriteCommand::DoNothing,
            WriteCommand::Write {
                target: Target::Index,
                offset: IndexHeader::BYTES,
                bytes: &index_header_bytes,
            },
        ],
        false,
    );

    // Test removing the index file. This should generate an error because the
    // existing strata can be found.
    test_write_after(&[WriteCommand::RemoveIndex], true);

    // Test writing a valid index header, then overwriting part of the second header,
    // causing one header to fail to validate while the other can't parse due to
    // a crc error. This should never happen in real life.
    index_header.transaction_id = TransactionId::from(1);
    index_header_bytes.clear();
    index_header.write_to(&mut index_header_bytes).unwrap();
    // Overwrite part of the transaction id of the second header, causing its crc to fail.
    index_header_bytes.push(1);
    test_write_after(
        &[WriteCommand::Write {
            target: Target::Index,
            offset: 0,
            bytes: &index_header_bytes,
        }],
        true,
    );

    // Write enough data to need two stratum, then remove the first to receiven
    // error. There are 3 grains required at the current allocation strategy for
    // a commit log entry that describes 1 new grain. The test function writes
    // each grain such that it takes up 63 consecutive grains.
    let mut commands = vec![WriteCommand::DoNothing; 16_372 / (3 + 63) + 1];
    *commands.last_mut().unwrap() = WriteCommand::RemoveStratum(Some(StratumId::new(0).unwrap()));
    test_write_after(&commands, true);
}

#[test]
fn invalid_checkpointing() {
    let path = Path::new("invalid-checkpointing");
    if path.exists() {
        fs::remove_dir_all(path).unwrap();
    }

    let db = Database::recover(path).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    assert!(matches!(
        tx.checkpoint_to(TransactionId::from(1)).unwrap_err(),
        Error::InvalidTransactionId
    ));
    assert!(matches!(
        tx.checkpointed_to(TransactionId::from(1)).unwrap_err(),
        Error::InvalidTransactionId
    ));
    drop(tx);

    db.shutdown().unwrap();
    fs::remove_dir_all(path).unwrap();
}
