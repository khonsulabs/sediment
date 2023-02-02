use std::sync::{Arc, Weak};
use std::thread::JoinHandle;

use okaywal::WriteAheadLog;
use watchable::{Watchable, Watcher};

use crate::format::TransactionId;
use crate::{Data, Database, Error, Result};

#[derive(Debug)]
pub struct Checkpointer {
    watchable: Watchable<TransactionId>,
    handle_receiver: flume::Receiver<JoinHandle<Result<(), Error>>>,
}

impl Checkpointer {
    pub fn new(current_checkpointed_transaction: TransactionId) -> (Self, Spawner) {
        let watchable = Watchable::new(current_checkpointed_transaction);
        let watcher = watchable.watch();
        let (handle_sender, handle_receiver) = flume::bounded(1);

        (
            Self {
                watchable,
                handle_receiver,
            },
            Spawner {
                watcher,
                handle_sender,
            },
        )
    }

    pub fn checkpoint_to(&self, tx_id: TransactionId) {
        let _ = self.watchable.update(tx_id);
    }

    pub fn shutdown(&self) -> Result<()> {
        self.watchable.shutdown();
        let join_handle = self
            .handle_receiver
            .recv()
            .expect("handle should always be sent after spawning");
        join_handle.join().map_err(|_| Error::ThreadJoin)?
    }
}

#[derive(Debug)]
pub struct Spawner {
    watcher: Watcher<TransactionId>,
    handle_sender: flume::Sender<JoinHandle<Result<(), Error>>>,
}

impl Spawner {
    pub(super) fn spawn(
        self,
        current_checkpointed_tx: TransactionId,
        data: &Arc<Data>,
        wal: &WriteAheadLog,
    ) -> Result<()> {
        let data = Arc::downgrade(data);
        let wal = wal.clone();
        let thread_handle = std::thread::Builder::new()
            .name(String::from("sediment-cp"))
            .spawn(move || {
                sediment_checkpoint_thread(current_checkpointed_tx, self.watcher, data, wal)
            })
            .expect("failed to spawn thread");
        self.handle_sender
            .send(thread_handle)
            .expect("this send should never fail");
        Ok(())
    }
}

fn sediment_checkpoint_thread(
    baseline_transaction: TransactionId,
    mut tx_receiver: Watcher<TransactionId>,
    data: Weak<Data>,
    wal: WriteAheadLog,
) -> Result<()> {
    let mut current_tx_id = baseline_transaction;
    while let Ok(transaction_to_checkpoint) = tx_receiver.next_value() {
        if transaction_to_checkpoint <= current_tx_id {
            continue;
        }

        if let Some(data) = data.upgrade() {
            let db = Database {
                data,
                wal: wal.clone(),
            };

            // Find all commit log entries that are <=
            // transaction_to_checkpoint.
            let mut current_commit_log = db.commit_log_head()?;
            let mut archived_grains = Vec::new();
            let mut commit_logs_to_archive = Vec::new();
            while let Some(entry) = current_commit_log {
                if entry.transaction_id > current_tx_id
                    && entry.transaction_id <= transaction_to_checkpoint
                {
                    archived_grains.extend(entry.archived_grains.iter().copied());
                    commit_logs_to_archive.push(entry.grain_id);
                } else if entry.transaction_id <= current_tx_id {
                    // We can't go any further back.
                    break;
                }

                current_commit_log = entry.next_entry(&db)?;
            }

            let mut tx = db.begin_transaction()?;
            for commit_log_id in commit_logs_to_archive {
                tx.archive(commit_log_id)?;
            }
            tx.free_grains(&archived_grains)?;
            tx.checkpointed_to(transaction_to_checkpoint)?;
            tx.commit()?;

            current_tx_id = transaction_to_checkpoint;
        }
    }

    Ok(())
}
