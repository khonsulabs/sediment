use flume::{Receiver, Sender};

use crate::{Error, Result};

use std::{
    fs::File,
    sync::{Arc, Condvar, Mutex},
    thread::{self, available_parallelism, JoinHandle},
};

#[derive(Debug)]
pub struct FSyncManager {
    data: Mutex<ThreadState>,
}

impl FSyncManager {
    pub fn new(maximum_threads: usize) -> Self {
        Self {
            data: Mutex::new(ThreadState::Uninitialized { maximum_threads }),
        }
    }
    pub fn shutdown(&self) -> Result<()> {
        let mut data = self.data.lock()?;
        if let ThreadState::Running(ManagerThread {
            handle,
            command_sender,
        }) = std::mem::replace(&mut *data, ThreadState::Shutdown)
        {
            drop(command_sender);
            handle.join().map_err(|_| Error::ThreadJoin)??;
        }
        Ok(())
    }

    fn with_running_thread<R>(&self, cb: impl FnOnce(&mut ManagerThread) -> R) -> Result<R> {
        let mut data = self.data.lock()?;
        match &*data {
            ThreadState::Uninitialized { maximum_threads } => {
                let (command_sender, command_receiver) = flume::unbounded();
                let threads_to_spawn = maximum_threads.checked_sub(1).unwrap_or_default();
                let handle = thread::Builder::new()
                    .name(String::from("sediment-sync"))
                    .spawn(move || fsync_thread(command_receiver, threads_to_spawn))?;
                *data = ThreadState::Running(ManagerThread {
                    handle,
                    command_sender,
                });
            }
            ThreadState::Shutdown => return Err(Error::Shutdown),
            ThreadState::Running(_) => {}
        }

        let ThreadState::Running(thread) = &mut *data else { unreachable!("initialized above")};
        Ok(cb(thread))
    }

    pub fn new_batch(&self) -> Result<FSyncBatch> {
        let notify = Arc::new(FSyncNotify {
            remaining: Mutex::new(0),
            sync: Condvar::new(),
        });

        Ok(FSyncBatch {
            command_sender: self.with_running_thread(|t| t.command_sender.clone())?,
            notify,
        })
    }
}

impl Default for FSyncManager {
    fn default() -> Self {
        Self::new(available_parallelism().map_or(4, |nonzero| nonzero.get()))
    }
}

enum SpawnStatus {
    CanSpawn,
    Spawned(JoinHandle<Result<(), Error>>),
    AtLimit,
}

fn fsync_thread(command_receiver: Receiver<FSync>, threads_to_spawn: usize) -> Result<(), Error> {
    let mut spawn_status = if threads_to_spawn > 0 {
        SpawnStatus::CanSpawn
    } else {
        SpawnStatus::AtLimit
    };

    while let Ok(fsync) = command_receiver.recv() {
        // Check if we should spawn a thread.
        if matches!(spawn_status, SpawnStatus::CanSpawn) && !command_receiver.is_empty() {
            let command_receiver = command_receiver.clone();
            let handle = thread::Builder::new()
                .name(String::from("sediment-sync"))
                .spawn(move || fsync_thread(command_receiver, threads_to_spawn))?;
            spawn_status = SpawnStatus::Spawned(handle);
        }

        if fsync.all {
            fsync.file.sync_all()?;
        } else {
            fsync.file.sync_data()?;
        }

        let mut remaining_syncs = fsync.notify.remaining.lock()?;
        *remaining_syncs -= 1;
        drop(remaining_syncs);
        fsync.notify.sync.notify_one();
    }

    if let SpawnStatus::Spawned(handle) = spawn_status {
        handle.join().map_err(|_| Error::ThreadJoin)??;
    }

    Ok(())
}

#[derive(Debug)]
enum ThreadState {
    Uninitialized { maximum_threads: usize },
    Running(ManagerThread),
    Shutdown,
}

#[derive(Debug)]
struct ManagerThread {
    handle: JoinHandle<Result<()>>,
    command_sender: Sender<FSync>,
}

struct FSync {
    all: bool,
    file: File,
    notify: Arc<FSyncNotify>,
}

#[derive(Debug)]
pub struct FSyncBatch {
    command_sender: Sender<FSync>,
    notify: Arc<FSyncNotify>,
}

impl FSyncBatch {
    pub fn queue_fsync_all(&self, file: File) -> Result<()> {
        self.command_sender
            .send(FSync {
                all: true,
                file,
                notify: self.notify.clone(),
            })
            .map_err(|_| Error::Shutdown)?;

        let mut remaining_syncs = self.notify.remaining.lock()?;
        *remaining_syncs += 1;

        Ok(())
    }

    pub fn queue_fsync_data(&self, file: File) -> Result<()> {
        self.command_sender
            .send(FSync {
                all: false,
                file,
                notify: self.notify.clone(),
            })
            .map_err(|_| Error::Shutdown)?;

        let mut remaining_syncs = self.notify.remaining.lock()?;
        *remaining_syncs += 1;

        Ok(())
    }

    pub fn wait_all(self) -> Result<()> {
        let mut remaining_syncs = self.notify.remaining.lock()?;

        while *remaining_syncs > 0 {
            remaining_syncs = self.notify.sync.wait(remaining_syncs)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct FSyncNotify {
    remaining: Mutex<usize>,
    sync: Condvar,
}
