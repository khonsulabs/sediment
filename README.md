# Sediment

**This crate is not anywhere near being ready to be used in production projects.
Constructive feedback is welcome!**

![sediment forbids unsafe code](https://img.shields.io/badge/unsafe-forbid-success)
[![crate version](https://img.shields.io/crates/v/sediment.svg)](https://crates.io/crates/sediment)
[![Live Build Status](https://img.shields.io/github/actions/workflow/status/khonsulabs/sediment/rust.yml?branch=main)](https://github.com/khonsulabs/sediment/actions?query=workflow:Tests)
[![HTML Coverage Report for `main` branch](https://khonsulabs.github.io/sediment/coverage/badge.svg)](https://khonsulabs.github.io/sediment/coverage/)
[![Documentation](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/sediment/main/sediment)

This storage format is meant to provide a foundation for building ACID-compliant
databases.

- Uses a [write-ahead log][okaywal] for efficient, atomic, durable writes.
- Data chunks can be written and assigned a unique ID.
- Data chunks can be archived by their unique ID.
- The database can be checkpointed to free preivously archived data for reuse.
- The lifecycle of data allows building full database replication from the
  commit log contained within the database and ensuring data isn't overwritten
  before all clients are replicated.

This database uses a folder to store its files. Additional files can be stored
within the folder without affecting Sediment.

## The Grain Lifecycle

The storage format of Sediment is organized around storing chunks of data in
slots known as *grains*. When data is written to Sediment, it is given a
`GrainId`. The data associated with a `GrainId` is immutable.

A `GrainId` can be used to read previously stored data. Once a grain is no
longer needed, it can be archived. Archiving a grain marks it as being able to
be freed during a checkpointing operation.

When Sediment's database is checkpointed, all archived grains are freed. After
this operation, the previously allocated regions will be able to be reused.

This design allows Sediment to be viewed as an append-only data format despite
not being implemented as such. This design also enables building a replication
log using the built-in commit log.

## Sediment Architecture

Sediment's internal implementation is separated into several main types:

- `Atlas`: The Atlas keeps track of the in-memory representation of the state of
  the Sediment database. The Atlas knows whether a particular grain is in the
  write-ahead log or isn't even allocated at all.
- `Store`: The Store manages updating the on-disk representation of the Sediment
  database.
- `WalManager`: The WalManager persists changes from the write-ahead log to the
  Store, and updates the Atlas when data has moved from the write-ahead log to
  the primary data store.
- `Checkpointer`: The Checkpointer is responsible for freeing archived grains
  and removing old commit log entries. Without the Checkpointer, Sediment would
  be an append-only database that data could never be removed from.

### Disk Format

On-disk, Sediment uses an `index` file and a collection of "Strata" -- sticking
with the geology theme. Each Stratum is organized by its Basin, which determines
the individual grain size within the Stratum.

The `index` contains two copies of the `IndexHeader`. When updating the `index`,
the inactive version should be overwritten. This ensures that if a crash happens
while updating the header, the currently active version remains untouched.

Each Stratum contains two copies of a `StratumHeader`. Just like the `index`,
when updating a Stratum's header, the inactive version should be overwritten.
After the `StratumHeader`s, the remainder of the file is an array of grains.

A grain allocation can span multiple grains. Each grain begins with an 8-byte
value representing the `TransactionId` that the grain was allocated during.
Next, a 4-byte encoded representation of the allocation's exact byte length. The
grain's data comes after the length field. Finally a CRC32 is written directly
after the grain, which is a checksum of the data itself. This means that each
grain has 16 bytes of overhead.

### Opening a database (Recovery)

To open a database from an unknown state, the first step is opening the `index`.
The two `IndexHeaders`'s `transaction_id`s are compared. The most recent version
is attempted to be read from initially.

To verify that the `IndexHeader` is valid, we must inspect all data written. If
any checksums fail or inconsistencies are detected, the older `IndexHeader`
should be used instead.

To validate the last commit, the `commit_log_head` is loaded and each grain
operation must have the updates on-disk validated. To ensure a newly written
grain's data is correct, the grain header's `TransactionId` must match the
`IndexHeader` and the data's CRC must be validated. The expected CRC is not only
written on disk but is also contained in the `CommitLogEntry`.

Once the `Store` has restored its state from disk, the `Atlas` can be
initialized from the disk state. With the `Store` and the `Atlas` created, the
`WriteAheadLog` can be opened.

While recovering the `WriteAheadLog`, the `WalManager` will update the `Atlas`
with changes that were previously written to the WAL but haven't been
checkpointed into primary storage yet.

After the `WriteAheadLog` has finished recovery, the Sediment database is now
fully validated.

### Transaction Flow (Writing Data)

Sediment uses OkayWAL for its `WriteAheadLog` implementation. OkayWAL only
allows a single thread to write an Entry at any given time, but will
transparently batch multiple threads when their writes can be `fsync`ed
simultaneously. To leverage this, we need to ensure that changes written in
transactions are available to future transactions but are not published to
readers outside of a transaction until after the `WriteAheadLog` has confirmed
the commit.

This state is managed by the `TransactionLock`. It hands out a single
`TransactionGuard` at a time, which allows a thread to begin a new entry in the
`WriteAheadLog`. It also provides access to the transactional version of the
`IndexMetadata`.

Before committing a transaction's Entry to the `WriteAheadLog`,
`TransactionGuard::store` is called, which updates the transactional
`IndexMetadata` and stages the information about the current commit in a
location that can be published after the `WriteAheadLog` confirms the entry is
written. The thread is handed back a `TransactionFnalizer`. Another thread will
now be able to acquire a `TransactionGuard`.

The thread then commits the `WriteAheadLog` entry. Because another thread can
acquire a `TransactionGuard`, the `WriteAheadLog` is able to queue up and batch
multiple operations being made by different threads.

When the `WriteAheadLog` entry is committed, the `TransactionFinalizer` can be
finalized, which publishes all staged transactions up to and including the one
that was written by this thread.

Publishing the staged changes involves notifying the `Atlas` of the
`LogPosition`s that new `GrainId`s are available at, or marking `GrainId`s as
freed.

### `WriteAheadLog` Checkpointing

Periodically, the `WriteAheadLog` will invoke `WalManager::checkpoint_to()`,
part of the `LogManager` trait. During this stage, the WAL needs the
`WalManager` to move all data from the entries being checkpointed into the
primary data store.

The `LogManager` iterates over the entries being checkpointed. When a new grain
is written, the grain data at the appropriate location in the appropriate
Stratum is written including the grain's header and CRC. All grain changes are
accumulated during this scanning operation.

Once all entries have been processed, the `StratumHeader`s need to be updated to
reflect the new grain states. Once all updates to a given Stratum have been
processed, a new version of the `StratumHeader` is written to the file.

Once all Stratum have been updated, the `IndexHeader` is updated with the latest
checkpointed information. The new `IndexHeader` is then written to the `index`.

Finally, all changed Stratum, the `index`, and if there were new files, the
directory are `fsync`ed.

After all files are fully synchronized to disk, the `Atlas` is notified of the
grains that were checkpointed, allowing it to now serve read requests for grains
from primary storage instead of the WAL.

### Sediment Checkpointing (Removing Data)

Sediment stores a `CommitLogEntry` for each transaction, which describes the
changes performed to each grain. This `CommitLogEntry` contains enough
information to enable replicating a Sediment database. These `CommitLogEntry`s
are important to the recovery process, but they can take up a lot of disk space
if they aren't removed.

The process of cleaning up the Sediment database is called checkpointing, and is
driven by the `Checkpointer`. The `IndexMetadata` has two fields related to
checkpointing: `checkpointed_to` and `checkpoint_target`.

Sediment is built around atomic operations. Because data cannot be fully freed
until it has been checkpointed into primary storage, we first commit an update
to the `checkpoint_target` value. Once the update to `checkpoint_target` has
been persisted by the `WalManager` during a WAL Checkpoint, the `Checkpointer`
is notified of the updated target.

The `Checkpointer` scans the database for all `CommitLogEntry`s that have
`TransactionId`s between `checkpointed_to` and the updated `checkpoint_target`.
If any matching `CommitLogEntry` has any `archived_grains`, they are collected
into a list of grains to free. Each `CommitLogEntry`'s `GrainId` is collected to
archive.

Once all matching `CommitLogEntry`s have been found, a new transaction is begun.
The transaction writes the collected grain archive and free commands.
Additionally, the transaction updates the `checkpointed_to` value to the
`TransactionId` just checkpointed.

Once the transaction is committed, the Checkpointer waits for a new
`checkpoint_target`.

[okaywal]: https://github.com/khonsulabs/okaywal
