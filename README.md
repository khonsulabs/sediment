# Sediment

**This repository isn't ready for public consumption. It just reached a stage
where I wanted to start sharing ideas with others as well as using the issues
tracker.**

An experimental storage format that supports highly concurrent, ACID-compliant
MVCC reads and writes in a single file. It is a low level crate and is not meant
for general purpose storage. It is a layer that higher level databases could be
built atop.

The basic interface is the ability to store immutable chunks of data and receive
a `GrainId` and `SequenceId` that can be used to retrieve the data in the
future. Only the `GrainId` is required to retrieve the value stored, and the
`SequenceId` can optionally be used to verify the value stored is the same one
originally written.

Modifications are performed in sessions. Many sessions can be executing
concurrently. In a session, chunks of data can be written to the database, and
existing `GrainId`s can be archived. When a session ends, all of the changes
performed are committed to the database. This design allows the database to
attempt batching commits automatically.

Data is stored in segments of files known as grains. This file is organized into
a 2-deep tree, with the leafs being sections of fixed-length grains. This design
enables Sediment to create a permanent `GrainId` for a written value that can be
relocated as needed. The location within the grain map must stay the same, but
the grain map can move and be reallocated without breaking the ability to locate
the data from a previously returned `GrainId`.

Data stored may span multiple grains in the underlying storage layer, in which
case the `GrainId` points to the first grain allocated.

## Goals of this format

* ACID-complaint, single-file storage
* Allows storing arbitrary "Grains" of data, returning an ID that can be used to
  look that data up again in the future.
* Supports removing data and reusing space
* Supports MVCC with custom checkpointing: Each change to a Grain is assigned a
  unique SequenceId. Reads can be verified to be from the same SequenceId. A
  Grain will not be reallocated until its "free SequenceId" has been
  checkpointed.
* Supports simultaneous writers: Using asynchronous IO, multiple threads should
  be able to write to a single database simultaneously. The storage layer should
  prevent operations to grains that are protected based on the guarantees
  outlined.
* Ability to roll back modifications instead of committing. Each writer's
  changes are not part of the on-disk tree structure until the session is
  committed. Rolling back a session is a memory-only operation that simply
  resets the allocation states.
* Embedding a header: The database's header will contain an optional `GrainId`
  that can be used to point to an embedded header.

  This is how Nebari will build a B+Tree atop this format. Nebari already uses
  arbitrary u64s as IDs within its B+Tree implementation. By serializing the
  Root into a Grain during the transactional commit, Nebari's active root is now
  able to be restored upon loading from disk, but it will move around each time
  the file is written, rather than always being appended to the end of the file.

## One-fsync ACID writes

This file's format stores several pieces of information in pairs. The root
header's `SequenceId`. Each batch written is assigned a new `SequenceId`. This
means the higher value is the most recently written value.

The file header is stored twice at the start of the file. When opening an
existing database, the header with the highest sequence is validated. If it
cannot be validated, the in-memory state is initialized with the older version.
When writing a new version of the header, it will overwrite the version that was
not used to load from disk.

The file header contains a list of "basins", which are also stored in pairs. The
file header will note which of the pair is active, allowing for basin headers to
only be rewritten when changed.

The file header contains a `GrainId` that points to a serialized structure
containing a `GrainId` pointing to the current Log. The log contains pointers to
each of the log entries. Each log entry contains a list of each grain changed
and the sequence of the commit.

To validate the saved transaction, each of these grains must be looked up,
verify that the `SequenceId` matches, and check the CRC of the written content.

This validation verifies information on every page written during the commit. If
any single page were partially written, it would be detected, and the previously
stable state would be used instead.

This design allows the commit operation to queue up all of the writes and
perform a single `fsync` operation to ensure complete data reliability.

## Theoretical Limits

* Up to 254 Basins per file
* Up to 254 Strata per Basin
* Up to 64,516 Strata per file
* Up to 4 billion Grains per Stratum
* Up to 277 trillion Grains per file
* Up to 4GB (2^32 bytes - 32 bytes) per Grain

Practically speaking, a file would need no fragmentation to reach all of the
limits.
