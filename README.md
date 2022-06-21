# Sediment

**This repository isn't ready for public consumption. It just reached a stage
where I wanted to start sharing ideas with others as well as using the issues
tracker.**

An experimental storage format designed for ACID-compliant, bulk blob storage.
This crate is designed as a replacement to the append-only format that
[Nebari][nebari] began with.

Each blob stored is assigned a unique `GrainId`. Once a blob is no longer
needed, its `GrainId` can be archived. Once the database's log has been
checkpointed far enough, the blob will be fully freed, and its storage will be
able to be reused.

## Goals of this format

The ultimate goal is for this crate to sit below [Nebari][nebari]. These are the
specific features this crate is aiming to provide to achieve that goal:

* ACID-complaint, single-file storage.
* Allows storing arbitrary chunks of data, returning an opaque ID that can be
  used to look that data up again in the future.
* Supports reusing reclaimed storage space.
* Supports simultaneous writers with automatic batching of commits.
* MVCC-transactions with the ability to roll back modifications instead of
  committing.
* Embedding a header: The database's header will contain an optional `GrainId`
  that can be used to point to an embedded header.

## One-fsync ACID writes

This file's format stores several pieces of information in pairs. Each batch
written is assigned a new `BatchId`. This means the higher value is the most
recently written value.

The file header is stored twice at the start of the file. When opening an
existing database, the header with the largest BatchId is validated. If it
cannot be validated, the in-memory state is initialized with the older version.
When writing a new version of the header, it will overwrite the version that was
not used to load from disk.

The file header contains a list of "basins", which are also stored in pairs. The
file header will note which of the pair is active, allowing for basin headers to
only be rewritten when changed.

The file header also points to the head page of a commit log. The commit log
page contains an entry for each `BatchId` written, and a pointer to a previous
location in the file if there are more batches before the current page. Each
pointed-to log entry contains a list of grain operations performed.

To validate the file header, each of the grains in the most recent commit must
be validated in addition to all of the headers touched during the commit. If any
parts cannot be validated, the entire commit must be rolled back and the bad
data scrubbed.

This design allows the commit operation to queue up all of the writes and
perform a single `fsync` operation and be able to recover safely regardless of a
power failure or crash before or during an fsync operation.

## Theoretical Limits

* Up to 254 Basins per file
* Up to 254 Strata per Basin
* Up to 64,516 Strata per file
* Up to 281 trillion Grains per Stratum
* Up to 18 quintillion Grains per file
* Up to 4GB (2^32 bytes) per Grain

[nebari]: https://github.com/khonsulabs/nebari
