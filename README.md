# Sediment

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

[okaywal]: https://github.com/khonsulabs/okaywal
