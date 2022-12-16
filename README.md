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

[okaywal]: https://github.com/khonsulabs/okaywal
