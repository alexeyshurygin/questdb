# Primary/Replica Replication

## Overview

A QuestDB node can run as a **primary** that uploads its committed WAL
transactions to an object store, or as a **replica** that follows the object
store as a read-only hot standby. The settings, their defaults and the behaviour
they describe follow the replication section of QuestDB Enterprise's
`server.conf`. The object layout and the code are this fork's own, so a store
written by this fork cannot be read by QuestDB Enterprise and the other way
round.

Supported object stores: AWS S3 (and S3-compatible stores such as MinIO) and a
filesystem directory (for development and tests).

```properties
# primary
replication.role=primary
replication.object.store=s3::bucket=my-bucket;root=prod-db;region=ap-northeast-1;
# group transactions for 1 s instead of the default 10 s
replication.primary.throttle.window.duration=1000

# replica: the same object store string
replication.role=replica
replication.object.store=s3::bucket=my-bucket;root=prod-db;region=ap-northeast-1;
```

The `access_key_id`/`secret_access_key`/`session_token` keys of the store string
are optional. Without them the S3 client reads `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` from the environment, and then
the EC2 instance role through IMDSv2. In a container on EC2, IMDSv2 needs a
metadata hop limit of 2 (`http_put_response_hop_limit`).
`replication.object.store` is a sensitive setting: SHOW PARAMETERS masks it, and
`replication.object.store.file` (or `QDB_REPLICATION_OBJECT_STORE_FILE`) can name
a file that holds it.

## What replicates

- WAL tables: inserts (SQL, ILP, QWP), UPDATE, every ALTER, TRUNCATE, dedup
  settings, column type changes, partition drops, RENAME TABLE and DROP TABLE.
- Not in this version: views, materialized views and live views (live views are
  node-local in QuestDB Enterprise too), and non-WAL tables. A primary logs an
  advisory for each such object it skips.

A replica appends each shipped transaction to its own sequencer with the
primary's WAL coordinates and commit timestamp, so transaction numbers,
structure versions and the `now()` of a replayed UPDATE match the primary's. The
regular WAL apply job then applies them.

## Roles and write fencing

`ReplicatedCairoEngine` refuses client writes unless the node is a writable
primary. `isReadOnlyMode()` is true on:

| Engine role | Meaning |
|---|---|
| `PRIMARY_CATCHUP` | configured as primary, still applying what the store holds or waiting for the lease |
| `PRIMARY` | holds the lease; accepts writes and uploads them |
| `REPLICA` | follows the store; refuses writes |
| `FENCED` | a primary that lost the lease, found diverging history or another database in the store |

Refused writes fail with `replica access is read-only`, the message QuestDB
uses for every read-only refusal. QWP clients see the replica role. Queries work
in every role. Live views, system tables and the temporary tables of query
exports stay writable on every node.

## Object store layout

```
_index                                        commit point: DataID, ownership lease, table catalog
_checkpoints/<instance id>                    checkpoint history of one instance
t/<table id>_<create ts>/create               table create manifest (sequencer _meta.0)
t/<table id>_<create ts>/seq/<part>.<epoch>    shipped transaction log records
t/<table id>_<create ts>/wal/<walId>/<segment>/<lo>-<hi>.<epoch>
                                              byte ranges of WAL segment transactions lo..hi
```

Every object is wrapped in a 32-byte envelope: zstd compression
(`replication.primary.compression.level`) and a CRC32C checksum
(`replication.primary.checksum`: by default only for stores that do not verify
integrity themselves, that is the filesystem store).

The primary uploads per batch, for each touched WAL segment, one delta object
with the byte ranges the new segment transactions occupy (derived from the
committed event records, never from file lengths), then the transaction log
parts holding the batch's records, and finally replaces `_index` with a
conditional write (`If-Match`). A batch becomes visible to replicas only with
that index write. WAL segments and sequencer parts that are not uploaded yet
keep an `upload.pending` marker, which stops the WAL purge job from deleting
them.

## Guardrails

1. **Ownership lease.** The index names the primary that owns the store. Every
   index write renews it; an idle primary renews it every
   `replication.primary.keepalive.interval`. Another node takes over only when
   the lease was released by a graceful stop, or the index has not changed for
   three keepalive intervals, measured on the taking node's monotonic clock.
2. **Conditional index writes.** Two primaries can never both commit: the one
   whose write loses the compare-and-swap fences itself.
3. **Epochs.** Each new primary term writes its objects under a new epoch, so a
   deposed primary cannot overwrite what its successor wrote.
4. **Catch-up before writes.** A starting primary applies everything the store
   holds, takes the lease, and catches up again before it opens writes.
5. **Divergence check.** A node whose transaction at a given number differs from
   the store's (for example a crashed primary with transactions it never
   uploaded, after a failover) is fenced and never serves the different history.
6. **DataID.** A node only joins a store of the same database. A replica adopts
   the store's DataID; an empty node starting as primary does too.
7. **Gap check.** A replica that needs transactions the store no longer holds
   stops replicating that table and says so.
8. **Strict configuration.** An unknown role, a malformed store string, or
   `readonly=true` combined with a role stops the server at startup.
9. **Asynchronous.** A transaction is acknowledged before it is uploaded. If the
   primary dies, the transactions of its last throttle window can be lost, as
   in QuestDB Enterprise.

## Promotion

1. Stop the primary (a graceful stop uploads what is committed and releases the
   lease), or lose it.
2. Restart the replica with `replication.role=primary`. It catches up, takes the
   lease (at once after a graceful stop, after three keepalive intervals
   otherwise) and opens writes.
3. Restart the former primary with `replication.role=replica` to make it follow
   the new primary. If it had transactions it never uploaded, it is fenced
   instead and needs a fresh copy (see bootstrap).

## Bootstrap and disaster recovery

- **Fresh pair:** start both empty; the replica follows from the first
  transaction.
- **Existing database:** enable `replication.role=primary` on it. Tables that
  existed before replication start replicating at their next transaction. To add
  a replica, run `CHECKPOINT CREATE` on the primary, copy the install root's `db`
  and `.checkpoint` directories (rsync, or a volume snapshot), run
  `CHECKPOINT RELEASE`, put a `_restore` file in the copy's install root, and
  start the copy with `replication.role=replica`. A copy of a stopped primary's
  `db` directory works too. Do not copy `.replication/`: the instance id there
  identifies a node and must not travel with a copy.
- **Disaster recovery without a replica:** restore a checkpoint-consistent volume
  snapshot on a new node and start it with `replication.role=primary`. It rolls
  every table forward from the store to the last uploaded transaction, provided
  the WAL cleaner has not removed the transactions after the snapshot.

## Checkpoint history and the WAL cleaner

Every node records its checkpoints in `_checkpoints/<instance id>` when
`CHECKPOINT RELEASE` runs (`checkpoint.history.*`). On the primary, the WAL
cleaner (`replication.primary.cleaner.*`) deletes a table's history up to the
point every instance's newest `replication.primary.cleaner.backup.window.count`
checkpoints have passed, raising the table's first transaction in the index
before it deletes anything, and deletes dropped tables after
`replication.primary.cleaner.dropped.table.cooloff`. Until an instance records
that many checkpoints, nothing is cleaned.

## Not in this version

- In-place role switch: promotion and demotion need a restart.
- Rewinding a former primary's unuploaded transactions: it is fenced and needs a
  fresh copy.
- Azure Blob Storage and Google Cloud Storage.
