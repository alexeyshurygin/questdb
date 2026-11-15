# Primary/Replica Replication

## Overview

A QuestDB node can run as a **primary** that uploads its committed WAL
transactions to an object store, or as a **replica** that follows the object
store as a read-only hot standby. The setting names and defaults are those of
the replication section of QuestDB Enterprise's `server.conf`; the ownership
lease follows the description there. The code and the object layout are this
fork's own, and the behaviour differs from QuestDB Enterprise in the ways listed
under "Differences from QuestDB Enterprise".

Supported object stores: AWS S3, S3-compatible stores that honour `If-Match` and
`If-None-Match` on PutObject, and a filesystem directory (for development and
tests). The Docker suite runs against the moto S3 emulator.

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
metadata hop limit of 2 (`http_put_response_hop_limit`). EKS pod identity and
ECS task roles are not read.
`replication.object.store` is a sensitive setting: SHOW PARAMETERS masks it, and
`replication.object.store.file` (or `QDB_REPLICATION_OBJECT_STORE_FILE`) can name
a file that holds it.

The S3 bucket:

- Keep versioning off, or expire noncurrent versions with a lifecycle rule. The
  primary rewrites `_index` at least every keepalive interval; on a versioned
  bucket that is about 100,000 versions in 12 days, and S3 throttles a bucket
  that holds objects with very many versions.
- The role needs `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and
  `s3:ListBucket`. A conditional replace needs `s3:GetObject` too.
- Directory buckets (S3 Express One Zone) do not work: their listings are not
  ordered. Default encryption with SSE-KMS is untested.

## What replicates

- WAL tables: inserts, UPDATE, TRUNCATE, RENAME TABLE, DROP TABLE, and the
  ALTERs the tests cover: add, drop and rename a column, change a column's type,
  dedup settings, TTL, drop a partition, convert a partition to Parquet and
  back. The data arrives through whichever protocol wrote it; the JUnit tests
  write through SQL and WAL writers, the Docker suite through SQL and ILP over
  HTTPS.
- `ATTACH PARTITION` and `DETACH PARTITION` replicate as commands only: no
  partition files travel. Converting a table to or from WAL is not replicated.
  `ALTER TABLE ... REBASE WAL` is refused on a replicated node.
- Not in this version: views, materialized views, live views and non-WAL
  tables. They stay local to the node that created them, and a replica refuses
  to create a view or a materialized view. A primary logs an advisory for the
  views and materialized views it finds when its term starts.

A replica appends each shipped transaction to its own sequencer with the
primary's WAL coordinates and commit timestamp, so transaction numbers,
structure versions and the `now()` of a replayed UPDATE match the primary's. The
regular WAL apply job then applies them.

## Roles and write fencing

`ReplicatedCairoEngine` refuses client writes unless the node is a writable
primary: `isReadOnlyMode()` is false in the `PRIMARY` role only.

| Engine role | Meaning |
|---|---|
| `PRIMARY_CATCHUP` | configured as primary, still applying what the store holds, waiting for the lease, or waiting for the store to answer |
| `PRIMARY` | holds the lease; accepts writes and uploads them |
| `REPLICA` | follows the store; refuses writes |
| `FENCED` | a primary that lost the lease, found diverging history or another database in the store, or lost its coordinator; read-only until restarted |

Refused writes fail with `replica access is read-only`, the message QuestDB
uses for every read-only refusal. QWP clients see the primary-catch-up role on
a starting primary and the replica role on replicas and fenced nodes. Queries
work in every role, from the node's local tables. Live views, system tables and
non-WAL tables stay writable at the engine on every node; SQL still refuses an
INSERT on a read-only node.

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
   three keepalive intervals. The taking node counts those on its own monotonic
   clock with its own setting, so give every node the same keepalive interval.
   The object store enforces none of this: it only compares versions. A node
   that restarts with the instance id of the current owner takes over without
   waiting, which is why `.replication/` must never be copied to another node.
2. **Conditional index writes.** Two primaries can never both commit: the store
   accepts an index only from the writer that saw the previous one, and the
   primary whose write loses fences itself. Until that write, at most one
   keepalive interval later, a deposed primary still acknowledges writes.
3. **Epochs.** Each new primary term writes its objects under a new epoch, so a
   deposed primary cannot overwrite what its successor wrote.
4. **Catch-up before writes.** A starting primary applies everything the store
   holds, takes the lease, and catches up again before it opens writes. While
   the store is unreachable it stays read-only and keeps trying.
5. **Divergence check.** A node compares its history with the store's before
   it extends either. A node starting as primary that holds a different
   transaction at the same number (a crashed primary with transactions it never
   uploaded, after a failover) fences itself and uploads none of it. On a
   replica, the table that differs stops replicating and the log says so; the
   node stays a replica and keeps serving its local rows of that table, so it
   must be re-seeded.
6. **DataID.** A node only joins a store of the same database. A replica adopts
   the store's DataID; an empty node starting as primary does too.
7. **Gap check.** A replica that needs transactions the store no longer holds
   stops replicating that table and says so.
8. **Strict configuration.** An unknown role, a malformed store string, or
   `readonly=true` combined with a role stops the server at startup.
9. **Asynchronous.** A transaction is acknowledged before it is uploaded. If the
   primary dies, what it had not uploaded yet is lost: up to one throttle
   window of transactions, more while uploads are failing. A primary that
   cannot reach the object store keeps accepting writes and uploads them when
   the store is back. It does not stop when its lease runs out, so stop the old
   primary, or make sure it cannot be reached, before promoting another node:
   what it accepts after the promotion is never replicated, and it fences
   itself only at its next index write that reaches the store.

## Promotion

1. Stop the primary, or lose it. A graceful stop uploads everything that is
   committed and then releases the lease. If it cannot upload all of it (the
   store is unreachable, the stop times out), it leaves the lease to expire, so
   a released lease always means a complete store.
2. Restart the replica with `replication.role=primary`. It catches up, takes the
   lease (at once after a graceful stop, after three keepalive intervals
   otherwise) and opens writes.
3. Restart the former primary with `replication.role=replica` to make it follow
   the new primary. If it had transactions it never uploaded, the tables they
   belong to stop replicating once the new primary writes those transaction
   numbers, and the node needs a fresh copy (see bootstrap).

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
  identifies a node. A node started with `_restore` replaces a copied id; a
  plain copy does not.
- **Disaster recovery without a replica:** restore a checkpoint-consistent volume
  snapshot on a new node and start it with `replication.role=primary`. It rolls
  every table forward from the store to the last uploaded transaction, provided
  the WAL cleaner has not removed the transactions after the snapshot. A table
  that existed before replication was enabled needs a snapshot taken after that.

## Checkpoint history and the WAL cleaner

Every node records its checkpoints in `_checkpoints/<instance id>` when
`CHECKPOINT RELEASE` runs (`checkpoint.history.*`). On the primary, the WAL
cleaner (`replication.primary.cleaner.*`) looks at the newest
`replication.primary.cleaner.backup.window.count` checkpoints of every instance
that recorded at least that many, and deletes a table's history up to the
oldest of them, raising the table's first transaction in the index before it
deletes anything. It deletes dropped tables after
`replication.primary.cleaner.dropped.table.cooloff`. Until an instance records
that many checkpoints, nothing is cleaned: a store whose nodes never run
`CHECKPOINT RELEASE` grows without limit. An instance that is gone keeps
holding history back until its `_checkpoints/<instance id>` object is deleted
by hand. The cleaner retries a deletion that
fails on its next run. It keeps that list in memory: a primary that restarts
before the retry leaves those objects in the store, where no index names them
any more.

## Differences from QuestDB Enterprise

Checked against the QuestDB Enterprise documentation; where it is silent, this
says so.

- **Role switch.** Enterprise promotes and demotes in place (`SWITCH ROLE`).
  Here both need a restart.
- **Promotion guard.** Enterprise refuses a promotion while the node is behind
  the store, and its restart path needs a `_migrate_primary` file. Here a node
  started with `replication.role=primary` catches up and takes the lease by
  itself.
- **Former primary.** Enterprise lets it rejoin as a replica and discards the
  transactions it never uploaded. Here those tables stop replicating and the
  node needs a fresh copy.
- **Views.** Enterprise keeps materialized views in sync on replicas and
  replicates live view definitions. Here views of every kind are node-local.
- **Commit mode.** A node with a replication role defaults `cairo.commit.mode`
  to `adaptive`. Enterprise leaves the default at `nosync` and tells operators
  to set `adaptive` on every node that can become primary.
- **Store outage.** The Enterprise documentation does not say what a primary
  does while the store is unreachable. Here it keeps accepting writes.
- **Settings without effect.** These are accepted and range-checked, and do
  nothing: `replication.summary.interval`, `replication.metrics.per.table`,
  `replication.metrics.dropped.table.poll.count`,
  `replication.requests.max.batch.size.slow`,
  `replication.primary.compression.threads`,
  `replication.primary.index.upload.throttle.interval`,
  `replication.primary.cleaner.progress.write.interval`,
  `replication.primary.cleaner.retry.attempts` and
  `replication.primary.cleaner.retry.interval`. There are no per-table
  replication metrics and no progress summary in the log.
- **Missing.** `replication.disabled.tables`, the `primary-catchup-uploads`
  role, durable acknowledgements of replicated transactions, point-in-time
  recovery, seeding a replica from an object-store backup, Azure Blob Storage,
  Google Cloud Storage, and the `ca_cert_file` store option.
- **Stores are not interchangeable** with QuestDB Enterprise's: the object
  layout is this fork's own.

## Not in this version

- In-place role switch: promotion and demotion need a restart.
- Rewinding a former primary's unuploaded transactions: its tables stop
  replicating and it needs a fresh copy.
- Azure Blob Storage and Google Cloud Storage.
