/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/


package io.questdb.replication;

import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.NativeReader;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.replication.codec.TableCreateManifest;
import io.questdb.replication.codec.TxnPart;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.std.IntList;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Brings one table on this node up to the object store: creates it when it is new, then downloads
 * and applies its transactions in order. Used by replicas and by a primary catching up before it
 * opens writes. Keeps the downloaded transaction log part and how far each local WAL segment was
 * written, so that a run that starts where the previous one stopped downloads nothing twice.
 * One instance per table; never used by two threads at once.
 */
public class TableReplayer {
    private static final Log LOG = LogFactory.getLog(TableReplayer.class);
    // (walId << 32 | segmentId) -> last segment txn written locally
    private final LongLongHashMap segmentHighWater = new LongLongHashMap(16, 0.5, -1, -1);
    // WALs whose transactions the current batch sequenced under a durable commit mode
    private final IntList replayedWalIds = new IntList();
    private final String tableKey;
    // the unapplied tail was written again from the store after this start
    private boolean isTailRepaired;
    private TxnPart cachedPart;
    private long cachedPartEpoch = -1;
    private static final int MAX_MISSING_ROUNDS = 5;
    private boolean isDone;
    private int missingRounds;
    private long prefixCheckedTxn;
    private String staleReason;
    private TableToken token;

    public TableReplayer(@NotNull String tableKey) {
        this.tableKey = tableKey;
    }

    private static long segmentKey(int walId, int segmentId) {
        return ((long) walId << 32) | (segmentId & 0xffffffffL);
    }

    /**
     * True once a replicated drop was applied: nothing more will ever arrive for the table.
     */
    public boolean isDone() {
        return isDone;
    }

    /**
     * Why the table stopped replicating, or null while it replicates.
     */
    public String getStaleReason() {
        return staleReason;
    }

    public boolean isStale() {
        return staleReason != null;
    }

    /**
     * Applies up to {@code maxTxnCount} of the transactions the object store holds past the local
     * log.
     *
     * @return the number of transactions applied
     */
    public long replay(
            @NotNull ReplicatedCairoEngine engine,
            @NotNull IndexTable entry,
            @NotNull ReplicationWorker worker,
            @Nullable ReplicaSegmentPins pins,
            long maxTxnCount
    ) {
        if (isDone || staleReason != null) {
            return 0;
        }
        try {
            final long applied = replay0(engine, entry, worker, pins, maxTxnCount);
            missingRounds = 0;
            return applied;
        } catch (ReplicationException e) {
            if (e.getKind() == ReplicationException.Kind.MISSING && ++missingRounds < MAX_MISSING_ROUNDS) {
                LOG.info().$("object missing from the store, retrying with a fresh index [table=").$(entry.getTableName())
                        .$(", error=").$safe(e.getMessage()).I$();
                reset();
                return 0;
            }
            staleReason = e.getMessage();
            LOG.critical().$("table stopped replicating [table=").$(entry.getTableName()).$(", key=").$(tableKey)
                    .$(", kind=").$(e.getKind().name()).$(", error=").$safe(e.getMessage()).I$();
            return 0;
        }
    }

    private void applyDelta(
            ReplicatedCairoEngine engine,
            TxnRecord record,
            ReplicationWorker worker,
            boolean isDurable,
            @Nullable ReplicaSegmentPins pins
    ) {
        final long segKey = segmentKey(record.getWalId(), record.getSegmentId());
        final long highWater = segmentHighWater.get(segKey);
        if (highWater != -1 && highWater >= record.getSegmentTxn()) {
            return;
        }
        if (record.getDeltaEpoch() < 0 || record.getDeltaFirstSegmentTxn() < 0
                || record.getSegmentTxn() < record.getDeltaFirstSegmentTxn()
                || record.getSegmentTxn() > record.getDeltaLastSegmentTxn()) {
            throw ReplicationException.corrupt("data transaction does not name its WAL delta [table=" + entry(token)
                    + ", txn=" + record.getTxn() + ']');
        }
        final String key = StoreKeys.segmentDelta(tableKey, record.getWalId(), record.getSegmentId(),
                record.getDeltaFirstSegmentTxn(), record.getDeltaLastSegmentTxn(), record.getDeltaEpoch());
        final NativeBuffer buf = worker.getBuffer();
        if (!worker.getIo().get(key, buf)) {
            throw ReplicationException.missing("WAL delta is missing from the object store [key=" + key + ']');
        }
        final SegmentDelta delta;
        try {
            delta = worker.getDelta().of(buf.ptr(), buf.size());
        } catch (NativeReader.CorruptObjectException e) {
            throw corruptDelta(key, e);
        }
        if (delta.getWalId() != record.getWalId() || delta.getSegmentId() != record.getSegmentId()
                || delta.getFirstSegmentTxn() != record.getDeltaFirstSegmentTxn()
                || delta.getLastSegmentTxn() != record.getDeltaLastSegmentTxn()) {
            throw ReplicationException.corrupt("WAL delta does not match its key [key=" + key + ']');
        }
        try {
            // the ranges decode while they are written
            worker.getDeltaWriter().apply(engine.getConfiguration().getDbRoot(), token.getDirName(), delta, isDurable);
        } catch (NativeReader.CorruptObjectException e) {
            throw corruptDelta(key, e);
        }
        segmentHighWater.put(segKey, delta.getLastSegmentTxn());
        if (pins != null) {
            pins.onDownloaded(token.getDirName(), record.getWalId(), record.getSegmentId(), System.currentTimeMillis());
        }
    }

    /**
     * Verifies that this node's last transaction that the store also holds is the same transaction in
     * both: equal WAL coordinates, structure version, commit timestamp and change.
     */
    void checkCommonPrefix(ReplicatedCairoEngine engine, IndexTable entry, ReplicationWorker worker, long local) {
        final long txn = Math.min(local, entry.getLastTxn());
        if (txn < entry.getFirstTxn() || txn < 1) {
            return;
        }
        final long partNo = IndexTable.partOf(txn, entry.getPartTxnCount());
        final TxnPart p = loadPart(entry, worker, partNo, entry.getAuthoritativeEpoch(partNo), txn);
        final TxnRecord stored = p.getRecord(txn);
        final TxnRecord localRecord = new TxnRecord();
        // by the entry's directory: the service calls this before the first replay resolved the token
        final TxnLogReader reader = worker.getTxnLogReader().of(engine.getConfiguration().getDbRoot(), entry.getDirName());
        try {
            reader.readRecord(txn, localRecord);
        } finally {
            reader.closeFiles();
        }
        if (!localRecord.isSameTxn(stored)) {
            throw ReplicationException.divergence("local history differs from the object store [table=" + entry.getTableName()
                    + ", txn=" + txn + ", local=" + localRecord + ", store=" + stored + ']');
        }
    }

    private static ReplicationException corruptDelta(String key, NativeReader.CorruptObjectException e) {
        return ReplicationException.corrupt("corrupt WAL delta [key=" + key + ", error=" + e.getMessage() + ']');
    }

    private String entry(TableToken t) {
        return t != null ? t.getTableName() : tableKey;
    }

    private TxnPart loadPart(IndexTable entry, ReplicationWorker worker, long part, long epoch, long txn) {
        if (cachedPart != null && cachedPart.getPart() == part && cachedPartEpoch == epoch && cachedPart.getLastTxn() >= txn) {
            return cachedPart;
        }
        if (epoch < 0) {
            throw ReplicationException.corrupt("no epoch wrote the transaction log part [table=" + entry.getTableName()
                    + ", part=" + part + ']');
        }
        final String key = StoreKeys.seqPart(tableKey, part, epoch);
        final NativeBuffer buf = worker.getPartBuffer();
        if (!worker.getIo().get(key, buf)) {
            throw ReplicationException.missing("transaction log part is missing from the object store [key=" + key + ']');
        }
        final TxnPart p;
        try {
            p = TxnPart.decode(buf.ptr(), buf.size());
        } catch (NativeReader.CorruptObjectException e) {
            throw ReplicationException.corrupt("corrupt transaction log part [key=" + key + ", error=" + e.getMessage() + ']');
        }
        if (p.getTableId() != entry.getTableId() || p.getCreateTimestamp() != entry.getCreateTimestamp()
                || p.getPart() != part || p.getPartTxnCount() != entry.getPartTxnCount()) {
            throw ReplicationException.corrupt("transaction log part does not match its table [key=" + key + ']');
        }
        if (p.getLastTxn() < txn) {
            // the index commits after the part upload, so this is an inconsistent store, not a race
            throw ReplicationException.corrupt("transaction log part ends before the index says [key=" + key
                    + ", partLastTxn=" + p.getLastTxn() + ", needed=" + txn + ']');
        }
        cachedPart = p;
        cachedPartEpoch = epoch;
        return p;
    }

    private long replay0(
            ReplicatedCairoEngine engine,
            IndexTable entry,
            ReplicationWorker worker,
            @Nullable ReplicaSegmentPins pins,
            long maxTxnCount
    ) {
        final TableSequencerAPI api = engine.getTableSequencerAPI();
        if (token == null) {
            token = engine.getTableTokenByDirName(entry.getDirName());
            if (token == null) {
                // a dropped table ends absent: a node without it has nothing to replay, and the directory
                // of a drop this node applied itself may still wait for the purge job
                if (entry.isDropped()) {
                    isDone = true;
                    return 0;
                }
                if (entry.getFirstTxn() > 1) {
                    throw ReplicationException.gap("the object store holds the table from transaction " + entry.getFirstTxn()
                            + " only; bootstrap this node from a checkpoint copy [table=" + entry.getTableName() + ']');
                }
                final NativeBuffer buf = worker.getBuffer();
                final String key = StoreKeys.create(tableKey);
                if (!worker.getIo().get(key, buf)) {
                    throw ReplicationException.missing("table create manifest is missing from the object store [key=" + key + ']');
                }
                final TableCreateManifest manifest;
                try {
                    manifest = TableCreateManifest.decode(buf.ptr(), buf.size());
                } catch (NativeReader.CorruptObjectException e) {
                    throw ReplicationException.corrupt("corrupt table create manifest [key=" + key + ", error=" + e.getMessage() + ']');
                }
                if (manifest.getTableId() != entry.getTableId() || manifest.getCreateTimestamp() != entry.getCreateTimestamp()
                        || !manifest.getDirName().equals(entry.getDirName())) {
                    throw ReplicationException.corrupt("table create manifest does not match its table [key=" + key + ']');
                }
                token = worker.getTableApplier().createTable(manifest);
            }
        }
        long local = api.lastTxn(token);
        final long common = Math.min(local, entry.getLastTxn());
        if (common > prefixCheckedTxn) {
            // a node that wrote history of its own (a former primary) must never extend it with the store's
            checkCommonPrefix(engine, entry, worker, local);
            prefixCheckedTxn = common;
        }
        if (!isTailRepaired) {
            repairUnappliedTail(engine, entry, worker, pins, common);
            isTailRepaired = true;
        }
        if (local < entry.getFirstTxn() - 1) {
            throw ReplicationException.gap("the object store no longer holds the transactions after this node's last one [table="
                    + entry.getTableName() + ", local=" + local + ", storeFirst=" + entry.getFirstTxn() + ']');
        }
        if (local >= entry.getLastTxn()) {
            // ahead of the store, as a copy taken before the primary uploaded is: wait for the store
            return 0;
        }
        // The engine resolves the mode. A replica does not flush what it replays: the object store is its
        // durable copy. A primary catching up flushes as its own writers do.
        final boolean isDurable = CommitMode.isPowerLossDurable(engine.getResolvedCommitMode());
        final MicrosecondClock clock = engine.getConfiguration().getMicrosecondClock();
        final WalLocker walLocker = engine.getWalLocker();
        long applied = 0;
        long rows = 0;
        try {
            while (local < entry.getLastTxn() && applied < maxTxnCount) {
                final long txn = local + 1;
                final long part = IndexTable.partOf(txn, entry.getPartTxnCount());
                final TxnPart p = loadPart(entry, worker, part, entry.getAuthoritativeEpoch(part), txn);
                final TxnRecord record = p.getRecord(txn);
                if (record.isData()) {
                    // a WAL writer lock keeps the purge job off the segment until its transaction is sequenced
                    walLocker.lockWriter(token, record.getWalId(), record.getSegmentId());
                    try {
                        applyDelta(engine, record, worker, isDurable, pins);
                        token = worker.getTableApplier().apply(token, record);
                        // the sequencer pins by its own rule, whatever this node flushes
                        if (!replayedWalIds.contains(record.getWalId())) {
                            replayedWalIds.add(record.getWalId());
                        }
                    } finally {
                        walLocker.unlockWriter(token, record.getWalId());
                    }
                    rows += record.getRowCount();
                } else {
                    token = worker.getTableApplier().apply(token, record);
                }
                local = txn;
                applied++;
                if (record.isDrop()) {
                    isDone = true;
                    break;
                }
            }
        } catch (Throwable th) {
            try {
                releaseDurabilityPins(api);
            } catch (Throwable e) {
                th.addSuppressed(e);
            }
            throw th;
        }
        releaseDurabilityPins(api);
        if (applied > 0 && !isDone) {
            engine.getRecentWriteTracker().recordReplicaDownload(token, local, clock.getTicks(), rows, local < entry.getLastTxn());
        }
        return applied;
    }

    /**
     * Writes again, from the object store, the WAL data of the transactions this node replayed before
     * it last stopped and has not applied to the table yet. A replica does not flush what it replays,
     * so after a power loss its sequencer log can name transactions whose WAL data never reached the
     * disk; the apply job would fail on them and suspend the table. The store holds all of it, so
     * nothing local has to be intact. Runs once per table after a start, before anything new is
     * replayed; what the table already applied is left alone, and so is what the store no longer holds.
     */
    private void repairUnappliedTail(
            ReplicatedCairoEngine engine,
            IndexTable entry,
            ReplicationWorker worker,
            @Nullable ReplicaSegmentPins pins,
            long common
    ) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        final long appliedTxn = tracker.getWriterTxn();
        final long from = Math.max(Math.max(appliedTxn, 0) + 1, entry.getFirstTxn());
        if (from > common) {
            return;
        }
        final WalLocker walLocker = engine.getWalLocker();
        long repaired = 0;
        for (long txn = from; txn <= common; txn++) {
            final long part = IndexTable.partOf(txn, entry.getPartTxnCount());
            final TxnRecord record = loadPart(entry, worker, part, entry.getAuthoritativeEpoch(part), txn).getRecord(txn);
            if (!record.isData()) {
                continue;
            }
            walLocker.lockWriter(token, record.getWalId(), record.getSegmentId());
            try {
                applyDelta(engine, record, worker, false, pins);
                repaired++;
            } catch (ReplicationException e) {
                if (e.getKind() != ReplicationException.Kind.MISSING) {
                    throw e;
                }
                // the cleaner removed it: the local copy is all there is
                LOG.info().$("WAL data of an unapplied transaction is no longer in the object store [table=").$(token)
                        .$(", txn=").$(txn).I$();
            } finally {
                walLocker.unlockWriter(token, record.getWalId());
            }
        }
        if (repaired > 0) {
            LOG.info().$("rewrote the WAL data of unapplied transactions from the object store [table=").$(token)
                    .$(", from=").$(from).$(", to=").$(common).I$();
            engine.notifyWalTxnCommitted(token);
        }
    }

    /**
     * Under a durable commit mode the sequencer pins every transaction it appends for the WAL writer
     * that wrote it, and holds the table's durable frontier below the pin until that writer flushes
     * the sequencer log. The writer of a replicated transaction is on the primary, so this node flushes
     * the log and drops the pins itself, once per batch. Left alone, the pins would keep the frontier
     * where it was: a node that catches up and then becomes the primary would upload nothing more, and
     * a replica would collect one pin for every WAL the primary ever opened.
     */
    private void releaseDurabilityPins(TableSequencerAPI api) {
        if (replayedWalIds.size() == 0) {
            return;
        }
        try {
            if (!isDone) {
                final SeqTxnTracker tracker = api.getTxnTrackerIfExists(token);
                // no pin, as under nosync, means nothing to flush and nothing to release
                if (tracker != null && tracker.getPendingWriterPinCount() > 0) {
                    final long orphanSweepMark = tracker.snapshotOrphanSweepMark();
                    final long covered = api.fdatasyncTxnLog(token);
                    for (int i = 0, n = replayedWalIds.size(); i < n; i++) {
                        tracker.markWriterDurable(replayedWalIds.getQuick(i), orphanSweepMark, covered);
                    }
                }
            }
        } finally {
            replayedWalIds.clear();
        }
    }

    /**
     * Forgets cached state after the table changed underneath (for example a failed apply).
     */
    public void reset() {
        cachedPart = null;
        cachedPartEpoch = -1;
        segmentHighWater.clear();
    }
}
