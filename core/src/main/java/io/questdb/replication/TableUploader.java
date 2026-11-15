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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.TableCreateManifest;
import io.questdb.replication.codec.TxnPart;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Uploads one table's committed transactions on the primary. Each batch ships, per touched WAL
 * segment, one delta object with the segment transactions the batch adds, then the transaction log
 * part(s) holding the batch's records; the index commit that follows makes the batch visible. Keeps
 * which segments it still owes an upload, so that their {@code upload.pending} markers come off
 * once the store holds them. One instance per table; never used by two threads at once.
 */
public class TableUploader {
    private static final Log LOG = LogFactory.getLog(TableUploader.class);
    // segments closed by their writer -> the writer's last sequenced transaction at the close:
    // (walId << 32 | segmentId), plus whole WALs as (walId << 32 | 0xffffffff)
    private final HashMap<Long, Long> closedSegments = new HashMap<>();
    private final IndexTable entry = new IndexTable();
    // the entry as it was before the batch being uploaded touched it
    private final IndexTable entryBefore = new IndexTable();
    private final ObjList<TxnRecord> batch = new ObjList<>();
    // segment -> highest txn referencing it that was uploaded
    private final HashMap<Long, Long> segmentRefs = new HashMap<>();
    // segments found at startup: their writers did not survive the restart
    private final HashSet<Long> startupSegments = new HashSet<>();
    private final String tableKey;
    private long committedTxn;
    private boolean isCreateCommitted;
    private boolean isCreateUploaded;
    private boolean isDropCommitted;
    // records were added to the current part since it was last uploaded
    private boolean isPartDirty;
    private int localPartSize;
    private TxnPart part;
    private long pendingTxn;
    private long startupLastTxn = -1;
    private TableToken token;

    public TableUploader(@NotNull TableToken token, @NotNull IndexTable committedEntry, boolean isCreateCommitted) {
        this.token = token;
        this.entry.copyFrom(committedEntry);
        this.tableKey = committedEntry.getKey();
        this.committedTxn = committedEntry.getLastTxn();
        this.pendingTxn = committedTxn;
        this.isCreateCommitted = isCreateCommitted;
        this.isCreateUploaded = isCreateCommitted;
        this.isDropCommitted = committedEntry.isDropped();
    }

    private static long segmentKey(int walId, int segmentId) {
        return ((long) walId << 32) | (segmentId & 0xffffffffL);
    }

    private static long walKey(int walId) {
        return ((long) walId << 32) | 0xffffffffL;
    }

    /**
     * The table's index entry including the uploaded but not yet committed batch.
     */
    /**
     * The last transaction a committed index names for this table.
     */
    public long getCommittedTxn() {
        return committedTxn;
    }

    public IndexTable getEntry() {
        return entry;
    }

    public long getPendingTxn() {
        return pendingTxn;
    }

    public TableToken getToken() {
        return token;
    }

    public boolean hasPending() {
        return pendingTxn > committedTxn || (isCreateUploaded && !isCreateCommitted);
    }

    /**
     * True once the store holds the table's create manifest.
     */
    public boolean isCreateUploaded() {
        return isCreateUploaded;
    }

    public boolean isDropCommitted() {
        return isDropCommitted;
    }

    /**
     * Whether the uploader still needs sequencer part {@code partId} of the local log.
     */
    public boolean isSeqPartPinned(long partId) {
        if (localPartSize < 1) {
            return true;
        }
        return partId >= committedTxn / localPartSize;
    }

    /**
     * Loads the transaction log part the next upload continues, from the object store. Call once,
     * before the first upload of a primary term.
     */
    public void loadPart(@NotNull ReplicationWorker worker) {
        final long next = committedTxn + 1;
        final long partNo = IndexTable.partOf(next, entry.getPartTxnCount());
        // the store's first part of a table that existed before replication starts at its first replicated txn
        final long startTxn = Math.max(partNo * entry.getPartTxnCount() + 1, entry.getFirstTxn());
        part = new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp(), partNo, entry.getPartTxnCount(), startTxn);
        if (committedTxn < entry.getFirstTxn() || IndexTable.partOf(committedTxn, entry.getPartTxnCount()) != partNo) {
            // the next transaction starts a part, or the store holds nothing yet
            return;
        }
        final long epoch = entry.getAuthoritativeEpoch(partNo);
        final String key = StoreKeys.seqPart(tableKey, partNo, epoch);
        final NativeBuffer buf = worker.getPartBuffer();
        if (epoch < 0 || !worker.getIo().get(key, buf)) {
            throw ReplicationException.gap("transaction log part is missing from the object store [key=" + key + ']');
        }
        final TxnPart stored = TxnPart.decode(buf.ptr(), buf.size());
        stored.truncateAfter(committedTxn);
        if (stored.getLastTxn() != committedTxn) {
            throw ReplicationException.corrupt("transaction log part ends before the index says [key=" + key + ']');
        }
        part = stored;
    }

    public void onCommitted() {
        committedTxn = pendingTxn;
        if (isCreateUploaded) {
            isCreateCommitted = true;
        }
        if (entry.isDropped()) {
            isDropCommitted = true;
        }
    }

    /**
     * @param lastSeqTxn the writer's last sequenced transaction at the close; every transaction of the
     *                   segment is at or below it
     */
    public void onSegmentClosed(int walId, int segmentId, long lastSeqTxn) {
        closedSegments.merge(segmentKey(walId, segmentId), lastSeqTxn, Math::max);
    }

    public void onWalClosed(int walId, long lastSeqTxn) {
        closedSegments.merge(walKey(walId), lastSeqTxn, Math::max);
    }

    /**
     * Removes the {@code upload.pending} markers of every segment the store now holds entirely: closed
     * by its writer, and with the writer's last transaction at the close committed. The highest
     * transaction uploaded so far says nothing: a batch can end in the middle of a segment, and the
     * purge job deletes an applied segment as soon as its marker goes.
     */
    public void releaseMarkers(@NotNull ReplicationWalDirectoryPolicy policy, @NotNull Path path, @NotNull CharSequence dbRoot) {
        if (isCreateCommitted) {
            policy.removeMarker(path.of(dbRoot).concat(token.getDirName()).concat(WalUtils.SEQ_DIR));
        }
        final Iterator<Map.Entry<Long, Long>> it = segmentRefs.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Long, Long> e = it.next();
            final long key = e.getKey();
            final int walId = (int) (key >>> 32);
            final int segmentId = (int) key;
            Long closeTxn = closedSegments.get(key);
            if (closeTxn == null) {
                closeTxn = closedSegments.get(walKey(walId));
            }
            if (closeTxn != null && closeTxn <= committedTxn && e.getValue() <= committedTxn) {
                path.of(dbRoot).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
                policy.removeMarker(path);
                it.remove();
                closedSegments.remove(key);
            }
        }
        if (startupLastTxn > -1 && committedTxn >= startupLastTxn) {
            // every transaction of the segments found at startup is at or below the log's last one then
            for (long key : startupSegments) {
                path.of(dbRoot).concat(token.getDirName()).concat(WalUtils.WAL_NAME_BASE).put((int) (key >>> 32)).slash().put((int) key);
                policy.removeMarker(path);
                segmentRefs.remove(key);
            }
            startupSegments.clear();
            startupLastTxn = -1;
        }
    }

    /**
     * Notes the local log's last transaction at startup and the WAL segments that exist then, before
     * any writer opens: once the store holds that transaction, the markers of those segments can go.
     * A segment created later answers to its own close.
     */
    public void setStartupLastTxn(long startupLastTxn, @NotNull Path path, @NotNull CharSequence dbRoot, @NotNull FilesFacade ff) {
        this.startupLastTxn = startupLastTxn;
        startupSegments.clear();
        listSegments(path, dbRoot, ff);
    }

    public void setToken(@NotNull TableToken token) {
        this.token = token;
    }

    /**
     * Uploads transactions {@code (pendingTxn, targetTxn]}, at most {@code maxTxnCount} of them.
     *
     * @return the number of transactions uploaded
     */
    public long upload(
            @NotNull ReplicatedCairoEngine engine,
            @NotNull ReplicationWorker worker,
            long epoch,
            long targetTxn,
            long maxTxnCount
    ) {
        final CharSequence dbRoot = engine.getConfiguration().getDbRoot();
        if (!isCreateUploaded) {
            uploadCreateManifest(engine, worker);
        }
        if (targetTxn <= pendingTxn || entry.isDropped()) {
            return 0;
        }
        if (part == null) {
            loadPart(worker);
        }
        final long hi = Math.min(targetTxn, pendingTxn + maxTxnCount);
        final TxnLogReader reader = worker.getTxnLogReader().of(dbRoot, token.getDirName());
        try {
            localPartSize = reader.getPartSize() > 0 ? reader.getPartSize() : Integer.MAX_VALUE;
            batch.clear();
            for (long txn = pendingTxn + 1; txn <= hi; txn++) {
                final TxnRecord rec = new TxnRecord();
                reader.readRecord(txn, rec);
                batch.add(rec);
                if (rec.isDrop()) {
                    break;
                }
            }
        } finally {
            reader.closeFiles();
        }
        if (batch.size() == 0) {
            return 0;
        }
        uploadDeltas(dbRoot, worker, epoch);
        // From here the batch changes the part and the index entry before the store holds it. A failed
        // put must leave both where pendingTxn says they are: the next attempt reads the same
        // transactions again, and a keepalive must not commit an entry whose part was never written.
        final TxnPart partBefore = part;
        final boolean wasPartDirty = isPartDirty;
        entryBefore.copyFrom(entry);
        try {
            final long firstTxn = batch.getQuick(0).getTxn();
            entry.addEpochStart(epoch, firstTxn);
            final NativeBuffer buf = worker.getBuffer();
            for (int i = 0, n = batch.size(); i < n; i++) {
                final TxnRecord rec = batch.getQuick(i);
                if (part.isFull()) {
                    // a part filled by an earlier batch is in the store already, and the cleaner may have removed it
                    if (isPartDirty) {
                        uploadPart(worker, buf, epoch);
                    }
                    part = new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp(), part.getPart() + 1, entry.getPartTxnCount());
                }
                part.add(rec);
                isPartDirty = true;
                if (rec.isStructural() && rec.getPayload() != null
                        && ReplicaTableApplier.readCommand(rec.getPayload()) == io.questdb.griffin.engine.ops.AlterOperation.RENAME_TABLE) {
                    entry.setTableName(ReplicaTableApplier.readRenameTarget(rec.getPayload()));
                }
                if (rec.isDrop()) {
                    entry.setDropped(engine.getConfiguration().getMicrosecondClock().getTicks());
                }
            }
            uploadPart(worker, buf, epoch);
        } catch (Throwable th) {
            part = partBefore;
            part.truncateAfter(pendingTxn);
            // a part object this batch did write stays in the store with records the index does not
            // name yet; the retry adds the same records and writes it again
            isPartDirty = wasPartDirty;
            entry.copyFrom(entryBefore);
            throw th;
        }
        final long last = batch.getLast().getTxn();
        entry.setLastTxn(last);
        pendingTxn = last;
        return batch.size();
    }

    // collects wal<N>/<segment> directories of the table into startupSegments
    private void listSegments(Path path, CharSequence dbRoot, FilesFacade ff) {
        path.of(dbRoot).concat(token.getDirName());
        final int tableLen = path.size();
        final DirectUtf8StringZ name = new DirectUtf8StringZ();
        final long find = ff.findFirst(path.$());
        if (find < 1) {
            return;
        }
        try {
            do {
                if (ff.findType(find) != Files.DT_DIR) {
                    continue;
                }
                final String walName = name.of(ff.findName(find)).toString();
                if (!walName.startsWith(WalUtils.WAL_NAME_BASE)) {
                    continue;
                }
                final int walId;
                try {
                    walId = Integer.parseInt(walName.substring(WalUtils.WAL_NAME_BASE.length()));
                } catch (NumberFormatException e) {
                    continue;
                }
                path.trimTo(tableLen).concat(walName);
                final int walLen = path.size();
                final long segFind = ff.findFirst(path.$());
                if (segFind < 1) {
                    continue;
                }
                try {
                    do {
                        if (ff.findType(segFind) != Files.DT_DIR) {
                            continue;
                        }
                        final String seg = name.of(ff.findName(segFind)).toString();
                        final int segmentId;
                        try {
                            segmentId = Integer.parseInt(seg);
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        startupSegments.add(segmentKey(walId, segmentId));
                    } while (ff.findNext(segFind) > 0);
                } finally {
                    ff.findClose(segFind);
                }
            } while (ff.findNext(find) > 0);
        } finally {
            ff.findClose(find);
            path.trimTo(tableLen);
        }
    }

    private void uploadCreateManifest(ReplicatedCairoEngine engine, ReplicationWorker worker) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final Path path = Path.getThreadLocal(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(WalUtils.SEQ_DIR)
                .concat(WalUtils.INITIAL_META_FILE_NAME);
        final byte[] meta0 = readFile(ff, path);
        final TableCreateManifest manifest = new TableCreateManifest().of(
                entry.getTableId(),
                entry.getCreateTimestamp(),
                entry.getKind(),
                entry.getPartTxnCount(),
                entry.getDirName(),
                entry.getTableName(),
                meta0,
                null
        );
        final NativeBuffer buf = worker.getBuffer();
        manifest.encode(buf);
        worker.getIo().put(StoreKeys.create(tableKey), buf, true);
        isCreateUploaded = true;
        LOG.info().$("uploaded table create manifest [table=").$(token).I$();
    }

    private void uploadDeltas(CharSequence dbRoot, ReplicationWorker worker, long epoch) {
        // per segment, the batch's segment txns are contiguous: a segment's transactions are sequenced in order
        final HashSet<Long> seen = new HashSet<>();
        for (int i = 0, n = batch.size(); i < n; i++) {
            final TxnRecord rec = batch.getQuick(i);
            if (!rec.isData()) {
                continue;
            }
            final long key = segmentKey(rec.getWalId(), rec.getSegmentId());
            if (!seen.add(key)) {
                continue;
            }
            int lo = Integer.MAX_VALUE;
            int hi = -1;
            for (int j = i; j < n; j++) {
                final TxnRecord r = batch.getQuick(j);
                if (r.isData() && r.getWalId() == rec.getWalId() && r.getSegmentId() == rec.getSegmentId()) {
                    lo = Math.min(lo, r.getSegmentTxn());
                    hi = Math.max(hi, r.getSegmentTxn());
                }
            }
            final NativeBuffer buf = worker.getBuffer();
            final SegmentDeltaBuilder builder = worker.getDeltaBuilder();
            builder.build(dbRoot, token.getDirName(), rec.getWalId(), rec.getSegmentId(), lo, hi, buf);
            worker.getIo().put(StoreKeys.segmentDelta(tableKey, rec.getWalId(), rec.getSegmentId(), lo, hi, epoch), buf, true);
            // name the delta in every record it covers and fill in the statistics from the events
            final LongList stats = builder.getStats();
            for (int j = i; j < n; j++) {
                final TxnRecord r = batch.getQuick(j);
                if (r.isData() && r.getWalId() == rec.getWalId() && r.getSegmentId() == rec.getSegmentId()) {
                    r.setDelta(epoch, lo, hi);
                    final int s = (r.getSegmentTxn() - lo) * SegmentDeltaBuilder.STATS_STRIDE;
                    if (WalTxnType.isDataType((byte) stats.getQuick(s + 1))) {
                        r.setStatistics(stats.getQuick(s + 3), stats.getQuick(s + 4), stats.getQuick(s + 2));
                    } else {
                        r.setStatistics(Long.MAX_VALUE, -1, 0);
                    }
                    segmentRefs.merge(segmentKey(r.getWalId(), r.getSegmentId()), r.getTxn(), Math::max);
                }
            }
        }
    }

    private void uploadPart(ReplicationWorker worker, NativeBuffer buf, long epoch) {
        part.encode(buf);
        worker.getIo().put(StoreKeys.seqPart(tableKey, part.getPart(), epoch), buf, false);
        isPartDirty = false;
    }

    private static byte @Nullable [] readFile(FilesFacade ff, Path path) {
        final long fd = ff.openRO(path.$());
        if (fd < 0) {
            throw ReplicationException.corrupt("could not open table file [path=" + path + ']');
        }
        try {
            final long len = ff.length(fd);
            if (len < 0 || len > 64 * 1024 * 1024) {
                throw ReplicationException.corrupt("invalid table file length [path=" + path + ", length=" + len + ']');
            }
            final byte[] b = new byte[(int) len];
            final long buf = Unsafe.malloc(Math.max(1, len), io.questdb.std.MemoryTag.NATIVE_REPL);
            try {
                if (ff.read(fd, buf, len, 0) != len) {
                    throw ReplicationException.corrupt("could not read table file [path=" + path + ']');
                }
                Unsafe.copyMemory(null, buf, b, Unsafe.BYTE_OFFSET, len);
            } finally {
                Unsafe.free(buf, Math.max(1, len), io.questdb.std.MemoryTag.NATIVE_REPL);
            }
            return b;
        } finally {
            ff.close(fd);
        }
    }
}
