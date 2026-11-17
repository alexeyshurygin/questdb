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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.codec.CheckpointHistory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.TxnPart;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.replication.store.ObjectStore;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * The primary's WAL cleaner: works out which objects no node can need any more and which index
 * changes must be committed before they go. A table's objects up to a cutoff go once every
 * instance that recorded at least {@code replication.primary.cleaner.backup.window.count} checkpoints
 * would, restored from any of its newest that many, resume past the cutoff. A dropped table's
 * objects go after {@code replication.primary.cleaner.dropped.table.cooloff}. Parts are deleted
 * whole; the table's first transaction in the index moves past them first, so a replica never reads
 * a part that is about to disappear.
 */
public class WalCleaner {
    private static final Log LOG = LogFactory.getLog(WalCleaner.class);
    private final ReplicationConfiguration config;
    private final ObjectStore store;

    public WalCleaner(@NotNull ReplicationConfiguration config, @NotNull ObjectStore store) {
        this.config = config;
        this.store = store;
    }

    /**
     * Per table directory, the last transaction no kept checkpoint needs from the store; null when no
     * instance kept enough checkpoints yet. A table missing from a kept checkpoint needs all of its
     * history, so its cutoff is 0.
     */
    public @Nullable Map<String, Long> computeCutoffs(@NotNull ReplicationWorker worker) {
        final List<String> keys = new ArrayList<>();
        store.list(StoreKeys.CHECKPOINTS_PREFIX, null, (key, size) -> {
            keys.add(key);
            return true;
        });
        final int window = config.getCleanerBackupWindowCount();
        final List<List<CheckpointHistory.Checkpoint>> windows = new ArrayList<>();
        final NativeBuffer buf = worker.getBuffer();
        for (String key : keys) {
            if (!worker.getIo().get(key, buf)) {
                continue;
            }
            final byte[] text = new byte[(int) buf.size()];
            Unsafe.copyMemory(null, buf.ptr(), text, Unsafe.BYTE_OFFSET, text.length);
            final CheckpointHistory history;
            try {
                history = CheckpointHistory.decode(text, 0, text.length);
            } catch (RuntimeException e) {
                LOG.error().$("ignoring an unreadable checkpoint history [key=").$(key).$(", error=").$safe(e.getMessage()).I$();
                continue;
            }
            final List<CheckpointHistory.Checkpoint> all = history.getCheckpoints();
            if (all.size() >= window) {
                windows.add(all.subList(all.size() - window, all.size()));
            }
        }
        if (windows.isEmpty()) {
            return null;
        }
        final Map<String, Long> cutoffs = new HashMap<>();
        // a table missing from any kept checkpoint gets 0; collect every dir named anywhere first
        final HashSet<String> dirs = new HashSet<>();
        for (List<CheckpointHistory.Checkpoint> w : windows) {
            for (CheckpointHistory.Checkpoint c : w) {
                dirs.addAll(c.getTables().keySet());
            }
        }
        for (String dir : dirs) {
            long cutoff = Long.MAX_VALUE;
            for (List<CheckpointHistory.Checkpoint> w : windows) {
                for (CheckpointHistory.Checkpoint c : w) {
                    cutoff = Math.min(cutoff, Math.max(0, c.getSeqTxn(dir)));
                }
            }
            cutoffs.put(dir, cutoff);
        }
        return cutoffs;
    }

    /**
     * Plans the deletion of a table's parts that end at or before {@code cutoff}.
     *
     * @return the keys to delete and the table's new first transaction, or null when nothing goes
     */
    public @Nullable TablePlan planTable(@NotNull IndexTable entry, long cutoff, @NotNull ReplicationWorker worker) {
        final int p = entry.getPartTxnCount();
        final long lastDeletable = Math.min(cutoff, entry.getLastTxn()) / p - 1;
        final long firstPart = IndexTable.partOf(entry.getFirstTxn(), p);
        if (lastDeletable < firstPart || entry.getLastTxn() < entry.getFirstTxn()) {
            return null;
        }
        final String tableKey = entry.getKey();
        final HashSet<String> deltas = new HashSet<>();
        for (long part = firstPart; part <= lastDeletable; part++) {
            collectDeltas(entry, part, worker, deltas);
        }
        // a delta can also serve the first transactions of the next part
        final long nextPart = lastDeletable + 1;
        if (nextPart * p + 1 <= entry.getLastTxn()) {
            final HashSet<String> kept = new HashSet<>();
            collectDeltas(entry, nextPart, worker, kept);
            deltas.removeAll(kept);
        }
        final TablePlan plan = new TablePlan((lastDeletable + 1) * p + 1);
        final String seqPrefix = StoreKeys.tablePrefix(tableKey) + "seq/";
        store.list(seqPrefix, null, (key, size) -> {
            final long part = parsePart(key.substring(seqPrefix.length()));
            if (part >= 0 && part <= lastDeletable) {
                plan.keys.add(key);
            }
            return true;
        });
        plan.keys.addAll(deltas);
        return plan;
    }

    /**
     * Every object of a dropped table.
     */
    public List<String> planDroppedTable(@NotNull IndexTable entry) {
        final List<String> keys = new ArrayList<>();
        store.list(StoreKeys.tablePrefix(entry.getKey()), null, (key, size) -> {
            keys.add(key);
            return true;
        });
        return keys;
    }

    private static long parsePart(String name) {
        final int dot = name.indexOf('.');
        if (dot < 1) {
            return -1;
        }
        try {
            return Long.parseLong(name.substring(0, dot));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void collectDeltas(IndexTable entry, long part, ReplicationWorker worker, HashSet<String> out) {
        final long epoch = entry.getAuthoritativeEpoch(part);
        final String key = StoreKeys.seqPart(entry.getKey(), part, epoch);
        final NativeBuffer buf = worker.getPartBuffer();
        if (epoch < 0 || !worker.getIo().get(key, buf)) {
            return;
        }
        final TxnPart txnPart = TxnPart.decode(buf.ptr(), buf.size());
        for (int i = 0, n = txnPart.getRecords().size(); i < n; i++) {
            final TxnRecord r = txnPart.getRecords().getQuick(i);
            if (r.isData() && r.getDeltaEpoch() >= 0) {
                out.add(StoreKeys.segmentDelta(entry.getKey(), r.getWalId(), r.getSegmentId(), r.getDeltaFirstSegmentTxn(),
                        r.getDeltaLastSegmentTxn(), r.getDeltaEpoch()));
            }
        }
    }

    public static final class TablePlan {
        private final List<String> keys = new ArrayList<>();
        private final long newFirstTxn;

        TablePlan(long newFirstTxn) {
            this.newFirstTxn = newFirstTxn;
        }

        public List<String> getKeys() {
            return keys;
        }

        public long getNewFirstTxn() {
            return newFirstTxn;
        }
    }
}
