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


package io.questdb.replication.codec;

import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;

/**
 * One table's entry in the replication index: its identity on the primary, the last transaction
 * the object store holds for it, and the epochs that wrote its history.
 */
public final class IndexTable {
    public static final int KIND_MAT_VIEW = 2;
    public static final int KIND_TABLE = 0;
    public static final int KIND_VIEW = 1;
    public static final int STATE_ACTIVE = 0;
    public static final int STATE_DROPPED = 1;
    // pairs of (epoch, first txn the epoch wrote), ascending
    private final LongList epochStarts = new LongList();
    private long createTimestamp;
    private String dirName;
    private long droppedAt;
    private long firstTxn = 1;
    private int kind;
    private long lastTxn;
    private int partTxnCount;
    private int state;
    private int tableId;
    private String tableName;

    /**
     * The transaction log part that holds {@code txn} (1-based transaction numbers).
     */
    public static long partOf(long txn, int partTxnCount) {
        return (txn - 1) / partTxnCount;
    }

    public static @NotNull String key(int tableId, long createTimestamp) {
        return tableId + "_" + createTimestamp;
    }

    /**
     * Records that {@code epoch} writes this table's transactions from {@code firstTxn} on. A later
     * call for the same epoch is ignored.
     */
    public void addEpochStart(long epoch, long firstTxn) {
        final int n = epochStarts.size();
        if (n > 0) {
            final long lastEpoch = epochStarts.getQuick(n - 2);
            if (lastEpoch == epoch) {
                return;
            }
            if (epoch < lastEpoch || firstTxn < epochStarts.getQuick(n - 1)) {
                throw new IllegalArgumentException("epoch starts must ascend [epoch=" + epoch + ", firstTxn=" + firstTxn
                        + ", lastEpoch=" + lastEpoch + ", lastFirstTxn=" + epochStarts.getQuick(n - 1) + ']');
            }
        }
        epochStarts.add(epoch, firstTxn);
    }

    public void copyFrom(IndexTable other) {
        tableId = other.tableId;
        createTimestamp = other.createTimestamp;
        dirName = other.dirName;
        tableName = other.tableName;
        kind = other.kind;
        state = other.state;
        lastTxn = other.lastTxn;
        partTxnCount = other.partTxnCount;
        droppedAt = other.droppedAt;
        firstTxn = other.firstTxn;
        epochStarts.clear();
        epochStarts.add(other.epochStarts);
    }

    /**
     * The epoch whose upload of transaction log part {@code part} is authoritative: the newest epoch
     * that wrote any transaction up to the end of the part. A new epoch re-uploads the part it starts
     * in, in full, so its copy supersedes the older epochs' copies. Part {@code p} holds transactions
     * {@code [p * partTxnCount + 1, (p + 1) * partTxnCount]}; -1 means no epoch wrote that far.
     */
    public long getAuthoritativeEpoch(long part) {
        final long partLastTxn = (part + 1) * partTxnCount;
        long result = -1;
        for (int i = 0, n = epochStarts.size(); i < n; i += 2) {
            if (epochStarts.getQuick(i + 1) <= partLastTxn) {
                result = epochStarts.getQuick(i);
            }
        }
        return result;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public String getDirName() {
        return dirName;
    }

    public long getDroppedAt() {
        return droppedAt;
    }

    /**
     * First transaction the object store holds for the table: 1 for a table created while
     * replication was on, the next transaction for a table that existed before, and higher once the
     * cleaner deleted old parts. A replica must already hold every transaction before it.
     */
    public long getFirstTxn() {
        return firstTxn;
    }

    public LongList getEpochStarts() {
        return epochStarts;
    }

    public @NotNull String getKey() {
        return key(tableId, createTimestamp);
    }

    public int getKind() {
        return kind;
    }

    public long getLastTxn() {
        return lastTxn;
    }

    public int getPartTxnCount() {
        return partTxnCount;
    }

    public int getState() {
        return state;
    }

    public int getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isDropped() {
        return state == STATE_DROPPED;
    }

    public IndexTable of(int tableId, long createTimestamp, String dirName, String tableName, int kind, int partTxnCount) {
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.dirName = dirName;
        this.tableName = tableName;
        this.kind = kind;
        this.partTxnCount = partTxnCount;
        this.state = STATE_ACTIVE;
        this.lastTxn = 0;
        this.droppedAt = 0;
        this.firstTxn = 1;
        this.epochStarts.clear();
        return this;
    }

    public void setDropped(long droppedAt) {
        this.state = STATE_DROPPED;
        this.droppedAt = droppedAt;
    }

    public void setFirstTxn(long firstTxn) {
        this.firstTxn = firstTxn;
    }

    public void setLastTxn(long lastTxn) {
        this.lastTxn = lastTxn;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
