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

import io.questdb.cairo.wal.WalUtils;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * One sequencer transaction as replication ships it: the fields of the primary's transaction log
 * record, the WAL segment delta object that carries a data transaction's bytes, and the serialized
 * change of a structural transaction.
 */
public final class TxnRecord {
    private long commitTimestamp;
    private long deltaEpoch = -1;
    private int deltaFirstSegmentTxn = -1;
    private int deltaLastSegmentTxn = -1;
    private byte @Nullable [] payload;
    private long rowCount;
    private int segmentId;
    private int segmentTxn;
    private long structureVersion;
    private long txn;
    private long txnMaxTimestamp;
    private long txnMinTimestamp;
    private int walId;

    public void copyFrom(TxnRecord other) {
        txn = other.txn;
        walId = other.walId;
        segmentId = other.segmentId;
        segmentTxn = other.segmentTxn;
        structureVersion = other.structureVersion;
        commitTimestamp = other.commitTimestamp;
        txnMinTimestamp = other.txnMinTimestamp;
        txnMaxTimestamp = other.txnMaxTimestamp;
        rowCount = other.rowCount;
        deltaEpoch = other.deltaEpoch;
        deltaFirstSegmentTxn = other.deltaFirstSegmentTxn;
        deltaLastSegmentTxn = other.deltaLastSegmentTxn;
        payload = other.payload;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    /**
     * Epoch of the WAL segment delta object holding this data transaction, -1 for other kinds.
     */
    public long getDeltaEpoch() {
        return deltaEpoch;
    }

    /**
     * First segment transaction of the delta object holding this data transaction, -1 for other kinds.
     */
    public int getDeltaFirstSegmentTxn() {
        return deltaFirstSegmentTxn;
    }

    /**
     * Last segment transaction of the delta object holding this data transaction, -1 for other kinds.
     */
    public int getDeltaLastSegmentTxn() {
        return deltaLastSegmentTxn;
    }

    /**
     * The serialized {@code AlterOperation} of a structural transaction, null for other kinds.
     */
    public byte @Nullable [] getPayload() {
        return payload;
    }

    public long getRowCount() {
        return rowCount;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getSegmentTxn() {
        return segmentTxn;
    }

    public long getStructureVersion() {
        return structureVersion;
    }

    public long getTxn() {
        return txn;
    }

    public long getTxnMaxTimestamp() {
        return txnMaxTimestamp;
    }

    public long getTxnMinTimestamp() {
        return txnMinTimestamp;
    }

    public int getWalId() {
        return walId;
    }

    public boolean isData() {
        return walId > 0;
    }

    public boolean isDrop() {
        return walId == WalUtils.DROP_TABLE_WAL_ID;
    }

    /**
     * True when both records describe the same transaction. A former primary and the object store
     * agree on a transaction only if every field matches.
     */
    public boolean isSameTxn(TxnRecord other) {
        return txn == other.txn
                && walId == other.walId
                && segmentId == other.segmentId
                && segmentTxn == other.segmentTxn
                && structureVersion == other.structureVersion
                && commitTimestamp == other.commitTimestamp
                && Arrays.equals(payload, other.payload);
    }

    public boolean isStructural() {
        return walId == WalUtils.METADATA_WALID;
    }

    public TxnRecord ofData(long txn, int walId, int segmentId, int segmentTxn, long structureVersion, long commitTimestamp,
                            long txnMinTimestamp, long txnMaxTimestamp, long rowCount) {
        of(txn, walId, segmentId, segmentTxn, structureVersion, commitTimestamp);
        this.txnMinTimestamp = txnMinTimestamp;
        this.txnMaxTimestamp = txnMaxTimestamp;
        this.rowCount = rowCount;
        return this;
    }

    public TxnRecord ofDrop(long txn, long structureVersion, long commitTimestamp) {
        return of(txn, WalUtils.DROP_TABLE_WAL_ID, 0, 0, structureVersion, commitTimestamp);
    }

    public TxnRecord ofStructural(long txn, long structureVersion, long commitTimestamp, byte[] payload) {
        of(txn, WalUtils.METADATA_WALID, -1, -1, structureVersion, commitTimestamp);
        this.payload = payload;
        return this;
    }

    public void read(NativeReader r) {
        txn = r.readLong();
        walId = r.readInt();
        segmentId = r.readInt();
        segmentTxn = r.readInt();
        structureVersion = r.readLong();
        commitTimestamp = r.readLong();
        txnMinTimestamp = r.readLong();
        txnMaxTimestamp = r.readLong();
        rowCount = r.readLong();
        deltaEpoch = r.readLong();
        deltaFirstSegmentTxn = r.readInt();
        deltaLastSegmentTxn = r.readInt();
        final int len = r.readInt();
        if (len == -1) {
            payload = null;
        } else {
            final long p = r.readBytes(len);
            payload = new byte[len];
            io.questdb.std.Unsafe.copyMemory(null, p, payload, io.questdb.std.Unsafe.BYTE_OFFSET, len);
        }
    }

    public void setDelta(long epoch, int firstSegmentTxn, int lastSegmentTxn) {
        this.deltaEpoch = epoch;
        this.deltaFirstSegmentTxn = firstSegmentTxn;
        this.deltaLastSegmentTxn = lastSegmentTxn;
    }

    public void setStatistics(long txnMinTimestamp, long txnMaxTimestamp, long rowCount) {
        this.txnMinTimestamp = txnMinTimestamp;
        this.txnMaxTimestamp = txnMaxTimestamp;
        this.rowCount = rowCount;
    }

    @Override
    public String toString() {
        return "TxnRecord{txn=" + txn + ", walId=" + walId + ", segmentId=" + segmentId + ", segmentTxn=" + segmentTxn
                + ", structureVersion=" + structureVersion + ", commitTimestamp=" + commitTimestamp + '}';
    }

    public void write(NativeBuffer b) {
        b.putLong(txn);
        b.putInt(walId);
        b.putInt(segmentId);
        b.putInt(segmentTxn);
        b.putLong(structureVersion);
        b.putLong(commitTimestamp);
        b.putLong(txnMinTimestamp);
        b.putLong(txnMaxTimestamp);
        b.putLong(rowCount);
        b.putLong(deltaEpoch);
        b.putInt(deltaFirstSegmentTxn);
        b.putInt(deltaLastSegmentTxn);
        if (payload == null) {
            b.putInt(-1);
        } else {
            b.putInt(payload.length);
            b.putBytes(payload, 0, payload.length);
        }
    }

    private TxnRecord of(long txn, int walId, int segmentId, int segmentTxn, long structureVersion, long commitTimestamp) {
        this.txn = txn;
        this.walId = walId;
        this.segmentId = segmentId;
        this.segmentTxn = segmentTxn;
        this.structureVersion = structureVersion;
        this.commitTimestamp = commitTimestamp;
        this.txnMinTimestamp = Long.MAX_VALUE;
        this.txnMaxTimestamp = -1;
        this.rowCount = 0;
        this.deltaEpoch = -1;
        this.deltaFirstSegmentTxn = -1;
        this.deltaLastSegmentTxn = -1;
        this.payload = null;
        return this;
    }
}
