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

import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * A transaction log part object: the shipped records of transactions
 * {@code [part * partTxnCount + 1, (part + 1) * partTxnCount]} of one table, in order and without
 * gaps. The first part of a table that existed before replication starts at the table's first
 * replicated transaction instead. The primary re-uploads the part that is filling up after each batch.
 * <pre>
 * int magic 'QRTP', byte version, int tableId, long createTimestamp, long part, int partTxnCount,
 * long startTxn, int recordCount, then recordCount records (see {@link TxnRecord#write(NativeBuffer)})
 * </pre>
 */
public final class TxnPart {
    public static final int MAGIC = 0x50545251; // "QRTP" read as a little-endian int
    public static final byte VERSION = 1;
    private final ObjList<TxnRecord> records = new ObjList<>();
    private long createTimestamp;
    private long part;
    private int partTxnCount;
    private long startTxn;
    private int tableId;

    public static @NotNull TxnPart decode(long ptr, long size) {
        final NativeReader r = new NativeReader().of(ptr, size);
        if (r.readInt() != MAGIC) {
            throw new NativeReader.CorruptObjectException("not a transaction log part");
        }
        final byte version = r.readByte();
        if (version != VERSION) {
            throw new NativeReader.CorruptObjectException("unsupported transaction log part version [version=" + version + ']');
        }
        final TxnPart p = new TxnPart();
        p.tableId = r.readInt();
        p.createTimestamp = r.readLong();
        p.part = r.readLong();
        p.partTxnCount = r.readInt();
        p.startTxn = r.readLong();
        final int count = r.readInt();
        final long partFirst = p.part * p.partTxnCount + 1;
        if (p.partTxnCount < 1 || p.part < 0 || count < 0 || p.startTxn < partFirst
                || p.startTxn - partFirst + count > p.partTxnCount) {
            throw new NativeReader.CorruptObjectException("invalid transaction log part header [part=" + p.part
                    + ", partTxnCount=" + p.partTxnCount + ", startTxn=" + p.startTxn + ", count=" + count + ']');
        }
        final long firstTxn = p.startTxn;
        for (int i = 0; i < count; i++) {
            final TxnRecord rec = new TxnRecord();
            rec.read(r);
            if (rec.getTxn() != firstTxn + i) {
                throw new NativeReader.CorruptObjectException("transaction log part out of order [expected=" + (firstTxn + i)
                        + ", actual=" + rec.getTxn() + ']');
            }
            p.records.add(rec);
        }
        if (r.hasMore()) {
            throw new NativeReader.CorruptObjectException("trailing bytes in transaction log part");
        }
        return p;
    }

    public void add(TxnRecord record) {
        final long expected = startTxn + records.size();
        if (record.getTxn() != expected || isFull()) {
            throw new IllegalArgumentException("record does not continue the part [expected=" + expected
                    + ", actual=" + record.getTxn() + ", size=" + records.size() + ']');
        }
        records.add(record);
    }

    public void encode(@NotNull NativeBuffer out) {
        out.clear();
        out.putInt(MAGIC);
        out.putByte(VERSION);
        out.putInt(tableId);
        out.putLong(createTimestamp);
        out.putLong(part);
        out.putInt(partTxnCount);
        out.putLong(startTxn);
        out.putInt(records.size());
        for (int i = 0, n = records.size(); i < n; i++) {
            records.getQuick(i).write(out);
        }
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    /**
     * First transaction the part holds, or would hold when empty.
     */
    public long getFirstTxn() {
        return startTxn;
    }

    /**
     * Last transaction held, or {@code getFirstTxn() - 1} when empty.
     */
    public long getLastTxn() {
        return startTxn + records.size() - 1;
    }

    public long getPart() {
        return part;
    }

    public int getPartTxnCount() {
        return partTxnCount;
    }

    public TxnRecord getRecord(long txn) {
        return records.getQuick((int) (txn - startTxn));
    }

    public ObjList<TxnRecord> getRecords() {
        return records;
    }

    public int getTableId() {
        return tableId;
    }

    public boolean isFull() {
        return getLastTxn() == (part + 1) * partTxnCount;
    }

    public TxnPart of(int tableId, long createTimestamp, long part, int partTxnCount) {
        return of(tableId, createTimestamp, part, partTxnCount, part * partTxnCount + 1);
    }

    /**
     * @param startTxn first transaction of the part, at or after the part's first slot
     */
    public TxnPart of(int tableId, long createTimestamp, long part, int partTxnCount, long startTxn) {
        if (startTxn < part * partTxnCount + 1 || startTxn > (part + 1) * partTxnCount + 1) {
            throw new IllegalArgumentException("start transaction outside the part [part=" + part + ", startTxn=" + startTxn + ']');
        }
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.part = part;
        this.partTxnCount = partTxnCount;
        this.startTxn = startTxn;
        this.records.clear();
        return this;
    }

    /**
     * Drops records after {@code lastTxn}, when an upload of them failed and must be redone.
     */
    public void truncateAfter(long lastTxn) {
        final long keep = Math.max(0, lastTxn - startTxn + 1);
        if (keep < records.size()) {
            records.setPos((int) keep);
        }
    }
}
