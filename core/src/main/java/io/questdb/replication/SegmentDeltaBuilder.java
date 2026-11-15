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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypeDriver;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Builds the {@link SegmentDelta} for a range of committed transactions of one WAL segment on the
 * primary, reading the segment files with pread at extents derived from the committed event records,
 * never from file lengths: an open segment's files are page-extended and hold uncommitted bytes past
 * the committed ones. The bytes of committed transactions never change, so reading them while the
 * WAL writer appends is safe once the transactions are known committed through the sequencer.
 * Not thread-safe.
 */
public class SegmentDeltaBuilder implements QuietCloseable {
    // per segment txn: segmentTxn, type, rowCount, minTimestamp, maxTimestamp
    public static final int STATS_STRIDE = 5;
    private static final int SCRATCH_SIZE = 64;
    private final FilesFacade ff;
    private final Path path = new Path();
    private final LongList stats = new LongList();
    private long scratch;

    public SegmentDeltaBuilder(@NotNull FilesFacade ff) {
        this.ff = ff;
        this.scratch = Unsafe.malloc(SCRATCH_SIZE, MemoryTag.NATIVE_REPL);
    }

    @Override
    public void close() {
        if (scratch != 0) {
            Unsafe.free(scratch, SCRATCH_SIZE, MemoryTag.NATIVE_REPL);
            scratch = 0;
        }
        path.close();
    }

    /**
     * Builds into {@code out} the delta that brings a replica's copy of segment
     * {@code <dbRoot>/<tableDir>/wal<walId>/<segmentId>} to hold segment transactions
     * {@code [lo, hi]}; the replica must already hold {@code [0, lo)}.
     */
    public void build(
            @NotNull CharSequence dbRoot,
            @NotNull CharSequence tableDir,
            int walId,
            int segmentId,
            int lo,
            int hi,
            @NotNull NativeBuffer out
    ) {
        if (lo < 0 || hi < lo) {
            throw CairoException.critical(0).put("invalid segment txn range [lo=").put(lo).put(", hi=").put(hi).put(']');
        }
        stats.clear();
        path.of(dbRoot).concat(tableDir).concat(WalUtils.WAL_NAME_BASE).put(walId).slash().put(segmentId);
        final int segLen = path.size();
        long eventFd = -1;
        long indexFd = -1;
        try {
            eventFd = openRO(path.trimTo(segLen).concat(WalUtils.EVENT_FILE_NAME).$());
            indexFd = openRO(path.trimTo(segLen).concat(WalUtils.EVENT_INDEX_FILE_NAME).$());
            final long evLo = readLong(indexFd, 8L * lo, WalUtils.EVENT_INDEX_FILE_NAME);
            final long evHi = readLong(indexFd, 8L * (hi + 1), WalUtils.EVENT_INDEX_FILE_NAME);
            long rowLo = Long.MAX_VALUE;
            long rowHi = Long.MIN_VALUE;
            long prev = evLo;
            for (int k = lo; k <= hi; k++) {
                final long r = prev;
                final long next = readLong(indexFd, 8L * (k + 1), WalUtils.EVENT_INDEX_FILE_NAME);
                read(eventFd, r, 13, WalUtils.EVENT_FILE_NAME);
                final int len = Unsafe.getUnsafe().getInt(scratch);
                final long segTxn = Unsafe.getUnsafe().getLong(scratch + 4);
                final byte type = Unsafe.getUnsafe().getByte(scratch + 12);
                if (len != next - r || len < 13 || segTxn != k) {
                    throw CairoException.critical(0).put("WAL event record does not match its index [path=").put(path.trimTo(segLen))
                            .put(", segmentTxn=").put(k).put(", length=").put(len).put(", indexLength=").put(next - r)
                            .put(", recordTxn=").put(segTxn).put(']');
                }
                long rows = 0;
                long minTs = Long.MAX_VALUE;
                long maxTs = -1;
                if (WalTxnType.isDataType(type)) {
                    read(eventFd, r + 13, 32, WalUtils.EVENT_FILE_NAME);
                    final long startRow = Unsafe.getUnsafe().getLong(scratch);
                    final long endRow = Unsafe.getUnsafe().getLong(scratch + 8);
                    minTs = Unsafe.getUnsafe().getLong(scratch + 16);
                    maxTs = Unsafe.getUnsafe().getLong(scratch + 24);
                    if (startRow < 0 || endRow < startRow) {
                        throw CairoException.critical(0).put("invalid WAL row range [path=").put(path.trimTo(segLen))
                                .put(", segmentTxn=").put(k).put(", startRowID=").put(startRow).put(", endRowID=").put(endRow).put(']');
                    }
                    rows = endRow - startRow;
                    rowLo = Math.min(rowLo, startRow);
                    rowHi = Math.max(rowHi, endRow);
                }
                stats.add((long) k, type);
                stats.add(rows);
                stats.add(minTs, maxTs);
                prev = next;
            }
            final int formatVersion = readInt(eventFd, WalUtils.WAL_FORMAT_OFFSET_32, WalUtils.EVENT_FILE_NAME);

            SegmentDelta.begin(out, walId, segmentId, lo, hi, formatVersion);
            copyRange(indexFd, WalUtils.EVENT_INDEX_FILE_NAME, 8L * lo, 8L * (hi + 2) - 8L * lo, out);
            copyRange(eventFd, WalUtils.EVENT_FILE_NAME, evLo, evHi - evLo, out);

            path.trimTo(segLen).concat(TableUtils.META_FILE_NAME).$();
            if (ff.exists(path.$())) {
                copyMetaAndColumns(segLen, rowLo, rowHi, out);
            }
            SegmentDelta.finish(out);
        } finally {
            ff.close(eventFd);
            ff.close(indexFd);
            path.trimTo(segLen);
        }
    }

    /**
     * Per segment transaction of the last {@link #build}: {@link #STATS_STRIDE} longs of segment txn,
     * type, row count, min timestamp and max timestamp.
     */
    public LongList getStats() {
        return stats;
    }

    private void copyColumnRange(int segLen, String fileName, long offset, long length, NativeBuffer out) {
        final long fd = openRO(path.trimTo(segLen).concat(fileName).$());
        try {
            copyRange(fd, fileName, offset, length, out);
        } finally {
            ff.close(fd);
        }
    }

    private void copyMetaAndColumns(int segLen, long rowLo, long rowHi, NativeBuffer out) {
        final long metaFd = openRO(path.trimTo(segLen).concat(TableUtils.META_FILE_NAME).$());
        final long metaStart;
        final int metaSize;
        try {
            metaSize = readInt(metaFd, WalUtils.SEQ_META_OFFSET_WAL_LENGTH, TableUtils.META_FILE_NAME);
            if (metaSize < (int) WalUtils.SEQ_META_OFFSET_COLUMNS) {
                throw CairoException.critical(0).put("invalid WAL segment metadata size [path=").put(path.trimTo(segLen))
                        .put(", size=").put(metaSize).put(']');
            }
            final long p = copyRange(metaFd, TableUtils.META_FILE_NAME, 0, metaSize, out);
            metaStart = p - out.ptr();
        } finally {
            ff.close(metaFd);
        }
        // parse the copy: the buffer may move as ranges are appended, so address it by offset
        final int columnCount = Unsafe.getUnsafe().getInt(out.ptr() + metaStart + WalUtils.SEQ_META_OFFSET_COLUMN_COUNT);
        final int timestampIndex = Unsafe.getUnsafe().getInt(out.ptr() + metaStart + WalUtils.SEQ_META_OFFSET_TIMESTAMP_INDEX);
        final boolean hasRows = rowHi > rowLo;
        long pos = WalUtils.SEQ_META_OFFSET_COLUMNS;
        for (int i = 0; i < columnCount; i++) {
            if (pos + 8 > metaSize) {
                throw CairoException.critical(0).put("truncated WAL segment metadata [path=").put(path.trimTo(segLen)).put(']');
            }
            final int type = Unsafe.getUnsafe().getInt(out.ptr() + metaStart + pos);
            final int nameLen = Unsafe.getUnsafe().getInt(out.ptr() + metaStart + pos + 4);
            if (nameLen < 0 || pos + 8 + 2L * nameLen > metaSize) {
                throw CairoException.critical(0).put("invalid WAL segment column name [path=").put(path.trimTo(segLen))
                        .put(", column=").put(i).put(']');
            }
            final char[] chars = new char[nameLen];
            for (int c = 0; c < nameLen; c++) {
                chars[c] = Unsafe.getUnsafe().getChar(out.ptr() + metaStart + pos + 8 + 2L * c);
            }
            pos += 8 + 2L * nameLen;
            if (type <= 0) {
                continue;
            }
            final String name = new String(chars);
            final String dFile = name + ".d";
            if (i == timestampIndex) {
                copyColumnRange(segLen, dFile, hasRows ? 16 * rowLo : 0, hasRows ? 16 * (rowHi - rowLo) : 0, out);
            } else if (!ColumnType.isVarSize(type)) {
                final int shl = ColumnType.pow2SizeOf(type);
                copyColumnRange(segLen, dFile, hasRows ? rowLo << shl : 0, hasRows ? (rowHi - rowLo) << shl : 0, out);
            } else {
                final ColumnTypeDriver driver = ColumnType.getDriver(type);
                final String iFile = name + ".i";
                final long auxFd = openRO(path.trimTo(segLen).concat(iFile).$());
                try {
                    if (hasRows) {
                        final long auxLo = driver.getAuxVectorOffset(rowLo);
                        final long auxHi = driver.getAuxVectorSize(rowHi);
                        copyRange(auxFd, iFile, auxLo, auxHi - auxLo, out);
                        final long dataLo = driver.getDataVectorSizeAtFromFd(ff, auxFd, rowLo - 1);
                        final long dataHi = driver.getDataVectorSizeAtFromFd(ff, auxFd, rowHi - 1);
                        if (dataLo < 0 || dataHi < dataLo) {
                            throw CairoException.critical(0).put("invalid WAL var-size column extent [path=").put(path.trimTo(segLen))
                                    .put(", column=").put(name).put(", dataLo=").put(dataLo).put(", dataHi=").put(dataHi).put(']');
                        }
                        copyColumnRange(segLen, dFile, dataLo, dataHi - dataLo, out);
                    } else {
                        // the apply opens every live column's files by name, even for an empty row range
                        copyRange(auxFd, iFile, 0, 0, out);
                        copyColumnRange(segLen, dFile, 0, 0, out);
                    }
                } finally {
                    ff.close(auxFd);
                }
            }
        }
    }

    // appends a range read from fd and returns the address of its bytes in out
    private long copyRange(long fd, String fileName, long offset, long length, NativeBuffer out) {
        final long p = SegmentDelta.appendRange(out, fileName, offset, length);
        if (length > 0) {
            final long n = ff.read(fd, p, length, offset);
            if (n != length) {
                throw CairoException.critical(ff.errno()).put("could not read WAL file [path=").put(path).put(", file=").put(fileName)
                        .put(", offset=").put(offset).put(", length=").put(length).put(", read=").put(n).put(']');
            }
        }
        return p;
    }

    private long openRO(LPSZ p) {
        final long fd = ff.openRO(p);
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not open WAL file [path=").put(p).put(']');
        }
        return fd;
    }

    private void read(long fd, long offset, int length, String fileName) {
        assert length <= SCRATCH_SIZE;
        final long n = ff.read(fd, scratch, length, offset);
        if (n != length) {
            throw CairoException.critical(ff.errno()).put("could not read WAL file [path=").put(path).put(", file=").put(fileName)
                    .put(", offset=").put(offset).put(", length=").put(length).put(", read=").put(n).put(']');
        }
    }

    private int readInt(long fd, long offset, String fileName) {
        read(fd, offset, 4, fileName);
        return Unsafe.getUnsafe().getInt(scratch);
    }

    private long readLong(long fd, long offset, String fileName) {
        read(fd, offset, 8, fileName);
        return Unsafe.getUnsafe().getLong(scratch);
    }
}
