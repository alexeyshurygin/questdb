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
import io.questdb.cairo.wal.WalUtils;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Reads a table's committed sequencer records straight from its {@code txn_seq} files with pread:
 * V1 records in {@code _txnlog}, V2 records in {@code _txn_parts/<part>}, and structural changes in
 * {@code _txnlog.meta.i/.d}. Records up to the committed max transaction never change, and the
 * caller learns that max through the sequencer, so the reads see whole records. Unlike a sequencer
 * cursor it keeps working after the table is dropped, which the upload of a drop needs.
 * Not thread-safe.
 */
public class TxnLogReader implements QuietCloseable {
    private static final int HEADER_PART_SIZE_OFFSET = 20;
    private static final int HEADER_SIZE = 76;
    private static final int MAX_TXN_OFFSET = 4;
    private static final int SCRATCH_SIZE = 64;
    private static final int V1_RECORD_SIZE = 28;
    private static final int V2_RECORD_SIZE = 60;
    private final FilesFacade ff;
    private final Path path = new Path();
    private int formatVersion;
    private long logFd = -1;
    private long metaDataFd = -1;
    private long metaIndexFd = -1;
    private long partFd = -1;
    private long partId = -1;
    private int partSize;
    private long scratch;
    private int seqLen;

    public TxnLogReader(@NotNull FilesFacade ff) {
        this.ff = ff;
        this.scratch = Unsafe.malloc(SCRATCH_SIZE, MemoryTag.NATIVE_REPL);
    }

    @Override
    public void close() {
        closeFiles();
        if (scratch != 0) {
            Unsafe.free(scratch, SCRATCH_SIZE, MemoryTag.NATIVE_REPL);
            scratch = 0;
        }
        path.close();
    }

    public void closeFiles() {
        logFd = closeFd(logFd);
        partFd = closeFd(partFd);
        metaIndexFd = closeFd(metaIndexFd);
        metaDataFd = closeFd(metaDataFd);
        partId = -1;
    }

    /**
     * Transactions per V2 part file; 0 for V1 logs.
     */
    public int getPartSize() {
        return formatVersion == WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2 ? partSize : 0;
    }

    /**
     * The committed max transaction in the log header.
     */
    public long getMaxTxn() {
        read(logFd, MAX_TXN_OFFSET, 8, WalUtils.TXNLOG_FILE_NAME);
        return Unsafe.getUnsafe().getLong(scratch);
    }

    /**
     * Opens the sequencer files of {@code <dbRoot>/<tableDir>}.
     */
    public TxnLogReader of(@NotNull CharSequence dbRoot, @NotNull CharSequence tableDir) {
        closeFiles();
        path.of(dbRoot).concat(tableDir).concat(WalUtils.SEQ_DIR);
        seqLen = path.size();
        logFd = openRO(WalUtils.TXNLOG_FILE_NAME);
        read(logFd, 0, 4, WalUtils.TXNLOG_FILE_NAME);
        formatVersion = Unsafe.getUnsafe().getInt(scratch);
        if (formatVersion == WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V2) {
            read(logFd, HEADER_PART_SIZE_OFFSET, 4, WalUtils.TXNLOG_FILE_NAME);
            partSize = Unsafe.getUnsafe().getInt(scratch);
            if (partSize < 1) {
                throw CairoException.critical(0).put("invalid sequencer part size [path=").put(path.trimTo(seqLen))
                        .put(", partSize=").put(partSize).put(']');
            }
        } else if (formatVersion != WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1) {
            throw CairoException.critical(0).put("unsupported sequencer format [path=").put(path.trimTo(seqLen))
                    .put(", version=").put(formatVersion).put(']');
        }
        return this;
    }

    /**
     * Reads committed transaction {@code txn} (1-based) into {@code out}, including the change of a
     * structural transaction. V1 logs keep no statistics: data records then carry
     * {@code Long.MAX_VALUE, -1, 0} until the caller fills them from the WAL events.
     */
    public void readRecord(long txn, @NotNull TxnRecord out) {
        if (txn < 1) {
            throw CairoException.critical(0).put("invalid transaction [txn=").put(txn).put(']');
        }
        final int recordSize;
        if (formatVersion == WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1) {
            recordSize = V1_RECORD_SIZE;
            read(logFd, HEADER_SIZE + (txn - 1) * V1_RECORD_SIZE, V1_RECORD_SIZE, WalUtils.TXNLOG_FILE_NAME);
        } else {
            recordSize = V2_RECORD_SIZE;
            final long part = (txn - 1) / partSize;
            if (part != partId) {
                partFd = closeFd(partFd);
                path.trimTo(seqLen).concat(WalUtils.TXNLOG_PARTS_DIR).slash().put(part);
                partFd = ff.openRO(path.$());
                if (partFd < 0) {
                    throw CairoException.critical(ff.errno()).put("could not open sequencer part [path=").put(path).put(']');
                }
                partId = part;
                path.trimTo(seqLen);
            }
            read(partFd, ((txn - 1) % partSize) * V2_RECORD_SIZE, V2_RECORD_SIZE, WalUtils.TXNLOG_PARTS_DIR);
        }
        final long structureVersion = Unsafe.getUnsafe().getLong(scratch);
        final int walId = Unsafe.getUnsafe().getInt(scratch + 8);
        final int segmentId = Unsafe.getUnsafe().getInt(scratch + 12);
        final int segmentTxn = Unsafe.getUnsafe().getInt(scratch + 16);
        final long commitTimestamp = Unsafe.getUnsafe().getLong(scratch + 20);
        if (walId > 0) {
            if (recordSize == V2_RECORD_SIZE) {
                out.ofData(txn, walId, segmentId, segmentTxn, structureVersion, commitTimestamp,
                        Unsafe.getUnsafe().getLong(scratch + 28), Unsafe.getUnsafe().getLong(scratch + 36), Unsafe.getUnsafe().getLong(scratch + 44));
            } else {
                out.ofData(txn, walId, segmentId, segmentTxn, structureVersion, commitTimestamp, Long.MAX_VALUE, -1, 0);
            }
            if (segmentTxn < 0 || segmentId < 0) {
                throw CairoException.critical(0).put("invalid WAL coordinates in sequencer record [path=").put(path.trimTo(seqLen))
                        .put(", txn=").put(txn).put(", segmentId=").put(segmentId).put(", segmentTxn=").put(segmentTxn).put(']');
            }
        } else if (walId == WalUtils.METADATA_WALID) {
            out.ofStructural(txn, structureVersion, commitTimestamp, readChange(structureVersion));
        } else if (walId == WalUtils.DROP_TABLE_WAL_ID) {
            out.ofDrop(txn, structureVersion, commitTimestamp);
        } else {
            throw CairoException.critical(0).put("invalid sequencer record [path=").put(path.trimTo(seqLen))
                    .put(", txn=").put(txn).put(", walId=").put(walId).put(']');
        }
    }

    private long closeFd(long fd) {
        if (fd > -1) {
            ff.close(fd);
        }
        return -1;
    }

    private long openRO(String fileName) {
        final long fd = ff.openRO(path.trimTo(seqLen).concat(fileName).$());
        path.trimTo(seqLen);
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not open sequencer file [path=").put(path).put(", file=").put(fileName).put(']');
        }
        return fd;
    }

    private void read(long fd, long offset, int length, String fileName) {
        assert length <= SCRATCH_SIZE;
        final long n = ff.read(fd, scratch, length, offset);
        if (n != length) {
            throw CairoException.critical(ff.errno()).put("could not read sequencer file [path=").put(path.trimTo(seqLen))
                    .put(", file=").put(fileName).put(", offset=").put(offset).put(", length=").put(length).put(", read=").put(n).put(']');
        }
    }

    // the change that produced structure version v: .meta.d [index[v-1], ...) holds int len + payload
    private byte[] readChange(long structureVersion) {
        if (structureVersion < 1) {
            throw CairoException.critical(0).put("invalid structure version in structural record [path=").put(path.trimTo(seqLen))
                    .put(", version=").put(structureVersion).put(']');
        }
        if (metaIndexFd < 0) {
            metaIndexFd = openRO(WalUtils.TXNLOG_FILE_NAME_META_INX);
            metaDataFd = openRO(WalUtils.TXNLOG_FILE_NAME_META_VAR);
        }
        read(metaIndexFd, (structureVersion - 1) * 8, 8, WalUtils.TXNLOG_FILE_NAME_META_INX);
        final long start = Unsafe.getUnsafe().getLong(scratch);
        read(metaDataFd, start, 4, WalUtils.TXNLOG_FILE_NAME_META_VAR);
        final int len = Unsafe.getUnsafe().getInt(scratch);
        if (len < 10 || len > 16 * 1024 * 1024) {
            throw CairoException.critical(0).put("invalid structural change length [path=").put(path.trimTo(seqLen))
                    .put(", version=").put(structureVersion).put(", length=").put(len).put(']');
        }
        final byte[] payload = new byte[len];
        final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_REPL);
        try {
            final long n = ff.read(metaDataFd, buf, len, start + 4);
            if (n != len) {
                throw CairoException.critical(ff.errno()).put("could not read structural change [path=").put(path.trimTo(seqLen))
                        .put(", version=").put(structureVersion).put(']');
            }
            Unsafe.copyMemory(null, buf, payload, Unsafe.BYTE_OFFSET, len);
        } finally {
            Unsafe.free(buf, len, MemoryTag.NATIVE_REPL);
        }
        return payload;
    }
}
