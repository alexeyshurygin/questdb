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

import org.jetbrains.annotations.NotNull;

/**
 * A WAL segment delta object: the bytes a replica needs to make its copy of one WAL segment hold
 * segment transactions {@code [firstSegmentTxn, lastSegmentTxn]}, as byte ranges of the segment's
 * files at their offsets on the primary. Shipped ranges of a segment follow each other without gaps,
 * so a replica's segment files are always a prefix of the primary's.
 * <pre>
 * int magic 'QRSD', byte version, int walId, int segmentId, int firstSegmentTxn, int lastSegmentTxn,
 * int eventFormatVersion, int fileRangeCount, then fileRangeCount x {str name, long offset, long length, bytes}
 * </pre>
 * The {@code _event} end marker and header are not shipped: the replica writes them itself, the
 * header last, from {@code lastSegmentTxn} and {@code eventFormatVersion}.
 */
public final class SegmentDelta {
    public static final int MAGIC = 0x44535251; // "QRSD" read as a little-endian int
    public static final byte VERSION = 1;
    private static final int RANGE_COUNT_OFFSET = 4 + 1 + 4 * 5;
    private final NativeReader reader = new NativeReader();
    private int eventFormatVersion;
    private long fileLength;
    private String fileName;
    private long fileOffset;
    private long filePtr;
    private int firstSegmentTxn;
    private int lastSegmentTxn;
    private int rangeCount;
    private int rangesLeft;
    private int segmentId;
    private int walId;

    /**
     * Starts an object in {@code out}; follow with {@link #appendRange} calls and {@link #finish}.
     */
    public static void begin(@NotNull NativeBuffer out, int walId, int segmentId, int firstSegmentTxn, int lastSegmentTxn, int eventFormatVersion) {
        out.clear();
        out.putInt(MAGIC);
        out.putByte(VERSION);
        out.putInt(walId);
        out.putInt(segmentId);
        out.putInt(firstSegmentTxn);
        out.putInt(lastSegmentTxn);
        out.putInt(eventFormatVersion);
        out.putInt(0); // range count, patched by finish()
    }

    /**
     * Appends a range header and reserves {@code length} bytes; returns the address to fill them at.
     */
    public static long appendRange(@NotNull NativeBuffer out, @NotNull CharSequence fileName, long offset, long length) {
        out.putStr(fileName);
        out.putLong(offset);
        out.putLong(length);
        final long p = out.ensureCapacity(length);
        out.setSize(out.size() + length);
        // count the range now so that a later failure leaves no reserved-but-uncounted bytes behind
        out.putInt(RANGE_COUNT_OFFSET, getRangeCount(out) + 1);
        return p;
    }

    public static void finish(@NotNull NativeBuffer out) {
        // the range count is kept current by appendRange(); nothing else to patch
        assert out.size() >= RANGE_COUNT_OFFSET + 4;
    }

    private static int getRangeCount(NativeBuffer out) {
        return io.questdb.std.Unsafe.getUnsafe().getInt(out.ptr() + RANGE_COUNT_OFFSET);
    }

    public int getEventFormatVersion() {
        return eventFormatVersion;
    }

    public long getFileLength() {
        return fileLength;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileOffset() {
        return fileOffset;
    }

    /**
     * Address of the current range's bytes, valid while the decoded buffer is.
     */
    public long getFilePtr() {
        return filePtr;
    }

    public int getFirstSegmentTxn() {
        return firstSegmentTxn;
    }

    public int getLastSegmentTxn() {
        return lastSegmentTxn;
    }

    public int getRangeCount() {
        return rangeCount;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public int getWalId() {
        return walId;
    }

    /**
     * Advances to the next file range; false after the last.
     */
    public boolean nextRange() {
        if (rangesLeft == 0) {
            if (reader.hasMore()) {
                throw new NativeReader.CorruptObjectException("trailing bytes in segment delta");
            }
            return false;
        }
        rangesLeft--;
        fileName = reader.readStr();
        if (fileName == null || fileName.isEmpty() || fileName.indexOf('/') >= 0 || fileName.indexOf('\\') >= 0
                || fileName.equals(".") || fileName.equals("..") || fileName.indexOf('\0') >= 0) {
            throw new NativeReader.CorruptObjectException("invalid file name in segment delta");
        }
        fileOffset = reader.readLong();
        fileLength = reader.readLong();
        if (fileOffset < 0) {
            throw new NativeReader.CorruptObjectException("negative offset in segment delta [file=" + fileName + ']');
        }
        filePtr = reader.readBytes(fileLength);
        return true;
    }

    /**
     * Decodes the header of an object held at {@code ptr}; iterate its ranges with {@link #nextRange()}.
     */
    public SegmentDelta of(long ptr, long size) {
        reader.of(ptr, size);
        if (reader.readInt() != MAGIC) {
            throw new NativeReader.CorruptObjectException("not a segment delta");
        }
        final byte version = reader.readByte();
        if (version != VERSION) {
            throw new NativeReader.CorruptObjectException("unsupported segment delta version [version=" + version + ']');
        }
        walId = reader.readInt();
        segmentId = reader.readInt();
        firstSegmentTxn = reader.readInt();
        lastSegmentTxn = reader.readInt();
        eventFormatVersion = reader.readInt();
        rangeCount = reader.readInt();
        if (walId < 1 || segmentId < 0 || firstSegmentTxn < 0 || lastSegmentTxn < firstSegmentTxn || rangeCount < 0) {
            throw new NativeReader.CorruptObjectException("invalid segment delta header [walId=" + walId + ", segmentId=" + segmentId
                    + ", firstSegmentTxn=" + firstSegmentTxn + ", lastSegmentTxn=" + lastSegmentTxn + ", rangeCount=" + rangeCount + ']');
        }
        rangesLeft = rangeCount;
        return this;
    }
}
