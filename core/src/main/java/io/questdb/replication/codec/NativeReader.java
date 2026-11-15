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

import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Bounds-checked sequential reader over native memory, the counterpart of {@link NativeBuffer}'s
 * put methods. Every read past the end throws {@link CorruptObjectException}, so a truncated or
 * damaged object can never make a reader touch memory outside it.
 */
public final class NativeReader {
    private long hi;
    private long lo;
    private long pos;

    public long getByteCount() {
        return hi - lo;
    }

    public long getPosition() {
        return pos - lo;
    }

    public boolean hasMore() {
        return pos < hi;
    }

    public NativeReader of(long ptr, long size) {
        this.lo = ptr;
        this.pos = ptr;
        this.hi = ptr + size;
        return this;
    }

    public byte readByte() {
        need(1);
        return Unsafe.getUnsafe().getByte(pos++);
    }

    /**
     * Skips {@code length} bytes and returns their address.
     */
    public long readBytes(long length) {
        if (length < 0) {
            throw new CorruptObjectException("negative length [length=" + length + ", position=" + getPosition() + ']');
        }
        need(length);
        final long p = pos;
        pos += length;
        return p;
    }

    public int readInt() {
        need(4);
        final int v = Unsafe.getUnsafe().getInt(pos);
        pos += 4;
        return v;
    }

    public long readLong() {
        need(8);
        final long v = Unsafe.getUnsafe().getLong(pos);
        pos += 8;
        return v;
    }

    public short readShort() {
        need(2);
        final short v = Unsafe.getUnsafe().getShort(pos);
        pos += 2;
        return v;
    }

    /**
     * Reads a string written by {@link NativeBuffer#putStr(CharSequence)}.
     */
    public String readStr() {
        final int len = readInt();
        if (len == -1) {
            return null;
        }
        final long p = readBytes(len);
        final byte[] bytes = new byte[len];
        Unsafe.copyMemory(null, p, bytes, Unsafe.BYTE_OFFSET, len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void need(long n) {
        if (n > hi - pos) {
            throw new CorruptObjectException("object truncated [position=" + getPosition() + ", need=" + n + ", size=" + getByteCount() + ']');
        }
    }

    public static class CorruptObjectException extends RuntimeException {
        public CorruptObjectException(String message) {
            super(message, null, false, false);
        }
    }
}
