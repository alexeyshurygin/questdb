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

import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;

/**
 * Growable native buffer for replication payloads, tagged {@link MemoryTag#NATIVE_REPL}. Not
 * thread-safe; each worker owns its buffers.
 */
public final class NativeBuffer implements QuietCloseable {
    private long capacity;
    private long ptr;
    private long size;

    public void clear() {
        size = 0;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, capacity, MemoryTag.NATIVE_REPL);
            ptr = 0;
            capacity = 0;
        }
        size = 0;
    }

    /**
     * Makes room for {@code n} more bytes past {@link #size()} and returns the address to write at.
     */
    public long ensureCapacity(long n) {
        final long required = size + n;
        if (required > capacity) {
            final long newCapacity = Math.max(required, Math.max(4096, capacity * 2));
            ptr = ptr == 0
                    ? Unsafe.malloc(newCapacity, MemoryTag.NATIVE_REPL)
                    : Unsafe.realloc(ptr, capacity, newCapacity, MemoryTag.NATIVE_REPL);
            capacity = newCapacity;
        }
        return ptr + size;
    }

    public long getCapacity() {
        return capacity;
    }

    public long ptr() {
        return ptr;
    }

    public void putByte(byte value) {
        Unsafe.getUnsafe().putByte(ensureCapacity(1), value);
        size++;
    }

    /**
     * Copies {@code length} bytes from native memory at {@code src}.
     */
    public void putBytes(long src, long length) {
        final long dst = ensureCapacity(length);
        Unsafe.copyMemory(src, dst, length);
        size += length;
    }

    public void putInt(int value) {
        Unsafe.getUnsafe().putInt(ensureCapacity(4), value);
        size += 4;
    }

    public void putInt(long offset, int value) {
        assert offset >= 0 && offset + 4 <= size;
        Unsafe.getUnsafe().putInt(ptr + offset, value);
    }

    public void putLong(long value) {
        Unsafe.getUnsafe().putLong(ensureCapacity(8), value);
        size += 8;
    }

    public void putShort(short value) {
        Unsafe.getUnsafe().putShort(ensureCapacity(2), value);
        size += 2;
    }

    /**
     * Writes a string as an int byte length followed by its UTF-8 bytes; null as length -1.
     */
    public void putStr(CharSequence value) {
        if (value == null) {
            putInt(-1);
            return;
        }
        final byte[] bytes = value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        putInt(bytes.length);
        putBytes(bytes, 0, bytes.length);
    }

    public void putBytes(byte[] src, int offset, int length) {
        final long dst = ensureCapacity(length);
        Unsafe.copyMemory(src, Unsafe.BYTE_OFFSET + offset, null, dst, length);
        size += length;
    }

    public void setSize(long size) {
        assert size >= 0 && size <= capacity;
        this.size = size;
    }

    public long size() {
        return size;
    }
}
