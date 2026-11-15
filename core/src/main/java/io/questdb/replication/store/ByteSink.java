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


package io.questdb.replication.store;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Growable heap byte buffer that object store reads land in. Reused across requests to avoid
 * allocating a buffer per download.
 */
public final class ByteSink {
    private byte[] data;
    private int size;

    public ByteSink() {
        this(4096);
    }

    public ByteSink(int initialCapacity) {
        this.data = new byte[Math.max(16, initialCapacity)];
    }

    public void clear() {
        size = 0;
    }

    /**
     * Makes room for {@code n} more bytes and returns the backing array; write at {@link #size()}
     * and then call {@link #advance(int)}.
     */
    public byte @NotNull [] ensureCapacity(int n) {
        final long required = (long) size + n;
        if (required > Integer.MAX_VALUE - 8) {
            throw ObjectStoreException.fatal("object too large to buffer [size=" + required + ']');
        }
        if (required > data.length) {
            data = Arrays.copyOf(data, (int) Math.min(Integer.MAX_VALUE - 8, Math.max(required, (long) data.length * 2)));
        }
        return data;
    }

    public void advance(int n) {
        assert n >= 0 && size + n <= data.length;
        size += n;
    }

    public byte @NotNull [] data() {
        return data;
    }

    public void put(byte @NotNull [] src, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(src, offset, data, size, length);
        size += length;
    }

    public int size() {
        return size;
    }

    public byte @NotNull [] toByteArray() {
        return Arrays.copyOf(data, size);
    }
}
