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

import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.ObjectEnvelope;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Reads and conditionally replaces the replication index object ({@value #INDEX_KEY}). Remembers
 * the ETag of the last version it read or wrote: a write succeeds only if the store still holds
 * that version, which makes the index the store's single commit point and fences a deposed
 * primary. Not thread-safe.
 */
public class IndexClient implements AutoCloseable {
    public static final String INDEX_KEY = "_index";
    private final boolean isChecksum;
    private final ByteSink download = new ByteSink();
    private final ObjectEnvelope envelope = new ObjectEnvelope();
    private final NativeBuffer raw = new NativeBuffer();
    private final ObjectStore store;
    private final ByteSink upload = new ByteSink();
    private String etag;

    public IndexClient(@NotNull ObjectStore store, boolean isChecksum) {
        this.store = store;
        this.isChecksum = isChecksum;
    }

    @Override
    public void close() {
        envelope.close();
        raw.close();
    }

    /**
     * ETag of the version last read or written, null when the index did not exist then.
     */
    public @Nullable String getEtag() {
        return etag;
    }

    /**
     * Reads the current index, or returns null when the store has none yet.
     */
    public @Nullable ReplicationIndex read() {
        try {
            etag = store.get(INDEX_KEY, null, download);
        } catch (ObjectStoreException e) {
            if (e.getKind() == ObjectStoreException.Kind.NOT_FOUND) {
                etag = null;
                return null;
            }
            throw e;
        }
        return decode();
    }

    /**
     * Reads the index only if it changed since the version last read or written.
     *
     * @return the new index, or null when unchanged
     * @throws ObjectStoreException NOT_FOUND when the index disappeared
     */
    public @Nullable ReplicationIndex readIfChanged() {
        if (etag == null) {
            return read();
        }
        final String newEtag = store.get(INDEX_KEY, etag, download);
        if (newEtag == null) {
            return null;
        }
        etag = newEtag;
        return decode();
    }

    /**
     * Replaces the index, provided the store still holds the version last read or written; creates
     * it when that version was "absent".
     *
     * @throws ObjectStoreException PRECONDITION_FAILED when another writer replaced it meanwhile
     */
    public void write(@NotNull ReplicationIndex index, int compressionLevel) {
        final byte[] text = index.encode();
        raw.clear();
        raw.putBytes(text, 0, text.length);
        envelope.encode(raw.ptr(), raw.size(), compressionLevel, isChecksum, upload);
        etag = store.put(INDEX_KEY, upload.data(), 0, upload.size(), etag, etag == null);
    }

    private ReplicationIndex decode() {
        raw.clear();
        envelope.decode(download.data(), download.size(), raw);
        final byte[] text = new byte[(int) raw.size()];
        Unsafe.copyMemory(null, raw.ptr(), text, Unsafe.BYTE_OFFSET, text.length);
        return ReplicationIndex.decode(text, 0, text.length);
    }
}
