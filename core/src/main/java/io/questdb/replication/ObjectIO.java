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
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Uploads and downloads replication objects, wrapping each in an {@link ObjectEnvelope}
 * (compression and checksum per the replication settings). One per worker thread.
 */
public class ObjectIO implements QuietCloseable {
    private final int compressionLevel;
    private final ByteSink download = new ByteSink(64 * 1024);
    private final ObjectEnvelope envelope = new ObjectEnvelope();
    private final boolean isChecksum;
    private final ObjectStore store;
    private final ByteSink upload = new ByteSink(64 * 1024);
    private long bytesDownloaded;
    private long bytesUploaded;

    public ObjectIO(@NotNull ObjectStore store, int compressionLevel, boolean isChecksum) {
        this.store = store;
        this.compressionLevel = compressionLevel;
        this.isChecksum = isChecksum;
    }

    public static boolean isChecksum(@NotNull ChecksumMode mode, @NotNull ObjectStore store) {
        return switch (mode) {
            case ALWAYS -> true;
            case NEVER -> false;
            case SERVICE_DEPENDENT -> !store.isIntegrityChecked();
        };
    }

    @Override
    public void close() {
        envelope.close();
    }

    /**
     * Downloads and decodes an object into {@code out}, replacing its content.
     *
     * @return false when the object does not exist
     */
    public boolean get(@NotNull String key, @NotNull NativeBuffer out) {
        try {
            store.get(key, null, download);
        } catch (ObjectStoreException e) {
            if (e.getKind() == ObjectStoreException.Kind.NOT_FOUND) {
                return false;
            }
            throw e;
        }
        bytesDownloaded += download.size();
        out.clear();
        try {
            envelope.decode(download.data(), download.size(), out);
        } catch (ObjectEnvelope.EnvelopeException e) {
            throw ReplicationException.corrupt("corrupt object [key=" + key + ", error=" + e.getMessage() + ']');
        }
        return true;
    }

    public long getBytesDownloaded() {
        return bytesDownloaded;
    }

    public long getBytesUploaded() {
        return bytesUploaded;
    }

    public @NotNull ObjectStore getStore() {
        return store;
    }

    /**
     * Encodes and uploads {@code raw}.
     *
     * @param isIfNoneMatch when true, an existing object is kept and the call returns null; use it
     *                      for immutable objects, whose content is a function of their key
     * @return the new ETag, or null when an immutable object already existed
     */
    public @Nullable String put(@NotNull String key, @NotNull NativeBuffer raw, boolean isIfNoneMatch) {
        envelope.encode(raw.ptr(), raw.size(), compressionLevel, isChecksum, upload);
        try {
            final String etag = store.put(key, upload.data(), 0, upload.size(), null, isIfNoneMatch);
            bytesUploaded += upload.size();
            return etag;
        } catch (ObjectStoreException e) {
            if (isIfNoneMatch && e.getKind() == ObjectStoreException.Kind.PRECONDITION_FAILED) {
                return null;
            }
            throw e;
        }
    }
}
