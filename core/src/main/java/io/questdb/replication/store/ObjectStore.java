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

import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Minimal object store used by replication: whole-object put/get with conditional requests,
 * listing and delete. Keys are relative to the store's {@code root} and use '/' separators.
 * Implementations are thread-safe; each call is one logical request and implementations retry
 * transient failures up to {@code replication.requests.retry.attempts} times before throwing a
 * {@link ObjectStoreException.Kind#RETRYABLE} exception.
 */
public interface ObjectStore extends QuietCloseable {

    /**
     * Deletes an object. Deleting a missing object succeeds.
     */
    void delete(@NotNull String key);

    /**
     * Reads a whole object into {@code sink}, replacing its content.
     *
     * @param ifNoneMatch when not null and equal to the object's current ETag, nothing is read and
     *                    the method returns null
     * @return the object's ETag, or null when {@code ifNoneMatch} matched
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#NOT_FOUND} when absent
     */
    @Nullable
    String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink);

    /**
     * Default delete request rate for the WAL cleaner, per {@code replication.primary.cleaner.max.requests.per.second}.
     */
    int getDefaultCleanerMaxRequestsPerSecond();

    /**
     * Describes the store for logs, without secrets.
     */
    @NotNull
    String getDescription();

    /**
     * True when the service verifies the integrity of what it stores and returns, so that
     * {@code replication.primary.checksum=service-dependent} skips our own checksum.
     */
    boolean isIntegrityChecked();

    /**
     * Lists object keys under {@code prefix} in lexicographic order, calling {@code consumer} for each,
     * starting after {@code startAfter} when not null. Listing stops early when the consumer returns false.
     */
    void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer);

    /**
     * Writes a whole object.
     *
     * @param ifMatch       when not null, the write succeeds only if the object's current ETag equals it
     * @param isIfNoneMatch when true, the write succeeds only if the object does not exist
     * @return the new object's ETag
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#PRECONDITION_FAILED} when a
     *                              condition does not hold
     */
    @NotNull
    String put(@NotNull String key, byte @NotNull [] data, int offset, int length, @Nullable String ifMatch, boolean isIfNoneMatch);

    @FunctionalInterface
    interface ListConsumer {
        /**
         * @return false to stop listing
         */
        boolean onKey(@NotNull String key, long size);
    }
}
