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
import org.jetbrains.annotations.Nullable;

/**
 * Passes every request to another store and remembers the last sign of life: a request that returned,
 * with a result or an error, a key that a listing delivered, or an upload that is still within the
 * time its size allows. The replication lease keeper reads it to tell a long upload or cleanup
 * round from threads that are stuck.
 */
public class ActivityObjectStore implements ObjectStore {
    private final Object busyLock = new Object();
    private final ObjectStore delegate;
    private final long minThroughput;
    private final long requestBaseTimeoutMillis;
    private final int retryAttempts;
    private final long retryIntervalMillis;
    // until when the uploads in flight may legitimately take; guarded by busyLock
    private long busyUntilNanos = System.nanoTime();
    // guarded by busyLock
    private int inFlightUploads;
    private volatile long lastActivityNanos = System.nanoTime();

    /**
     * @param requestBaseTimeoutMillis {@code replication.requests.base.timeout}
     * @param minThroughput            {@code replication.requests.min.throughput}, bytes per second
     * @param retryAttempts            {@code replication.requests.retry.attempts}
     * @param retryIntervalMillis      {@code replication.requests.retry.interval}
     */
    public ActivityObjectStore(@NotNull ObjectStore delegate, long requestBaseTimeoutMillis, long minThroughput, int retryAttempts, long retryIntervalMillis) {
        this.delegate = delegate;
        this.requestBaseTimeoutMillis = requestBaseTimeoutMillis;
        this.minThroughput = Math.max(1, minThroughput);
        this.retryAttempts = Math.max(0, retryAttempts);
        this.retryIntervalMillis = Math.max(0, retryIntervalMillis);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void delete(@NotNull String key) {
        try {
            delegate.delete(key);
        } finally {
            touch();
        }
    }

    @Override
    public @Nullable String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink) {
        try {
            return delegate.get(key, ifNoneMatch, sink);
        } finally {
            touch();
        }
    }

    @Override
    public int getDefaultCleanerMaxRequestsPerSecond() {
        return delegate.getDefaultCleanerMaxRequestsPerSecond();
    }

    @Override
    public @NotNull String getDescription() {
        return delegate.getDescription();
    }

    /**
     * How long ago the last sign of life was, in nanoseconds; 0 while an upload is within its time.
     */
    public long getIdleNanos() {
        final long busyUntil;
        synchronized (busyLock) {
            busyUntil = busyUntilNanos;
        }
        final long now = System.nanoTime();
        return Math.max(0, Math.min(now - lastActivityNanos, now - busyUntil));
    }

    @Override
    public boolean isIntegrityChecked() {
        return delegate.isIntegrityChecked();
    }

    @Override
    public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
        try {
            // a listing is many requests, one per page: every key shows that pages keep coming
            delegate.list(prefix, startAfter, (key, size) -> {
                touch();
                return consumer.onKey(key, size);
            });
        } finally {
            touch();
        }
    }

    @Override
    public @NotNull String put(@NotNull String key, byte @NotNull [] data, int offset, int length, @Nullable String ifMatch, boolean isIfNoneMatch) {
        // one upload may take every attempt the store makes, each as long as its size allows
        final long allowedMillis = (retryAttempts + 1) * (requestBaseTimeoutMillis + length * 1000L / minThroughput)
                + retryAttempts * retryIntervalMillis;
        final long until = System.nanoTime() + allowedMillis * 1_000_000L;
        synchronized (busyLock) {
            inFlightUploads++;
            if (until - busyUntilNanos > 0) {
                busyUntilNanos = until;
            }
        }
        try {
            return delegate.put(key, data, offset, length, ifMatch, isIfNoneMatch);
        } finally {
            synchronized (busyLock) {
                if (--inFlightUploads == 0) {
                    // nothing is in flight any more: from here on only returns and keys count
                    busyUntilNanos = System.nanoTime();
                }
            }
            touch();
        }
    }

    /**
     * Records a sign of life that is not a request.
     */
    public void touch() {
        lastActivityNanos = System.nanoTime();
    }
}
