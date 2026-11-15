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
 * Passes every request to another store and remembers when one last returned, with a result or an
 * error. The replication lease keeper reads it to tell a long upload or cleanup round, in which
 * requests keep returning, from threads that are stuck.
 */
public class ActivityObjectStore implements ObjectStore {
    private final ObjectStore delegate;
    private volatile long lastActivityNanos = System.nanoTime();

    public ActivityObjectStore(@NotNull ObjectStore delegate) {
        this.delegate = delegate;
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
     * {@link System#nanoTime()} of the last request that returned, or of the last {@link #touch()}.
     */
    public long getLastActivityNanos() {
        return lastActivityNanos;
    }

    @Override
    public boolean isIntegrityChecked() {
        return delegate.isIntegrityChecked();
    }

    @Override
    public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
        try {
            delegate.list(prefix, startAfter, consumer);
        } finally {
            touch();
        }
    }

    @Override
    public @NotNull String put(@NotNull String key, byte @NotNull [] data, int offset, int length, @Nullable String ifMatch, boolean isIfNoneMatch) {
        try {
            return delegate.put(key, data, offset, length, ifMatch, isIfNoneMatch);
        } finally {
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
