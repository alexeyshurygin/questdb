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


package io.questdb.test.replication;

import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps a real object store and lets a test fail chosen requests, before they reach the store or
 * after it carried them out (a request that landed but whose response was lost).
 */
public class FaultyObjectStore implements ObjectStore {
    private final ObjectStore delegate;
    private final Hook hook;

    public FaultyObjectStore(@NotNull ObjectStore delegate, @NotNull Hook hook) {
        this.delegate = delegate;
        this.hook = hook;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void delete(@NotNull String key) {
        hook.before("delete", key);
        delegate.delete(key);
        hook.after("delete", key);
    }

    @Override
    public @Nullable String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink) {
        hook.before("get", key);
        final String etag = delegate.get(key, ifNoneMatch, sink);
        hook.after("get", key);
        return etag;
    }

    @Override
    public int getDefaultCleanerMaxRequestsPerSecond() {
        return delegate.getDefaultCleanerMaxRequestsPerSecond();
    }

    @Override
    public @NotNull String getDescription() {
        return "faulty " + delegate.getDescription();
    }

    @Override
    public boolean isIntegrityChecked() {
        return delegate.isIntegrityChecked();
    }

    @Override
    public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
        hook.before("list", prefix);
        delegate.list(prefix, startAfter, consumer);
        hook.after("list", prefix);
    }

    @Override
    public @NotNull String put(@NotNull String key, byte @NotNull [] data, int offset, int length, @Nullable String ifMatch, boolean isIfNoneMatch) {
        hook.before("put", key);
        final String etag = delegate.put(key, data, offset, length, ifMatch, isIfNoneMatch);
        hook.after("put", key);
        return etag;
    }

    public interface Hook {
        default void after(String op, String key) {
        }

        default void before(String op, String key) {
        }
    }
}
