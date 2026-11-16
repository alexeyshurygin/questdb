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
import io.questdb.replication.store.ObjectStoreException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Thread-safe in-memory {@link ObjectStore} for replication tests, with request counters and a
 * scriptable fault hook.
 */
public class MemoryObjectStore implements ObjectStore {
    private final AtomicLong closeCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();
    private final AtomicLong etagSequence = new AtomicLong();
    private final AtomicLong getCount = new AtomicLong();
    private final AtomicLong listCount = new AtomicLong();
    private final TreeMap<String, Entry> objects = new TreeMap<>();
    private final AtomicLong putCount = new AtomicLong();
    private volatile FaultHook faultHook;
    private volatile boolean isIntegrityChecked = true;

    public synchronized boolean contains(String key) {
        return objects.containsKey(key);
    }

    @Override
    public void close() {
        // the store stays usable: the nodes of a test share it, each closing it as it stops
        closeCount.incrementAndGet();
    }

    @Override
    public void delete(@NotNull String key) {
        deleteCount.incrementAndGet();
        fault("delete", key);
        synchronized (this) {
            objects.remove(key);
        }
    }

    @Override
    public @Nullable String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink) {
        getCount.incrementAndGet();
        fault("get", key);
        final Entry e;
        synchronized (this) {
            e = objects.get(key);
        }
        if (e == null) {
            throw ObjectStoreException.notFound(key);
        }
        if (e.etag.equals(ifNoneMatch)) {
            return null;
        }
        sink.clear();
        sink.put(e.data, 0, e.data.length);
        return e.etag;
    }

    public synchronized byte[] getBytes(String key) {
        final Entry e = objects.get(key);
        return e == null ? null : e.data.clone();
    }

    /**
     * How many times a user closed the store.
     */
    public long getCloseCount() {
        return closeCount.get();
    }

    public long getDeleteCount() {
        return deleteCount.get();
    }

    @Override
    public int getDefaultCleanerMaxRequestsPerSecond() {
        return 0;
    }

    @Override
    public @NotNull String getDescription() {
        return "memory";
    }

    public long getGetCount() {
        return getCount.get();
    }

    public long getListCount() {
        return listCount.get();
    }

    public long getPutCount() {
        return putCount.get();
    }

    @Override
    public boolean isIntegrityChecked() {
        return isIntegrityChecked;
    }

    public synchronized List<String> keys(Predicate<String> filter) {
        final List<String> result = new ArrayList<>();
        for (String k : objects.keySet()) {
            if (filter.test(k)) {
                result.add(k);
            }
        }
        return result;
    }

    @Override
    public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
        listCount.incrementAndGet();
        fault("list", prefix);
        final List<Map.Entry<String, Entry>> snapshot = new ArrayList<>();
        synchronized (this) {
            for (Map.Entry<String, Entry> e : objects.tailMap(prefix, true).entrySet()) {
                if (!e.getKey().startsWith(prefix)) {
                    break;
                }
                snapshot.add(Map.entry(e.getKey(), e.getValue()));
            }
        }
        for (Map.Entry<String, Entry> e : snapshot) {
            if (startAfter != null && e.getKey().compareTo(startAfter) <= 0) {
                continue;
            }
            if (!consumer.onKey(e.getKey(), e.getValue().data.length)) {
                return;
            }
        }
    }

    @Override
    public @NotNull String put(@NotNull String key, byte @NotNull [] data, int offset, int length, @Nullable String ifMatch, boolean isIfNoneMatch) {
        putCount.incrementAndGet();
        fault("put", key);
        synchronized (this) {
            final Entry current = objects.get(key);
            if (isIfNoneMatch && current != null) {
                throw ObjectStoreException.preconditionFailed(key);
            }
            if (ifMatch != null && (current == null || !current.etag.equals(ifMatch))) {
                throw ObjectStoreException.preconditionFailed(key);
            }
            final Entry e = new Entry(Arrays.copyOfRange(data, offset, offset + length), "etag-" + etagSequence.incrementAndGet());
            objects.put(key, e);
            return e.etag;
        }
    }

    /**
     * Replaces an object as an outside writer would, bypassing the fault hook and conditions.
     */
    public synchronized void putRaw(String key, byte[] data) {
        objects.put(key, new Entry(data.clone(), "etag-" + etagSequence.incrementAndGet()));
    }

    public void setFaultHook(@Nullable FaultHook faultHook) {
        this.faultHook = faultHook;
    }

    public void setIntegrityChecked(boolean integrityChecked) {
        isIntegrityChecked = integrityChecked;
    }

    public synchronized int size() {
        return objects.size();
    }

    private void fault(String op, String key) {
        final FaultHook hook = faultHook;
        if (hook != null) {
            hook.onRequest(op, key);
        }
    }

    @FunctionalInterface
    public interface FaultHook {
        /**
         * Called before each request; throw to fail it.
         */
        void onRequest(String op, String key);
    }

    private record Entry(byte[] data, String etag) {
    }
}
