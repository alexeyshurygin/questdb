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

import io.questdb.replication.store.ActivityObjectStore;
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ActivityObjectStoreTest {
    private static final long PAUSE_MILLIS = 40;

    @Test
    public void testAnUploadIsAliveForAsLongAsItsSizeAllows() {
        final Recorder recorder = new Recorder();
        // base timeout 0 and no retries: an upload may take its size over 1000 bytes per second
        final ActivityObjectStore store = new ActivityObjectStore(recorder, 0, 1_000, 0, 0);
        recorder.store = store;

        // 10 bytes are allowed 10 ms: the 40 ms the request takes are past that
        Assert.assertEquals("etag-a", store.put("a", new byte[10], 0, 10, null, false));
        Assert.assertTrue("idle " + recorder.idleInsideNanos, recorder.idleInsideNanos >= TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        Assert.assertTrue(store.getIdleNanos() < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS));

        // 10000 bytes are allowed ten seconds: the request is alive all along
        Assert.assertEquals("etag-b", store.put("b", new byte[10_000], 0, 10_000, "x", true));
        Assert.assertEquals(0, recorder.idleInsideNanos);
        // once the request is back its allowance is over: idle time counts again
        Os.sleep(PAUSE_MILLIS);
        Assert.assertTrue(store.getIdleNanos() >= TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        Assert.assertEquals(List.of("put a 10 null false", "put b 10000 x true"), recorder.calls);
    }

    @Test
    public void testEachAttemptAndRetryPauseCountForAnUpload() {
        final Recorder recorder = new Recorder();
        // two retries of 20 ms each, 5 ms apart: 3 * 20 + 2 * 5 = 70 ms for an empty upload
        final ActivityObjectStore store = new ActivityObjectStore(recorder, 20, 1_000_000, 2, 5);
        recorder.store = store;
        store.put("a", new byte[0], 0, 0, null, false);
        // 40 ms into the request, within its 70 ms
        Assert.assertEquals(0, recorder.idleInsideNanos);

        // negative settings count as none: an empty upload gets no time at all
        final Recorder strict = new Recorder();
        final ActivityObjectStore strictStore = new ActivityObjectStore(strict, 0, -5, -1, -1);
        strict.store = strictStore;
        strictStore.put("a", new byte[0], 0, 0, null, false);
        Assert.assertTrue(strict.idleInsideNanos >= TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
    }

    @Test
    public void testEveryListedKeyIsASignOfLife() {
        final Recorder recorder = new Recorder();
        final ActivityObjectStore store = new ActivityObjectStore(recorder, 0, 1, 0, 0);
        recorder.store = store;
        final List<String> keys = new ArrayList<>();
        final List<Long> idleAtKey = new ArrayList<>();
        // the delegate pauses before every key, as a store does between pages
        store.list("p/", "p/0", (key, size) -> {
            idleAtKey.add(store.getIdleNanos());
            keys.add(key + ' ' + size);
            return keys.size() < 2;
        });
        Assert.assertEquals(List.of("p/1 1", "p/2 2"), keys);
        // the consumer stopped the listing after the second key
        Assert.assertEquals(List.of("list p/ p/0", "key p/1", "key p/2"), recorder.calls);
        for (long idle : idleAtKey) {
            Assert.assertTrue("idle " + idle, idle < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        }
        Assert.assertTrue(store.getIdleNanos() < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
    }

    @Test
    public void testIdleTimeGrowsUntilARequestReturns() {
        final Recorder recorder = new Recorder();
        final ActivityObjectStore store = new ActivityObjectStore(recorder, 0, 1, 0, 0);
        recorder.store = store;
        Os.sleep(PAUSE_MILLIS);
        Assert.assertTrue(store.getIdleNanos() >= TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        store.touch();
        Assert.assertTrue(store.getIdleNanos() < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));

        // a request is idle time while it runs, and a sign of life when it returns
        final ByteSink sink = new ByteSink();
        Assert.assertEquals("etag-k", store.get("k", "old", sink));
        Assert.assertTrue(recorder.idleInsideNanos >= TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        Assert.assertTrue(store.getIdleNanos() < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        store.delete("k");
        Assert.assertTrue(recorder.idleInsideNanos >= TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        Assert.assertTrue(store.getIdleNanos() < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        Assert.assertEquals(List.of("get k old", "delete k"), recorder.calls);
    }

    @Test
    public void testRequestThatFailsIsASignOfLifeToo() {
        final Recorder recorder = new Recorder();
        final ActivityObjectStore store = new ActivityObjectStore(recorder, 0, 1, 0, 0);
        recorder.store = store;
        recorder.isFailing = true;
        final ByteSink sink = new ByteSink();
        final ObjectStore.ListConsumer consumer = (key, size) -> true;
        final Runnable[] requests = {
                () -> store.delete("k"),
                () -> store.get("k", null, sink),
                () -> store.list("p/", null, consumer),
                () -> store.put("k", new byte[1], 0, 1, null, false),
        };
        for (Runnable request : requests) {
            Os.sleep(PAUSE_MILLIS);
            try {
                request.run();
                Assert.fail("the store's error must reach the caller");
            } catch (ObjectStoreException e) {
                Assert.assertEquals("injected", e.getMessage());
            }
            Assert.assertTrue(store.getIdleNanos() < TimeUnit.MILLISECONDS.toNanos(PAUSE_MILLIS - 10));
        }
        Assert.assertEquals(List.of("delete k", "get k null", "list p/ null", "put k 1 null false"), recorder.calls);
    }

    @Test
    public void testTheRestGoesStraightToTheStore() {
        final Recorder recorder = new Recorder();
        final ActivityObjectStore store = new ActivityObjectStore(recorder, 0, 1, 0, 0);
        Assert.assertEquals(7, store.getDefaultCleanerMaxRequestsPerSecond());
        Assert.assertEquals("recorder", store.getDescription());
        Assert.assertTrue(store.isIntegrityChecked());
        Assert.assertFalse(recorder.isClosed);
        store.close();
        Assert.assertTrue(recorder.isClosed);
    }

    // answers every request after a pause, and notes how idle the wrapping store was at the end of it
    private static class Recorder implements ObjectStore {
        final List<String> calls = new ArrayList<>();
        long idleInsideNanos = -1;
        boolean isClosed;
        boolean isFailing;
        ActivityObjectStore store;

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public void delete(@NotNull String key) {
            calls.add("delete " + key);
            pause();
        }

        @Override
        public @Nullable String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink) {
            calls.add("get " + key + ' ' + ifNoneMatch);
            pause();
            return "etag-" + key;
        }

        @Override
        public int getDefaultCleanerMaxRequestsPerSecond() {
            return 7;
        }

        @Override
        public @NotNull String getDescription() {
            return "recorder";
        }

        @Override
        public boolean isIntegrityChecked() {
            return true;
        }

        @Override
        public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
            calls.add("list " + prefix + ' ' + startAfter);
            for (int i = 1; i <= 3; i++) {
                pause();
                calls.add("key " + prefix + i);
                if (!consumer.onKey(prefix + i, i)) {
                    return;
                }
            }
        }

        @Override
        public @NotNull String put(@NotNull String key, byte @NotNull [] data, int offset, int length, @Nullable String ifMatch, boolean isIfNoneMatch) {
            calls.add("put " + key + ' ' + length + ' ' + ifMatch + ' ' + isIfNoneMatch);
            pause();
            return "etag-" + key;
        }

        private void pause() {
            Os.sleep(PAUSE_MILLIS);
            if (store != null) {
                idleInsideNanos = store.getIdleNanos();
            }
            if (isFailing) {
                throw ObjectStoreException.fatal("injected");
            }
        }
    }
}
