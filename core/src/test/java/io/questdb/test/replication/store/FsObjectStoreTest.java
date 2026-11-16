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

package io.questdb.test.replication.store;

import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.FsObjectStore;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreSettings;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

public class FsObjectStoreTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private ReplicationConfigurationImpl configuration;
    private Path root;

    public static ObjectStoreException assertFails(ObjectStoreException.Kind kind, int status, String message, Runnable action) {
        try {
            action.run();
        } catch (ObjectStoreException e) {
            Assert.assertEquals(message, e.getMessage());
            Assert.assertEquals(kind, e.getKind());
            Assert.assertEquals(status, e.getStatus());
            Assert.assertEquals(kind == ObjectStoreException.Kind.RETRYABLE, e.isRetryable());
            return e;
        }
        Assert.fail("expected " + kind + ": " + message);
        return null;
    }

    public static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // independent ETag oracle: first 16 bytes of SHA-256, lower-case hex
    public static String expectedETag(byte[] data) throws Exception {
        final byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            b.append(String.format("%02x", hash[i] & 0xff));
        }
        return b.toString();
    }

    public static String utf8(ByteSink sink) {
        return new String(sink.data(), 0, sink.size(), StandardCharsets.UTF_8);
    }

    @Before
    public void setUp() throws IOException {
        root = temp.newFolder("store").toPath().toRealPath();
        configuration = new ReplicationConfigurationImpl().setRetryAttempts(2).setRetryInterval(1);
    }

    @Test
    public void testAtomicWriteDirElsewhere() throws Exception {
        final Path scratch = temp.newFolder("scratch").toPath().toRealPath();
        try (FsObjectStore store = newStore("fs::root=" + root + ";atomic_write_dir=" + scratch + ';')) {
            Assert.assertEquals(scratch, store.getAtomicWriteDir());
            Assert.assertEquals("fs root=" + root + ", atomic_write_dir=" + scratch, store.getDescription());
            store.put("a", bytes("1"), 0, 1, null, false);
            // with the scratch directory outside the root, .tmp is an ordinary key
            store.put(".tmp/x", bytes("22"), 0, 2, null, true);
            Assert.assertEquals(List.of(".tmp/x:2", "a:1"), list(store, "", null));
            try (Stream<Path> files = Files.list(scratch)) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testAtomicWriteDirMustNotContainRoot() throws Exception {
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "object store setting atomic_write_dir must not be root or contain root [root=" + root + ", atomic_write_dir=" + root + ']',
                () -> newStore("fs::root=" + root + ";atomic_write_dir=" + root + "/;")
        );
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "object store setting atomic_write_dir must not be root or contain root [root=" + root + ", atomic_write_dir=" + root.getParent() + ']',
                () -> newStore("fs::root=" + root + ";atomic_write_dir=" + root.getParent() + ';')
        );
    }

    @Test
    public void testAtomicWriteDirNotWritable() throws Exception {
        final Path scratch = temp.newFolder("readonly").toPath();
        try (FsObjectStore store = newStore("fs::root=" + root + ";atomic_write_dir=" + scratch + ';')) {
            Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                Assume.assumeFalse("running as a user that ignores permissions", Files.isWritable(scratch));
                store.put("k", bytes("v"), 0, 1, null, false);
                Assert.fail("expected RETRYABLE");
            } catch (ObjectStoreException e) {
                Assert.assertEquals(ObjectStoreException.Kind.RETRYABLE, e.getKind());
                Assert.assertEquals(0, e.getStatus());
                Assert.assertTrue(e.getCause() instanceof AccessDeniedException);
                // the message names the generated temporary file, which carries the process id
                final String prefix = "object store I/O failed [op=PUT, key=k, attempts=3, error=java.nio.file.AccessDeniedException: "
                        + store.getAtomicWriteDir() + "/put-" + ProcessHandle.current().pid() + '-';
                Assert.assertTrue(e.getMessage(), e.getMessage().startsWith(prefix) && e.getMessage().endsWith(".tmp]"));
            } finally {
                Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
            Assert.assertFalse(Files.exists(root.resolve("k")));
            try (Stream<Path> files = Files.list(scratch)) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testComputeETag() throws Exception {
        final byte[] data = bytes("0123456789");
        Assert.assertEquals(expectedETag(bytes("2345")), FsObjectStore.computeETag(data, 2, 4));
        Assert.assertEquals("e3b0c44298fc1c149afbf4c8996fb924", FsObjectStore.computeETag(data, 0, 0));
    }

    @Test
    public void testConcurrentIfMatchCounter() throws Exception {
        // four writers, two store instances on one root, each increments a counter with If-Match
        final int threads = 4;
        final int increments = 10;
        try (FsObjectStore store1 = newStore(); FsObjectStore store2 = newStore()) {
            store1.put("counter", bytes("0"), 0, 1, null, true);
            final CyclicBarrier start = new CyclicBarrier(threads);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final AtomicInteger conflicts = new AtomicInteger();
            final List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final FsObjectStore store = t % 2 == 0 ? store1 : store2;
                final Thread worker = new Thread(() -> {
                    try {
                        start.await();
                        final ByteSink sink = new ByteSink();
                        // a broken If-Match fails every write; give up instead of spinning
                        for (int i = 0, attempts = 0; i < increments; ) {
                            Assert.assertTrue("too many conflicts", ++attempts < 1_000);
                            final String etag = store.get("counter", null, sink);
                            final byte[] next = bytes(String.valueOf(Integer.parseInt(utf8(sink)) + 1));
                            try {
                                store.put("counter", next, 0, next.length, etag, false);
                                i++;
                            } catch (ObjectStoreException e) {
                                Assert.assertEquals(ObjectStoreException.Kind.PRECONDITION_FAILED, e.getKind());
                                conflicts.incrementAndGet();
                            }
                        }
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            Assert.assertNull(failure.get());
            final ByteSink sink = new ByteSink();
            store1.get("counter", null, sink);
            Assert.assertEquals(String.valueOf(threads * increments), utf8(sink));
        }
    }

    @Test
    public void testConcurrentIfMatchOneWinner() throws Exception {
        try (FsObjectStore store1 = newStore(); FsObjectStore store2 = newStore()) {
            for (int round = 0; round < 20; round++) {
                final String key = "race/" + round;
                final String base = store1.put(key, bytes("base"), 0, 4, null, false);
                final String[] winners = race(
                        () -> store1.put(key, bytes("one"), 0, 3, base, false),
                        () -> store2.put(key, bytes("two"), 0, 3, '"' + base + '"', false)
                );
                assertOneWinner(store1, key, winners, "one", "two");
            }
        }
    }

    @Test
    public void testConcurrentIfNoneMatchOneWinner() throws Exception {
        try (FsObjectStore store1 = newStore(); FsObjectStore store2 = newStore()) {
            for (int round = 0; round < 20; round++) {
                final String key = "race/" + round;
                final String[] winners = race(
                        () -> store1.put(key, bytes("one"), 0, 3, null, true),
                        () -> store2.put(key, bytes("two"), 0, 3, null, true)
                );
                assertOneWinner(store1, key, winners, "one", "two");
            }
            try (Stream<Path> files = Files.list(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME))) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testDelete() throws Exception {
        try (FsObjectStore store = newStore()) {
            store.put("d/x", bytes("x"), 0, 1, null, false);
            store.put("k", bytes("k"), 0, 1, null, false);
            store.delete("k");
            Assert.assertFalse(Files.exists(root.resolve("k")));
            assertFails(ObjectStoreException.Kind.NOT_FOUND, 404, "object not found [key=k]", () -> store.get("k", null, new ByteSink()));
            // deleting a missing object succeeds
            store.delete("k");
            store.delete("missing/deep/key");
            // a directory is not an object
            store.delete("d");
            Assert.assertEquals(List.of("d/x:1"), list(store, "", null));
        }
    }

    @Test
    public void testDeleteRetriesThenFails() throws Exception {
        try (FsObjectStore store = newStore()) {
            store.put("d/x", bytes("x"), 0, 1, null, false);
            final Path dir = root.resolve("d");
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                Assume.assumeFalse("running as a user that ignores permissions", Files.isWritable(dir));
                final ObjectStoreException e = assertFails(
                        ObjectStoreException.Kind.RETRYABLE,
                        0,
                        "object store I/O failed [op=DELETE, key=d/x, attempts=3, error=java.nio.file.AccessDeniedException: " + dir.resolve("x") + ']',
                        () -> store.delete("d/x")
                );
                Assert.assertTrue(e.getCause() instanceof AccessDeniedException);
            } finally {
                Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
        }
    }

    @Test
    public void testGetDirectoryIsNotFound() throws Exception {
        try (FsObjectStore store = newStore()) {
            store.put("d/x", bytes("x"), 0, 1, null, false);
            assertFails(ObjectStoreException.Kind.NOT_FOUND, 404, "object not found [key=d]", () -> store.get("d", null, new ByteSink()));
        }
    }

    @Test
    public void testGetIfNoneMatch() throws Exception {
        try (FsObjectStore store = newStore()) {
            final byte[] data = bytes("payload");
            final String etag = store.put("k", data, 0, data.length, null, false);
            final ByteSink sink = new ByteSink(16);
            sink.put(bytes("old"), 0, 3);
            Assert.assertNull(store.get("k", etag, sink));
            Assert.assertEquals("old", utf8(sink));
            Assert.assertNull(store.get("k", '"' + etag + '"', sink));
            Assert.assertEquals("old", utf8(sink));
            Assert.assertEquals(etag, store.get("k", "0123456789abcdef0123456789abcdef", sink));
            Assert.assertEquals("payload", utf8(sink));
            assertFails(ObjectStoreException.Kind.NOT_FOUND, 404, "object not found [key=missing]", () -> store.get("missing", etag, sink));
        }
    }

    @Test
    public void testGetLargeObject() throws Exception {
        final byte[] data = new byte[300_001];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        try (FsObjectStore store = newStore()) {
            final String etag = store.put("big", data, 0, data.length, null, false);
            Assert.assertEquals(expectedETag(data), etag);
            final ByteSink sink = new ByteSink(16);
            Assert.assertEquals(etag, store.get("big", null, sink));
            Assert.assertArrayEquals(data, sink.toByteArray());
            // matching If-None-Match hashes the whole file across several read chunks
            Assert.assertNull(store.get("big", etag, sink));
            // a sink holding more than the object ends up with exactly the object
            final ByteSink bigSink = new ByteSink();
            bigSink.put(new byte[400_000], 0, 400_000);
            store.get("big", null, bigSink);
            Assert.assertArrayEquals(data, bigSink.toByteArray());
        }
    }

    @Test
    public void testGetObjectTooLargeToBuffer() throws Exception {
        try (FsObjectStore store = newStore()) {
            // a sparse file takes no space but reports a size beyond what a byte array holds
            try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(root.resolve("huge").toFile(), "rw")) {
                file.setLength(3_000_000_000L);
            }
            final ByteSink sink = new ByteSink();
            sink.put(bytes("old"), 0, 3);
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object too large to buffer [key=huge, size=3000000000]",
                    () -> store.get("huge", null, sink));
            Assert.assertEquals("old", utf8(sink));
            Assert.assertEquals(List.of("huge:3000000000"), list(store, "", null));
        }
    }

    @Test
    public void testGetUnreadableFileRetriesThenSucceeds() throws Exception {
        final List<String> retries = new ArrayList<>();
        try (FsObjectStore store = new FsObjectStore(settings("fs::root=" + root + ';'), configuration) {
            @Override
            protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
                retries.add(operation + ' ' + subject + ' ' + attempt + ' ' + error.getClass().getSimpleName());
                try {
                    Files.setPosixFilePermissions(root.resolve("k"), PosixFilePermissions.fromString("rw-r--r--"));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }) {
            store.put("k", bytes("v"), 0, 1, null, false);
            Files.setPosixFilePermissions(root.resolve("k"), PosixFilePermissions.fromString("---------"));
            Assume.assumeFalse("running as a user that ignores permissions", Files.isReadable(root.resolve("k")));
            final ByteSink sink = new ByteSink();
            Assert.assertEquals(expectedETag(bytes("v")), store.get("k", null, sink));
            Assert.assertEquals("v", utf8(sink));
            Assert.assertEquals(List.of("GET key=k 1 AccessDeniedException"), retries);
        }
    }

    @Test
    public void testIfMatch() throws Exception {
        try (FsObjectStore store = newStore()) {
            final String e1 = store.put("k", bytes("v1"), 0, 2, null, false);
            final String e2 = store.put("k", bytes("v2"), 0, 2, e1, false);
            Assert.assertEquals(expectedETag(bytes("v2")), e2);
            assertFails(
                    ObjectStoreException.Kind.PRECONDITION_FAILED,
                    412,
                    "object store precondition failed [key=k]",
                    () -> store.put("k", bytes("v3"), 0, 2, e1, false)
            );
            final ByteSink sink = new ByteSink();
            Assert.assertEquals(e2, store.get("k", null, sink));
            Assert.assertEquals("v2", utf8(sink));
            // the quoted form that HTTP uses works too
            final String e4 = store.put("k", bytes("v4"), 0, 2, '"' + e2 + '"', false);
            Assert.assertEquals(expectedETag(bytes("v4")), e4);
            // If-Match on a missing object fails
            assertFails(
                    ObjectStoreException.Kind.PRECONDITION_FAILED,
                    412,
                    "object store precondition failed [key=missing]",
                    () -> store.put("missing", bytes("v"), 0, 1, e4, false)
            );
            Assert.assertFalse(Files.exists(root.resolve("missing")));
            Assert.assertTrue(Files.exists(root.resolve(FsObjectStore.LOCK_FILE_NAME)));
            // neither the lock file nor the scratch directory is listed
            Assert.assertEquals(List.of("k:2"), list(store, "", null));
            try (Stream<Path> files = Files.list(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME))) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testIfMatchAndIfNoneMatchAreExclusive() throws Exception {
        try (FsObjectStore store = newStore()) {
            assertFails(
                    ObjectStoreException.Kind.FATAL,
                    0,
                    "ifMatch and isIfNoneMatch are mutually exclusive [key=k]",
                    () -> store.put("k", bytes("v"), 0, 1, "abc", true)
            );
            Assert.assertFalse(Files.exists(root.resolve("k")));
        }
    }

    @Test
    public void testIfNoneMatch() throws Exception {
        try (FsObjectStore store = newStore()) {
            final String etag = store.put("a/b", bytes("first"), 0, 5, null, true);
            Assert.assertEquals(expectedETag(bytes("first")), etag);
            assertFails(
                    ObjectStoreException.Kind.PRECONDITION_FAILED,
                    412,
                    "object store precondition failed [key=a/b]",
                    () -> store.put("a/b", bytes("second"), 0, 6, null, true)
            );
            Assert.assertEquals("first", Files.readString(root.resolve("a/b")));
            try (Stream<Path> files = Files.list(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME))) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testInterruptedRetry() throws Exception {
        final Path blocker = temp.newFile("blocker").toPath();
        try (FsObjectStore store = newStore("fs::root=" + root + ";atomic_write_dir=" + blocker.resolve("tmp") + ';')) {
            Thread.currentThread().interrupt();
            try {
                final ObjectStoreException e = assertFails(
                        ObjectStoreException.Kind.RETRYABLE,
                        0,
                        "object store request interrupted",
                        () -> store.put("k", bytes("v"), 0, 1, null, false)
                );
                Assert.assertTrue(e.getCause() instanceof InterruptedException);
                Assert.assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    public void testInvalidRootPath() {
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "object store setting is not a valid path [key=root, reason=Nul character not allowed]",
                () -> newStore("fs::root=/a\u0000b;")
        );
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "object store setting is not a valid path [key=atomic_write_dir, reason=Nul character not allowed]",
                () -> newStore("fs::root=/a;atomic_write_dir=/b\u0000c;")
        );
    }

    @Test
    public void testIoErrorRetriesThenFails() throws Exception {
        final Path blocker = temp.newFile("blocker").toPath();
        try (FsObjectStore store = newStore("fs::root=" + root + ";atomic_write_dir=" + blocker.resolve("tmp") + ';')) {
            final ObjectStoreException e = assertFails(
                    ObjectStoreException.Kind.RETRYABLE,
                    0,
                    "object store I/O failed [op=PUT, key=k, attempts=3, error=java.nio.file.FileSystemException: " + blocker.resolve("tmp") + ": Not a directory]",
                    () -> store.put("k", bytes("v"), 0, 1, null, false)
            );
            Assert.assertEquals(FileSystemException.class, e.getCause().getClass());
            Assert.assertFalse(Files.exists(root.resolve("k")));
        }
    }

    @Test
    public void testIoErrorRetriesThenSucceeds() throws Exception {
        final Path blocker = temp.newFile("blocker").toPath();
        final List<String> retries = new ArrayList<>();
        try (FsObjectStore store = new FsObjectStore(settings("fs::root=" + root + ";atomic_write_dir=" + blocker.resolve("tmp") + ';'), configuration) {
            @Override
            protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
                retries.add(operation + ' ' + subject + ' ' + attempt + ' ' + error.getClass().getSimpleName());
                if (attempt == 2) {
                    try {
                        Files.delete(blocker);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                super.onRetry(operation, subject, attempt, error);
            }
        }) {
            Assert.assertEquals(expectedETag(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            Assert.assertEquals("v", Files.readString(root.resolve("k")));
            Assert.assertEquals(List.of("PUT key=k 1 FileSystemException", "PUT key=k 2 FileSystemException"), retries);
        }
    }

    @Test
    public void testKeyConflicts() throws Exception {
        try (FsObjectStore store = newStore()) {
            store.put("f", bytes("f"), 0, 1, null, false);
            store.put("d/x", bytes("x"), 0, 1, null, false);
            assertFails(
                    ObjectStoreException.Kind.FATAL,
                    0,
                    "object key conflicts with an existing object [key=f/g]",
                    () -> store.put("f/g", bytes("g"), 0, 1, null, false)
            );
            assertFails(
                    ObjectStoreException.Kind.FATAL,
                    0,
                    "object key names a directory [key=d]",
                    () -> store.put("d", bytes("d"), 0, 1, null, false)
            );
            Assert.assertEquals(List.of("d/x:1", "f:1"), list(store, "", null));
        }
    }

    @Test
    public void testKeyValidation() throws Exception {
        try (FsObjectStore store = newStore()) {
            final ByteSink sink = new ByteSink();
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key is empty", () -> store.get("", null, sink));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key must be relative [key=/etc/passwd]", () -> store.get("/etc/passwd", null, sink));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key contains a backslash or NUL character [key=a\\b]", () -> store.put("a\\b", bytes("v"), 0, 1, null, false));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key contains a backslash or NUL character [key=a?b]", () -> store.delete("a\u0000b"));
            for (String key : new String[]{"a//b", "a/", "./a", "a/./b", "../a", "a/..", "a/../b", ".", ".."}) {
                assertFails(ObjectStoreException.Kind.FATAL, 0, "object key has an empty, '.' or '..' segment [key=" + key + ']', () -> store.put(key, bytes("v"), 0, 1, null, false));
            }
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key is reserved by the filesystem store [key=.lock]", () -> store.put(".lock", bytes("v"), 0, 1, null, false));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key is reserved by the filesystem store [key=.tmp]", () -> store.get(".tmp", null, sink));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key is reserved by the filesystem store [key=.tmp/x]", () -> store.delete(".tmp/x"));
            // names that merely start with dots are fine
            store.put("..a/.b/...", bytes("v"), 0, 1, null, false);
            store.put(".lock2", bytes("v"), 0, 1, null, false);
            Assert.assertEquals(List.of("..a/.b/...:1", ".lock2:1"), list(store, "", null));
            Assert.assertEquals(0, sink.size());
        }
    }

    @Test
    public void testList() throws Exception {
        // keys chosen so that sorting directory names, or UTF-16 units, gives a different order
        final String[] keys = {"a-c", "a/b", "a/c/d", "a0", "ab", "b", "z", "\u00e9", "\ufffd", "\uD83D\uDE00", "x/\uD83D\uDE00", "x/\ufffd"};
        try (FsObjectStore store = newStore()) {
            for (int i = 0; i < keys.length; i++) {
                final byte[] data = new byte[i + 1];
                store.put(keys[i], data, 0, data.length, null, false);
            }
            final List<String> expected = new ArrayList<>();
            final String[] sorted = keys.clone();
            Arrays.sort(sorted, (a, b) -> Arrays.compareUnsigned(bytes(a), bytes(b)));
            for (String key : sorted) {
                expected.add(key + ':' + (indexOf(keys, key) + 1));
            }
            // the oracle sort by UTF-8 bytes agrees with the order written out by hand
            final List<String> handSorted = List.of(
                    "a-c:1", "a/b:2", "a/c/d:3", "a0:4", "ab:5", "b:6", "x/\ufffd:12", "x/\uD83D\uDE00:11", "z:7", "\u00e9:8", "\ufffd:9",
                    "\uD83D\uDE00:10"
            );
            Assert.assertEquals(handSorted, expected);
            Assert.assertEquals(handSorted, list(store, "", null));
        }
    }

    @Test
    public void testListMissingOrFileRoot() throws Exception {
        try (FsObjectStore store = newStore("fs::root=" + root.resolve("not/yet") + ';')) {
            Assert.assertEquals(List.of(), list(store, "", null));
        }
        final Path file = temp.newFile("file-root").toPath();
        try (FsObjectStore store = newStore("fs::root=" + file + ';')) {
            Assert.assertEquals(List.of(), list(store, "", null));
        }
    }

    @Test
    public void testListPrefixAndStartAfter() throws Exception {
        try (FsObjectStore store = newStore()) {
            for (String key : new String[]{"a-c", "a/b", "a/c/d", "a/c/e", "a0", "ab", "b"}) {
                store.put(key, bytes(key), 0, key.length(), null, false);
            }
            Assert.assertEquals(List.of("a/b:3", "a/c/d:5", "a/c/e:5"), list(store, "a/", null));
            Assert.assertEquals(List.of("a-c:3", "a/b:3", "a/c/d:5", "a/c/e:5", "a0:2", "ab:2"), list(store, "a", null));
            Assert.assertEquals(List.of("a/c/d:5", "a/c/e:5"), list(store, "a/c", null));
            Assert.assertEquals(List.of("a/c/e:5"), list(store, "a/c/e", null));
            Assert.assertEquals(List.of(), list(store, "a/c/e/f", null));
            Assert.assertEquals(List.of(), list(store, "nomatch", null));
            // startAfter is exclusive and need not be a key
            Assert.assertEquals(List.of("a/c/d:5", "a/c/e:5", "a0:2", "ab:2", "b:1"), list(store, "", "a/b"));
            Assert.assertEquals(List.of("a/b:3", "a/c/d:5", "a/c/e:5", "a0:2", "ab:2", "b:1"), list(store, "", "a/"));
            Assert.assertEquals(List.of("a/c/e:5", "a0:2", "ab:2", "b:1"), list(store, "", "a/c/d"));
            Assert.assertEquals(List.of("a0:2", "ab:2", "b:1"), list(store, "", "a/z"));
            Assert.assertEquals(List.of("a-c:3", "a/b:3", "a/c/d:5", "a/c/e:5", "a0:2", "ab:2", "b:1"), list(store, "", ""));
            Assert.assertEquals(List.of(), list(store, "", "b"));
            Assert.assertEquals(List.of("a/c/e:5"), list(store, "a/", "a/c/d"));
            Assert.assertEquals(List.of("a/b:3", "a/c/d:5", "a/c/e:5"), list(store, "a/", "a-c"));
        }
    }

    @Test
    public void testListRetryResumesAfterLastKey() throws Exception {
        final List<String> retries = new ArrayList<>();
        final Path dir = root.resolve("b");
        try (FsObjectStore store = new FsObjectStore(settings("fs::root=" + root + ';'), configuration) {
            @Override
            protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
                retries.add(operation + " [" + subject + "] " + attempt + ' ' + error.getClass().getSimpleName());
                try {
                    Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }) {
            store.put("a", bytes("a"), 0, 1, null, false);
            store.put("b/x", bytes("bx"), 0, 2, null, false);
            store.put("c", bytes("ccc"), 0, 3, null, false);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("---------"));
            Assume.assumeFalse("running as a user that ignores permissions", Files.isReadable(dir));
            Assert.assertEquals(List.of("a:1", "b/x:2", "c:3"), list(store, "", null));
            Assert.assertEquals(List.of("LIST [prefix=] 1 AccessDeniedException"), retries);
        } finally {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    @Test
    public void testListRetriesThenFails() throws Exception {
        final Path dir = root.resolve("b");
        try (FsObjectStore store = newStore()) {
            store.put("b/x", bytes("bx"), 0, 2, null, false);
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("---------"));
            Assume.assumeFalse("running as a user that ignores permissions", Files.isReadable(dir));
            final ObjectStoreException e = assertFails(
                    ObjectStoreException.Kind.RETRYABLE,
                    0,
                    "object store I/O failed [op=LIST, prefix=b, attempts=3, error=java.nio.file.AccessDeniedException: " + dir + ']',
                    () -> list(store, "b", null)
            );
            Assert.assertTrue(e.getCause() instanceof AccessDeniedException);
        } finally {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    @Test
    public void testListSkipsDanglingLinksAndSpecialFiles() throws Exception {
        try (FsObjectStore store = newStore()) {
            store.put("k", bytes("v"), 0, 1, null, false);
            Files.createSymbolicLink(root.resolve("dangling"), root.resolve("nowhere"));
            // a link to a directory lists the directory's objects
            Files.createDirectories(temp.getRoot().toPath().resolve("elsewhere"));
            Files.writeString(temp.getRoot().toPath().resolve("elsewhere/obj"), "12");
            Files.createSymbolicLink(root.resolve("linked"), temp.getRoot().toPath().resolve("elsewhere"));
            Assert.assertEquals(List.of("k:1", "linked/obj:2"), list(store, "", null));
            // a named pipe is neither a regular file nor a directory
            final Path fifo = root.resolve("fifo");
            boolean hasFifo;
            try {
                hasFifo = new ProcessBuilder("mkfifo", fifo.toString()).start().waitFor() == 0 && Files.exists(fifo);
            } catch (IOException e) {
                hasFifo = false;
            }
            Assume.assumeTrue("mkfifo is not available", hasFifo);
            Assert.assertFalse(Files.isRegularFile(fifo));
            Assert.assertEquals(List.of("k:1", "linked/obj:2"), list(store, "", null));
        }
    }

    @Test
    public void testListStopsWhenConsumerReturnsFalse() throws Exception {
        try (FsObjectStore store = newStore()) {
            for (String key : new String[]{"a", "b/c", "b/d", "e"}) {
                store.put(key, bytes(key), 0, key.length(), null, false);
            }
            final List<String> seen = new ArrayList<>();
            store.list("", null, (key, size) -> {
                seen.add(key);
                return seen.size() < 2;
            });
            Assert.assertEquals(List.of("a", "b/c"), seen);
            seen.clear();
            store.list("", null, (key, size) -> {
                seen.add(key);
                return seen.size() < 3;
            });
            Assert.assertEquals(List.of("a", "b/c", "b/d"), seen);
        }
    }

    @Test
    public void testProperties() throws Exception {
        try (FsObjectStore store = newStore()) {
            Assert.assertEquals(root, store.getRoot());
            Assert.assertEquals(root.resolve(".tmp"), store.getAtomicWriteDir());
            Assert.assertEquals("fs root=" + root + ", atomic_write_dir=" + root.resolve(".tmp"), store.getDescription());
            Assert.assertFalse(store.isIntegrityChecked());
            Assert.assertEquals(100, store.getDefaultCleanerMaxRequestsPerSecond());
        }
        // a relative root resolves against the working directory
        try (FsObjectStore store = newStore("fs::root=rel/../store2;")) {
            Assert.assertEquals(Path.of("store2").toAbsolutePath(), store.getRoot());
        }
    }

    @Test
    public void testPutAndGet() throws Exception {
        try (FsObjectStore store = newStore()) {
            final byte[] data = bytes("hello, object store");
            final String etag = store.put("a/b/c.bin", data, 0, data.length, null, false);
            Assert.assertEquals(expectedETag(data), etag);
            Assert.assertArrayEquals(data, Files.readAllBytes(root.resolve("a/b/c.bin")));
            final ByteSink sink = new ByteSink(16);
            Assert.assertEquals(etag, store.get("a/b/c.bin", null, sink));
            Assert.assertArrayEquals(data, sink.toByteArray());
            // overwrite
            final String etag2 = store.put("a/b/c.bin", data, 7, 6, null, false);
            Assert.assertEquals(expectedETag(bytes("object")), etag2);
            Assert.assertEquals(etag2, store.get("a/b/c.bin", null, sink));
            Assert.assertEquals("object", utf8(sink));
            // empty object
            Assert.assertEquals("e3b0c44298fc1c149afbf4c8996fb924", store.put("empty", data, 3, 0, null, false));
            Assert.assertEquals("e3b0c44298fc1c149afbf4c8996fb924", store.get("empty", null, sink));
            Assert.assertEquals(0, sink.size());
            try (Stream<Path> files = Files.list(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME))) {
                Assert.assertEquals(0, files.count());
            }
            Assert.assertThrows(IndexOutOfBoundsException.class, () -> store.put("k", data, 5, data.length, null, false));
            Assert.assertThrows(IndexOutOfBoundsException.class, () -> store.put("k", data, -1, 1, null, false));
            Assert.assertFalse(Files.exists(root.resolve("k")));
        }
    }

    @Test
    public void testPutReportsOriginalErrorWhenTempFileCannotBeDeleted() throws Exception {
        configuration.setRetryAttempts(0);
        final Path fifo = root.resolve("fifo");
        boolean hasFifo;
        try {
            hasFifo = new ProcessBuilder("mkfifo", fifo.toString()).start().waitFor() == 0 && Files.exists(fifo);
        } catch (IOException e) {
            hasFifo = false;
        }
        Assume.assumeTrue("mkfifo is not available", hasFifo);
        final Path scratch = Files.createDirectories(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME));
        Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("r-xr-xr-x"));
        final boolean isWritable = Files.isWritable(scratch);
        Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
        Assume.assumeFalse("running as a user that ignores permissions", isWritable);

        try (FsObjectStore store = newStore()) {
            // If-Match makes the store open the target to hash it, and opening a FIFO for reading
            // blocks until a writer opens it too: the put holds its temporary file meanwhile
            final AtomicReference<Throwable> putFailure = new AtomicReference<>();
            final Thread putter = new Thread(() -> {
                try {
                    store.put("fifo", bytes("new"), 0, 3, "0123456789abcdef0123456789abcdef", false);
                } catch (Throwable e) {
                    putFailure.set(e);
                }
            });
            putter.setDaemon(true);
            putter.start();
            try {
                final Path tempFile = awaitSingleFile(scratch);
                Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("r-xr-xr-x"));
                // opening the FIFO for writing returns once the put has opened it for reading; the put
                // then fails to read the FIFO and fails to delete its temporary file as well
                final AtomicReference<Throwable> openFailure = new AtomicReference<>();
                final Thread opener = new Thread(() -> {
                    try {
                        new FileOutputStream(fifo.toFile()).close();
                    } catch (Throwable e) {
                        openFailure.set(e);
                    }
                });
                opener.setDaemon(true);
                opener.start();
                opener.join(30_000);
                putter.join(30_000);
                Assert.assertFalse("the put did not open the FIFO", opener.isAlive() || putter.isAlive());
                Assert.assertNull(openFailure.get());

                // the error is the read failure, not the failure to delete the temporary file
                Assert.assertTrue(String.valueOf(putFailure.get()), putFailure.get() instanceof ObjectStoreException);
                final ObjectStoreException e = (ObjectStoreException) putFailure.get();
                Assert.assertEquals(ObjectStoreException.Kind.RETRYABLE, e.getKind());
                Assert.assertEquals("object store I/O failed [op=PUT, key=fifo, attempts=1, error=java.io.IOException: Illegal seek]", e.getMessage());
                Assert.assertEquals(IOException.class, e.getCause().getClass());
                // the temporary file stays behind, and the target is untouched
                Assert.assertEquals("new", Files.readString(tempFile));
                Assert.assertTrue(Files.exists(fifo));
                Assert.assertFalse(Files.isRegularFile(fifo));
            } finally {
                Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
        }
    }

    @Test
    public void testPutSucceedsWhenDirectorySyncFails() throws Exception {
        try (FsObjectStore store = newStore()) {
            store.put("d/x", bytes("x"), 0, 1, null, false);
            final Path dir = root.resolve("d");
            // write and search permission but no read: files can move into the directory, but the
            // directory cannot be opened to sync it, which the store does as a best effort only
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("-wx------"));
            try {
                Assume.assumeFalse("running as a user that ignores permissions", Files.isReadable(dir));
                final String etag = store.put("d/k", bytes("v1"), 0, 2, null, false);
                Assert.assertEquals(expectedETag(bytes("v1")), etag);
                Assert.assertEquals(expectedETag(bytes("v2")), store.put("d/k", bytes("v2"), 0, 2, etag, false));
                Assert.assertEquals(expectedETag(bytes("v3")), store.put("d/n", bytes("v3"), 0, 2, null, true));
                Assert.assertEquals("v2", Files.readString(dir.resolve("k")));
                Assert.assertEquals("v3", Files.readString(dir.resolve("n")));
            } finally {
                Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
            Assert.assertEquals(List.of("d/k:2", "d/n:2", "d/x:1"), list(store, "", null));
            try (Stream<Path> files = Files.list(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME))) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testTempFileNameTakenByAnotherWriterIsKept() throws Exception {
        // Another writer sharing atomic_write_dir holds the name of the next temporary file. The put
        // fails to create its file under that name, must leave the other writer's file alone, and
        // succeeds on the next attempt under a new name.
        final Path scratch = Files.createDirectories(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME));
        final List<String> retries = new ArrayList<>();
        final AtomicReference<Path> foreign = new AtomicReference<>();
        try (FsObjectStore store = new FsObjectStore(settings("fs::root=" + root + ';'), configuration) {
            @Override
            protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
                retries.add(operation + ' ' + subject + ' ' + attempt + ' ' + error.getClass().getSimpleName());
                if (attempt == 1) {
                    try {
                        // the failed attempt named its temporary file; the next attempt takes the
                        // name after it, which the other writer now holds
                        Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
                        final String name = Path.of(((FileSystemException) error).getFile()).getFileName().toString();
                        final String prefix = name.substring(0, name.lastIndexOf('-') + 1);
                        final long n = Long.parseLong(name.substring(prefix.length(), name.length() - ".tmp".length()));
                        foreign.set(Files.writeString(scratch.resolve(prefix + (n + 1) + ".tmp"), "foreign"));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        }) {
            Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                Assume.assumeFalse("running as a user that ignores permissions", Files.isWritable(scratch));
                Assert.assertEquals(expectedETag(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            } finally {
                Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
            Assert.assertEquals(List.of("PUT key=k 1 AccessDeniedException", "PUT key=k 2 FileAlreadyExistsException"), retries);
            Assert.assertEquals("v", Files.readString(root.resolve("k")));
            // the other writer's file is intact, and it is the only file left
            Assert.assertTrue("another writer's temporary file was deleted: " + foreign.get(), Files.exists(foreign.get()));
            Assert.assertEquals("foreign", Files.readString(foreign.get()));
            try (Stream<Path> files = Files.list(scratch)) {
                Assert.assertEquals(List.of(foreign.get()), files.toList());
            }
        }
    }

    @Test
    public void testTempFileNameTakenReportsOriginalError() throws Exception {
        // A non-empty directory holds the name of the next temporary file: creating the file fails.
        // The store reports that failure, leaves what holds the name alone, and retries.
        final Path scratch = Files.createDirectories(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME));
        final List<String> retries = new ArrayList<>();
        final List<Path> blockers = new ArrayList<>();
        try (FsObjectStore store = new FsObjectStore(settings("fs::root=" + root + ';'), configuration) {
            @Override
            protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
                retries.add(operation + ' ' + subject + ' ' + attempt + ' ' + error.getClass().getSimpleName());
                try {
                    final Path file = Path.of(((FileSystemException) error).getFile());
                    if (attempt == 1) {
                        // the failed attempt named its temporary file, put-<pid>-<nonce>-<n>.tmp; the next
                        // attempts take the names after it
                        Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
                        final String name = file.getFileName().toString();
                        final String prefix = name.substring(0, name.lastIndexOf('-') + 1);
                        final long n = Long.parseLong(name.substring(prefix.length(), name.length() - ".tmp".length()));
                        for (long i = n + 1; i <= n + 16; i++) {
                            final Path blocker = Files.createDirectory(scratch.resolve(prefix + i + ".tmp"));
                            Files.writeString(blocker.resolve("keep"), "k");
                            blockers.add(blocker);
                        }
                    } else {
                        // the error names a blocker, which is still there
                        Assert.assertTrue(file.toString(), blockers.contains(file));
                        Assert.assertEquals("k", Files.readString(file.resolve("keep")));
                        for (Path blocker : blockers) {
                            Files.delete(blocker.resolve("keep"));
                            Files.delete(blocker);
                        }
                    }
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }) {
            Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                Assume.assumeFalse("running as a user that ignores permissions", Files.isWritable(scratch));
                Assert.assertEquals(expectedETag(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            } finally {
                Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
            Assert.assertEquals(List.of("PUT key=k 1 AccessDeniedException", "PUT key=k 2 FileAlreadyExistsException"), retries);
            Assert.assertEquals("v", Files.readString(root.resolve("k")));
            try (Stream<Path> files = Files.list(scratch)) {
                Assert.assertEquals(0, files.count());
            }
        }
    }

    @Test
    public void testTempFileNameUniqueAcrossProcesses() throws Exception {
        // Two nodes sharing atomic_write_dir, or a restarted process, can have the same pid and the
        // same temporary file counter. A name made of the pid and the counter alone collides with the
        // other process's temporary file; the name must carry a part of its own.
        final Path scratch = Files.createDirectories(root.resolve(FsObjectStore.DEFAULT_ATOMIC_WRITE_DIR_NAME));
        final long pid = ProcessHandle.current().pid();
        final List<String> retries = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        final AtomicReference<Path> foreign = new AtomicReference<>();
        try (FsObjectStore store = new FsObjectStore(settings("fs::root=" + root + ';'), configuration) {
            @Override
            protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
                retries.add(operation + ' ' + subject + ' ' + attempt + ' ' + error.getClass().getSimpleName());
                final String name = Path.of(((FileSystemException) error).getFile()).getFileName().toString();
                names.add(name);
                if (attempt == 1) {
                    try {
                        // the other process, with this pid, holds the temporary file whose counter is
                        // the one this process uses next
                        Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
                        final long n = Long.parseLong(name.substring(name.lastIndexOf('-') + 1, name.length() - ".tmp".length()));
                        foreign.set(Files.writeString(scratch.resolve("put-" + pid + '-' + (n + 1) + ".tmp"), "foreign"));
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        }) {
            Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("r-xr-xr-x"));
            try {
                Assume.assumeFalse("running as a user that ignores permissions", Files.isWritable(scratch));
                Assert.assertEquals(expectedETag(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            } finally {
                Files.setPosixFilePermissions(scratch, PosixFilePermissions.fromString("rwxr-xr-x"));
            }
            // the second attempt does not collide with the other process's file
            Assert.assertEquals(List.of("PUT key=k 1 AccessDeniedException"), retries);
            Assert.assertTrue(names.get(0), names.get(0).matches("put-" + pid + "-[0-9a-f]{16}-[0-9]+\\.tmp"));
            Assert.assertEquals("v", Files.readString(root.resolve("k")));
            Assert.assertTrue("another process's temporary file was deleted: " + foreign.get(), Files.exists(foreign.get()));
            Assert.assertEquals("foreign", Files.readString(foreign.get()));
            try (Stream<Path> files = Files.list(scratch)) {
                Assert.assertEquals(List.of(foreign.get()), files.toList());
            }
        }
    }

    @Test
    public void testWrongScheme() {
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "object store settings are not for the filesystem store [scheme=s3]",
                () -> new FsObjectStore(ObjectStoreSettings.parse("s3::bucket=b;"), configuration)
        );
    }

    static void assertOneWinner(ObjectStore store, String key, String[] results, String data1, String data2) {
        final boolean isFirstWinner = results[0] != null && results[0].startsWith("etag:");
        final boolean isSecondWinner = results[1] != null && results[1].startsWith("etag:");
        Assert.assertTrue(key + ' ' + Arrays.toString(results), isFirstWinner ^ isSecondWinner);
        Assert.assertEquals("PRECONDITION_FAILED", isFirstWinner ? results[1] : results[0]);
        final ByteSink sink = new ByteSink();
        final String etag = store.get(key, null, sink);
        Assert.assertEquals(isFirstWinner ? data1 : data2, utf8(sink));
        Assert.assertEquals("etag:" + etag, isFirstWinner ? results[0] : results[1]);
    }

    static List<String> list(ObjectStore store, String prefix, String startAfter) {
        final List<String> keys = new ArrayList<>();
        store.list(prefix, startAfter, (key, size) -> {
            keys.add(key + ':' + size);
            return true;
        });
        return keys;
    }

    /**
     * Runs two puts that start together; each slot ends as "etag:..." or the failure kind.
     */
    static String[] race(java.util.function.Supplier<String> first, java.util.function.Supplier<String> second) throws InterruptedException {
        final String[] results = new String[2];
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread[] threads = new Thread[2];
        for (int i = 0; i < 2; i++) {
            final int slot = i;
            final java.util.function.Supplier<String> action = i == 0 ? first : second;
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    results[slot] = "etag:" + action.get();
                } catch (ObjectStoreException e) {
                    results[slot] = e.getKind().name();
                } catch (Exception e) {
                    results[slot] = e.toString();
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        return results;
    }

    // Waits until the directory holds exactly one entry, one that another thread creates and keeps
    // while it is blocked; the wait ends on that state, whatever the thread scheduling.
    private static Path awaitSingleFile(Path dir) throws IOException {
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (true) {
            try (Stream<Path> files = Files.list(dir)) {
                final List<Path> found = files.toList();
                if (found.size() == 1) {
                    return found.get(0);
                }
                Assert.assertTrue(found.toString(), found.isEmpty());
            }
            Assert.assertTrue("no file appeared in " + dir, System.nanoTime() < deadline);
            LockSupport.parkNanos(1_000_000);
        }
    }

    private static int indexOf(String[] keys, String key) {
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(key)) {
                return i;
            }
        }
        return -1;
    }

    private FsObjectStore newStore() {
        return newStore("fs::root=" + root + ';');
    }

    private FsObjectStore newStore(String settings) {
        return new FsObjectStore(settings(settings), configuration);
    }

    private ObjectStoreSettings settings(String settings) {
        return ObjectStoreSettings.parse(settings);
    }
}
