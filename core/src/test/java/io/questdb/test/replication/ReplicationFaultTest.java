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

import io.questdb.Bootstrap;
import io.questdb.PropertyKey;
import io.questdb.replication.IndexClient;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.ReplicationConfiguration;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.store.FsObjectStore;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreSettings;
import io.questdb.std.Os;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Object store failures at each step of replication, and recovery from them.
 */
public class ReplicationFaultTest extends AbstractReplicationTest {

    @Test
    public void testCheckpointHistoryAndCleanerFailuresAreRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(
                    p, "primary",
                    PropertyKey.REPLICATION_PRIMARY_SEQUENCER_PART_TXN_COUNT.getPropertyPath() + "=2",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_INTERVAL.getPropertyPath() + "=1s",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_BACKUP_WINDOW_COUNT.getPropertyPath() + "=1",
                    PropertyKey.CHECKPOINT_HISTORY_LONG_RETRY_INTERVAL.getPropertyPath() + "=200"
            );
            final AtomicInteger historyFailures = new AtomicInteger(1);
            final AtomicInteger listFailures = new AtomicInteger(1);
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.startsWith("_checkpoints/") && historyFailures.getAndDecrement() > 0) {
                        throw ObjectStoreException.fatal("injected checkpoint history failure");
                    }
                    if (op.equals("list") && key.startsWith("_checkpoints/") && listFailures.getAndDecrement() > 0) {
                        throw ObjectStoreException.fatal("injected cleaner failure");
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 0; i < 6; i++) {
                    primary.execute("INSERT INTO t VALUES ('2024-01-0" + (i + 1) + "', " + i + ')');
                }
                awaitApplied(primary, "t");
                primary.execute("CHECKPOINT CREATE");
                primary.execute("CHECKPOINT RELEASE");
                // both failed once, both succeed on a retry: the history lands and the cleaner frees parts
                TestUtils.assertEventually(() -> Assert.assertTrue(java.nio.file.Files.exists(
                        java.nio.file.Paths.get(storeRoot, "_checkpoints", primary.getReplicationService().getInstanceId()))), 30);
                TestUtils.assertEventually(() -> Assert.assertTrue(historyFailures.get() < 0 && listFailures.get() < 0), 30);
                final String key = primary.getEngine().getTableTokenIfExists("t").getTableId() + "_";
                // the checkpoint at txn 6 frees all three 2-txn parts; the next upload starts part 3
                TestUtils.assertEventually(() -> Assert.assertEquals(0, seqParts(key)), 30);
                primary.execute("INSERT INTO t VALUES ('2024-01-09', 9)");
                TestUtils.assertEventually(() -> Assert.assertEquals(1, seqParts(key)), 30);
            }
        });
    }

    @Test
    public void testCoordinatorSurvivesAnUnexpectedFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            // not a store error: a failure nobody foresaw, thrown where only the coordinator runs
            final AtomicInteger bugs = new AtomicInteger();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && bugs.getAndDecrement() > 0) {
                        throw new IllegalStateException("injected unexpected failure");
                    }
                }
            }); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                bugs.set(2);
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                TestUtils.assertEventually(() -> Assert.assertTrue(bugs.get() < 0), 30);
                // the coordinator is still there: the insert goes up, the lease is renewed, later writes replicate
                awaitUploaded(primary, "t");
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
                TestUtils.assertContains(primary.getReplicationService().getStatus(), "primary, epoch");
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitSame(primary, replica, "t");
                replica.assertSql("SELECT count() n FROM t", "n\n2\n");
            }
        });
    }

    @Test
    public void testDeletedIndexFencesPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                // somebody wipes the store under a running primary
                java.nio.file.Files.delete(java.nio.file.Paths.get(storeRoot, "_index"));
                awaitRole(primary, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason(),
                        "the replication index is gone from the object store");
                try {
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                    Assert.fail("a fenced primary must refuse writes");
                } catch (Throwable e) {
                    TestUtils.assertContains(String.valueOf(e.getMessage()) + e.getCause(), io.questdb.cairo.CairoException.READ_ONLY_ACCESS_MESSAGE);
                }
            }
        });
    }

    @Test
    public void testFailedCheckpointHistoryUploadIsNotRecordedTwice() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(
                    p, "primary",
                    PropertyKey.REPLICATION_PRIMARY_SEQUENCER_PART_TXN_COUNT.getPropertyPath() + "=2",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_INTERVAL.getPropertyPath() + "=1s",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_BACKUP_WINDOW_COUNT.getPropertyPath() + "=2",
                    PropertyKey.CHECKPOINT_HISTORY_LONG_RETRY_INTERVAL.getPropertyPath() + "=200"
            );
            // the history upload fails twice before it reaches the store, then once after it landed
            final AtomicInteger failuresBefore = new AtomicInteger();
            final AtomicInteger failuresAfter = new AtomicInteger();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void after(String op, String key) {
                    if (op.equals("put") && key.startsWith("_checkpoints/") && failuresBefore.get() < 0 && failuresAfter.getAndDecrement() > 0) {
                        throw ObjectStoreException.retryable("injected lost response", null);
                    }
                }

                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.startsWith("_checkpoints/") && failuresBefore.getAndDecrement() > 0) {
                        throw ObjectStoreException.fatal("injected checkpoint history failure");
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 1; i <= 4; i++) {
                    primary.execute("INSERT INTO t VALUES ('2024-01-0" + i + "', " + i + ')');
                }
                awaitUploaded(primary, "t");
                final String key = primary.getEngine().getTableTokenIfExists("t").getTableId() + "_";
                final String instanceId = primary.getReplicationService().getInstanceId();
                Assert.assertEquals(2, seqParts(key));

                failuresBefore.set(2);
                failuresAfter.set(1);
                primary.execute("CHECKPOINT CREATE");
                primary.execute("CHECKPOINT RELEASE");
                TestUtils.assertEventually(() -> Assert.assertTrue(failuresBefore.get() < 0 && failuresAfter.get() < 0), 30);
                // one checkpoint, however many attempts it took to store it
                TestUtils.assertEventually(() -> Assert.assertEquals(1, readCheckpointHistory(instanceId).getCheckpoints().size()), 30);

                // the cleaner waits for a second checkpoint: with it, txns 1..4 of the older one go
                for (int i = 5; i <= 8; i++) {
                    primary.execute("INSERT INTO t VALUES ('2024-01-0" + i + "', " + i + ')');
                }
                awaitUploaded(primary, "t");
                Assert.assertEquals(4, seqParts(key));
                primary.execute("CHECKPOINT CREATE");
                primary.execute("CHECKPOINT RELEASE");
                TestUtils.assertEventually(() -> Assert.assertEquals(2, readCheckpointHistory(instanceId).getCheckpoints().size()), 30);
                TestUtils.assertEventually(() -> Assert.assertEquals(2, seqParts(key)), 30);
            }
        });
    }

    @Test
    public void testFailedCleanerDeletesAreRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(
                    p, "primary",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_INTERVAL.getPropertyPath() + "=1s",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_DROPPED_TABLE_COOLOFF.getPropertyPath() + "=0"
            );
            // the store refuses the first three deletes
            final AtomicInteger deleteFailures = new AtomicInteger();
            final java.util.Set<String> refused = java.util.concurrent.ConcurrentHashMap.newKeySet();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("delete") && key.startsWith("t/") && deleteFailures.getAndDecrement() > 0) {
                        refused.add(key);
                        throw ObjectStoreException.fatal("injected delete failure");
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO gone VALUES ('2024-01-01', 1)");
                awaitUploaded(primary, "gone");
                final String key = primary.getEngine().getTableTokenIfExists("gone").getTableId() + "_";
                Assert.assertTrue(tableObjects(key) >= 3);

                deleteFailures.set(3);
                primary.execute("DROP TABLE gone");
                TestUtils.assertEventually(() -> Assert.assertTrue(deleteFailures.get() < 0), 30);
                Assert.assertEquals(3, refused.size());
                // by now no index names the table: only the cleaner's own memory of the refused keys
                // brings a later run back to them
                TestUtils.assertEventually(() -> Assert.assertEquals(0, tableObjects(key)), 30);
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
            }
        });
    }

    @Test
    public void testFailedDrainAtShutdownIsUploadedAfterRestart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(r, "replica");
            final AtomicBoolean isStoreDown = new AtomicBoolean();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (isStoreDown.get()) {
                        throw ObjectStoreException.fatal("injected store outage");
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                // waits for the 10-minute window, then the store goes away before the graceful stop
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                awaitApplied(primary, "t");
                final io.questdb.cairo.TableToken t = primary.getEngine().verifyTableName("t");
                Assert.assertEquals(0, primary.getReplicationService().getUploadedTxn(t));
                isStoreDown.set(true);
            }
            // the stop could neither drain nor release the lease; nothing is lost: the next start uploads it
            writeConfiguration(p, "primary");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                awaitUploaded(primary, "t");
                awaitSame(primary, replica, "t");
                replica.assertSql("SELECT count() n FROM t", "n\n2\n");
            }
        });
    }

    @Test
    public void testConflictReportedForOwnIndexCommitKeepsPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            final AtomicInteger conflicts = new AtomicInteger();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void after(String op, String key) {
                    // the write landed, its answer got lost, and the store client's own retry met the
                    // new version: the caller sees a lost comparison
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && conflicts.getAndDecrement() > 0) {
                        throw ObjectStoreException.preconditionFailed(key);
                    }
                }
            }); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                conflicts.set(3);
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                TestUtils.assertEventually(() -> Assert.assertTrue(conflicts.get() < 0), 30);
                awaitUploaded(primary, "t");
                awaitSame(primary, replica, "t");
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
                Assert.assertNull(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason());
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                awaitSame(primary, replica, "t");
                replica.assertSql("SELECT count() n FROM t", "n\n3\n");
            }
        });
    }

    @Test
    public void testFailuresWhileTakingTheLeaseDoNotFence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(nodeA, "t");
            }
            // a released the lease. b takes it: its first index write fails outright, twice; on the third
            // attempt another node commits first, so b's write loses the comparison
            writeConfiguration(b, "primary");
            allowPromotion(b);
            final AtomicInteger failures = new AtomicInteger(2);
            final AtomicBoolean isRaceArmed = new AtomicBoolean(true);
            try (TestServerMain nodeB = startFaulty(b, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY)) {
                        if (failures.getAndDecrement() > 0) {
                            throw ObjectStoreException.retryable("injected timeout", null);
                        }
                        if (isRaceArmed.compareAndSet(true, false)) {
                            writeForeignIndex();
                        }
                    }
                }
            })) {
                // b never held the lease, so none of this fences it: it waits out the other node's
                // lease, which nobody renews, and becomes primary
                TestUtils.assertEventually(() -> {
                    Assert.assertTrue(failures.get() < 0);
                    Assert.assertFalse(isRaceArmed.get());
                }, 30);
                Assert.assertNotEquals(EngineRole.FENCED, role(nodeB));
                awaitRole(nodeB, EngineRole.PRIMARY);
                Assert.assertNull(((io.questdb.replication.ReplicatedCairoEngine) nodeB.getEngine()).getFenceReason());
                awaitApplied(nodeB, "t");
                nodeB.assertSql("SELECT count() n FROM t", "n\n1\n");
                nodeB.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitUploaded(nodeB, "t");
            }
        });
    }

    @Test
    public void testIndexCommitErrorWhileAnotherPrimaryTookOverFences() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary");
            final AtomicBoolean isArmed = new AtomicBoolean();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && isArmed.compareAndSet(true, false)) {
                        // another primary commits first, then this request fails without an answer
                        writeForeignIndex();
                        throw ObjectStoreException.retryable("injected lost response", null);
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                isArmed.set(true);
                awaitRole(primary, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason(), "lost its lease");
            }
        });
    }

    @Test
    public void testIndexCommitResponseLostOrRequestFailed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            final AtomicInteger landedButLost = new AtomicInteger();
            final AtomicInteger failedBefore = new AtomicInteger();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void after(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && landedButLost.getAndDecrement() > 0) {
                        throw ObjectStoreException.retryable("injected lost response", null);
                    }
                }

                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && failedBefore.getAndDecrement() > 0) {
                        throw ObjectStoreException.retryable("injected request failure", null);
                    }
                }
            }); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                landedButLost.set(1);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                TestUtils.assertEventually(() -> Assert.assertTrue(landedButLost.get() < 0), 30);
                failedBefore.set(2);
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                TestUtils.assertEventually(() -> Assert.assertTrue(failedBefore.get() < 0), 30);
                awaitSame(primary, replica, "t");
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
            }
        });
    }

    @Test
    public void testPrimaryIsReadOnlyWhileItsLeaseIsLapsedAndResumes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            // uploads wait for a ten-minute window, so the insert below is still local when the store goes
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(r, "replica");
            final AtomicBoolean isStoreDown = new AtomicBoolean();
            // when the store last let an index write through
            final AtomicLong lastIndexWriteNanos = new AtomicLong();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (isStoreDown.get()) {
                        throw ObjectStoreException.retryable("injected store outage", null);
                    }
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY)) {
                        lastIndexWriteNanos.set(System.nanoTime());
                    }
                }
            }); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(primary, "t");

                // the store goes away. Writes are accepted while the lease is current, and refused once it
                // is two and a half keepalive intervals old: another node may take over half an interval later
                isStoreDown.set(true);
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                final io.questdb.replication.ReplicatedCairoEngine engine = (io.questdb.replication.ReplicatedCairoEngine) primary.getEngine();
                // The lease counts from the moment the node sent its last accepted index write, a moment
                // before the store saw it, and lasts 2.5 keepalive intervals: 2.5 s here. That is less
                // than the 3 s after which another node may take over, which is what keeps two primaries
                // from accepting writes at once. The store saw that write at lastIndexWriteNanos, so the
                // lease ends no later than 2.5 s after it, and only the moment between sending and seeing
                // earlier.
                // An index write the store let through just before the outage may still be on its way
                // back: until the node has counted it, the two values belong to different writes.
                final AtomicLong lapse = new AtomicLong();
                TestUtils.assertEventually(() -> {
                    final long until = engine.getLeaseValidUntilNanos();
                    final long leaseMillis = TimeUnit.NANOSECONDS.toMillis(until - lastIndexWriteNanos.get());
                    Assert.assertTrue("the lease lasts " + leaseMillis + " ms", leaseMillis > 2_000 && leaseMillis <= 2_500);
                    lapse.set(until);
                }, 30);
                final long lapseNanos = lapse.get();
                // the node acts on it: writable until that moment, read-only from it
                final EngineRole roleNow = role(primary);
                if (System.nanoTime() - lapseNanos < 0) {
                    Assert.assertEquals(EngineRole.PRIMARY, roleNow);
                }
                TestUtils.assertEventually(() -> Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, role(primary)), 30);
                Assert.assertTrue(System.nanoTime() - lapseNanos >= 0);
                // nothing renewed it during the outage
                Assert.assertEquals(lapseNanos, engine.getLeaseValidUntilNanos());
                Assert.assertTrue(engine.isPrimaryRole());
                Assert.assertTrue(engine.isLeaseLapsed());
                Assert.assertNull(engine.getFenceReason());
                try {
                    primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                    Assert.fail("a primary with a lapsed lease must refuse writes");
                } catch (Throwable e) {
                    TestUtils.assertContains(String.valueOf(e.getMessage()) + e.getCause(), io.questdb.cairo.CairoException.READ_ONLY_ACCESS_MESSAGE);
                }
                TestUtils.assertEventually(() -> TestUtils.assertContains(primary.getReplicationService().getStatus(), "lease lapsed"), 30);
                primary.assertSql("SELECT count() n FROM t", "n\n2\n");

                // the store is back and nobody took it: the node renews its lease and carries on by itself
                isStoreDown.set(false);
                awaitRole(primary, EngineRole.PRIMARY);
                Assert.assertNull(engine.getFenceReason());
                TestUtils.assertEventually(() -> TestUtils.assertContains(primary.getReplicationService().getStatus(), "primary, epoch"), 30);
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
            }
            // the stop uploaded what the outage and the window had kept local
            Assert.assertEquals(3, readIndex().getTables().getQuick(0).getLastTxn());
            Assert.assertTrue(readIndex().isReleased());
        });
    }

    @Test
    public void testLeaseKeeperFencesAPrimaryDeposedDuringALongRound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary");
            final AtomicBoolean isBlocking = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final long foreignSequence;
            try (TestServerMain primary = startFaulty(p, blockingUploads(isBlocking, blocked, release))) {
                try {
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    awaitUploaded(primary, "t");
                    isBlocking.set(true);
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                    Assert.assertTrue("no upload started", blocked.await(30, TimeUnit.SECONDS));

                    // another node takes the store over while this one is stuck in its upload
                    writeForeignIndex();
                    foreignSequence = readIndex().getSequence();
                    // the coordinator cannot notice: it waits for the upload. The keeper's renewal does.
                    awaitRole(primary, EngineRole.FENCED);
                    Assert.assertEquals("the upload ended", 1, release.getCount());
                    TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason(), "lost its lease");
                    try {
                        primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                        Assert.fail("a fenced primary must refuse writes");
                    } catch (Throwable e) {
                        TestUtils.assertContains(String.valueOf(e.getMessage()) + e.getCause(), io.questdb.cairo.CairoException.READ_ONLY_ACCESS_MESSAGE);
                    }
                } finally {
                    release.countDown();
                }
            }
            // the upload that was in flight ended after the takeover: the index is as the other node left it
            final ReplicationIndex index = readIndex();
            Assert.assertEquals("another-primary", index.getOwnerInstanceId());
            Assert.assertEquals(foreignSequence, index.getSequence());
        });
    }

    @Test
    public void testLeaseKeeperRenewsThroughTheShutdownDrain() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary");
            final AtomicBoolean isBlocking = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final TestServerMain primary = startFaulty(p, blockingUploads(isBlocking, blocked, release));
            final Thread closer = new Thread(primary::close, "test-closer");
            try {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 0)");
                awaitUploaded(primary, "t");
                TestUtils.assertEventually(() -> Assert.assertEquals(1, lastTxn(readIndex(), "t")), 30);
                isBlocking.set(true);
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 1)");
                Assert.assertTrue("no upload started", blocked.await(30, TimeUnit.SECONDS));

                // the node is stopped while that upload is in flight: the stop waits for it
                closer.start();
                Os.sleep(500);
                final long sequenceAtStop = readIndex().getSequence();
                // Another node may be waiting for this lease, and takes it after three keepalive intervals
                // without an index write. The stop takes four here, and the lease is renewed throughout.
                final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(4_000);
                long previous = sequenceAtStop;
                long previousNanos = System.nanoTime();
                long longestGapNanos = 0;
                while (System.nanoTime() < deadline) {
                    final ReplicationIndex index = readIndex();
                    Assert.assertFalse(index.isReleased());
                    if (index.getSequence() != previous) {
                        previous = index.getSequence();
                        previousNanos = System.nanoTime();
                    }
                    longestGapNanos = Math.max(longestGapNanos, System.nanoTime() - previousNanos);
                    Os.sleep(20);
                }
                Assert.assertTrue("sequence " + sequenceAtStop + " -> " + previous, previous >= sequenceAtStop + 2);
                Assert.assertTrue("no index write for " + TimeUnit.NANOSECONDS.toMillis(longestGapNanos) + " ms", longestGapNanos < TimeUnit.MILLISECONDS.toNanos(3_000));
                Assert.assertEquals(1, lastTxn(readIndex(), "t"));
                Assert.assertTrue(closer.isAlive());
            } finally {
                release.countDown();
                if (closer.isAlive() || closer.getState() != Thread.State.NEW) {
                    closer.join(60_000);
                }
                primary.close();
            }
            // the stop then uploaded the rest and released the lease
            final ReplicationIndex released = readIndex();
            Assert.assertTrue(released.isReleased());
            Assert.assertEquals(2, lastTxn(released, "t"));
        });
    }

    @Test
    public void testSlowIndexWritesLeaveRoomForHousekeeping() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_PRIMARY_CLEANER_INTERVAL.getPropertyPath() + "=1s");
            final AtomicBoolean isSlow = new AtomicBoolean();
            final AtomicInteger cleanerRuns = new AtomicInteger();
            final AtomicInteger indexWrites = new AtomicInteger();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (!isSlow.get()) {
                        return;
                    }
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY)) {
                        // the store takes longer than a keepalive interval to accept an index
                        indexWrites.incrementAndGet();
                        Os.sleep(1_100);
                    } else if (op.equals("list") && key.startsWith("_checkpoints/")) {
                        cleanerRuns.incrementAndGet();
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 0)");
                awaitUploaded(primary, "t");
                isSlow.set(true);
                // Every index write is due again by the time it returns, if the next is counted from when
                // it was sent. The coordinator counts from when it returned, so it still gets to its
                // housekeeping; the lease keeper covers the lease.
                TestUtils.assertEventually(() -> Assert.assertTrue("cleaner runs: " + cleanerRuns.get(), cleanerRuns.get() >= 3), 60);
                Assert.assertTrue("index writes: " + indexWrites.get(), indexWrites.get() >= 3);
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
                isSlow.set(false);
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 1)");
                awaitUploaded(primary, "t");
            }
        });
    }

    @Test
    public void testSlowIndexWritesDoNotLetTheLeaseLapse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary");
            final AtomicBoolean isBlocking = new AtomicBoolean();
            final AtomicBoolean isSlow = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final FaultyObjectStore.Hook uploads = blockingUploads(isBlocking, blocked, release);
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (isSlow.get() && op.equals("put") && key.equals(IndexClient.INDEX_KEY)) {
                        // the store takes 0.6 of a keepalive interval to accept an index
                        Os.sleep(600);
                    }
                    uploads.before(op, key);
                }
            })) {
                try {
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 0)");
                    awaitUploaded(primary, "t");
                    TestUtils.assertEventually(() -> Assert.assertEquals(1, lastTxn(readIndex(), "t")), 30);
                    final long sequenceBefore = readIndex().getSequence();
                    isSlow.set(true);
                    isBlocking.set(true);
                    primary.execute("INSERT INTO t VALUES ('2024-01-02', 1)");
                    Assert.assertTrue("no upload started", blocked.await(30, TimeUnit.SECONDS));

                    // The lease runs from when a write was sent, so the next renewal is due from then too.
                    // Counted from when the slow write came back, it would start too late to land in time.
                    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(6_000);
                    while (System.nanoTime() < deadline) {
                        Assert.assertEquals(EngineRole.PRIMARY, role(primary));
                        Os.sleep(10);
                    }
                    Assert.assertEquals("the upload ended", 1, release.getCount());
                    Assert.assertTrue(readIndex().getSequence() >= sequenceBefore + 3);
                } finally {
                    isSlow.set(false);
                    release.countDown();
                }
                awaitUploaded(primary, "t");
            }
        });
    }

    @Test
    public void testLeaseLapsesWhenTheCoordinatorIsStuck() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            // an upload counts as alive for as long as its size and the request timeout allow: one second here
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_REQUESTS_BASE_TIMEOUT.getPropertyPath() + "=100");
            final AtomicBoolean isBlocking = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            try (TestServerMain primary = startFaulty(p, blockingUploads(isBlocking, blocked, release))) {
                try {
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 0)");
                    awaitUploaded(primary, "t");
                    TestUtils.assertEventually(() -> Assert.assertEquals(1, lastTxn(readIndex(), "t")), 30);
                    // three keepalive intervals without a sign of life, and the keeper gives the coordinator up
                    primary.getReplicationService().setStallLimitForTesting(3_000);
                    final long sequenceBefore = readIndex().getSequence();
                    isBlocking.set(true);
                    primary.execute("INSERT INTO t VALUES ('2024-01-02', 1)");
                    Assert.assertTrue("no upload started", blocked.await(30, TimeUnit.SECONDS));

                    // the only request in flight never returns. The keeper renews the lease at first, then
                    // stops: a node that replicates nothing must not stay the writable primary for ever
                    TestUtils.assertEventually(() -> Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, role(primary)), 30);
                    final io.questdb.replication.ReplicatedCairoEngine engine = (io.questdb.replication.ReplicatedCairoEngine) primary.getEngine();
                    Assert.assertTrue(engine.isLeaseLapsed());
                    Assert.assertNull(engine.getFenceReason());
                    final long sequenceAtLapse = readIndex().getSequence();
                    Assert.assertTrue("sequence " + sequenceBefore + " -> " + sequenceAtLapse, sequenceAtLapse > sequenceBefore);
                    // and it stays that way: no renewal for two more keepalive intervals
                    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(2_000);
                    while (System.nanoTime() < deadline) {
                        Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, role(primary));
                        Os.sleep(20);
                    }
                    Assert.assertEquals(sequenceAtLapse, readIndex().getSequence());
                    Assert.assertEquals("the upload ended", 1, release.getCount());
                    try {
                        primary.execute("INSERT INTO t VALUES ('2024-01-03', 2)");
                        Assert.fail("a primary with a lapsed lease must refuse writes");
                    } catch (Throwable e) {
                        TestUtils.assertContains(String.valueOf(e.getMessage()) + e.getCause(), io.questdb.cairo.CairoException.READ_ONLY_ACCESS_MESSAGE);
                    }
                } finally {
                    release.countDown();
                }
                // the coordinator comes back, nobody took the store: the node is the primary again
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 2)");
                awaitUploaded(primary, "t");
                Assert.assertEquals(3, lastTxn(readIndex(), "t"));
            }
        });
    }

    @Test
    public void testLongRoundDoesNotMakeAHealthyPrimaryReadOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            final AtomicBoolean isBlocking = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            try (TestServerMain primary = startFaulty(p, blockingUploads(isBlocking, blocked, release))) {
                try {
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 0)");
                    awaitUploaded(primary, "t");
                    TestUtils.assertEventually(() -> Assert.assertEquals(1, lastTxn(readIndex(), "t")), 30);
                    final ReplicationIndex before = readIndex();
                    final long uploadedBefore = lastTxn(before, "t");

                    // an upload that takes four keepalive intervals: longer than the lease of two and a half
                    isBlocking.set(true);
                    primary.execute("INSERT INTO t VALUES ('2024-01-02', 0)");
                    Assert.assertTrue("no upload started", blocked.await(30, TimeUnit.SECONDS));
                    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(4_000);
                    int inserted = 0;
                    while (System.nanoTime() < deadline) {
                        // the store answers, so the node never stops being the writable primary
                        Assert.assertEquals(EngineRole.PRIMARY, role(primary));
                        if (inserted < 8) {
                            primary.execute("INSERT INTO t VALUES ('2024-01-02', " + ++inserted + ")");
                        }
                        Os.sleep(20);
                    }
                    Assert.assertEquals("the upload ended", 1, release.getCount());

                    // the lease was renewed meanwhile, by index writes that carry nothing of the upload in flight
                    final ReplicationIndex during = readIndex();
                    Assert.assertTrue("sequence " + before.getSequence() + " -> " + during.getSequence(), during.getSequence() >= before.getSequence() + 2);
                    Assert.assertEquals(before.getOwnerInstanceId(), during.getOwnerInstanceId());
                    Assert.assertEquals(before.getEpoch(), during.getEpoch());
                    Assert.assertFalse(during.isReleased());
                    Assert.assertEquals(uploadedBefore, lastTxn(during, "t"));
                } finally {
                    release.countDown();
                }
                // the round ends and commits everything, and a replica reads the same rows
                awaitUploaded(primary, "t");
                try (TestServerMain replica = start(r)) {
                    awaitSame(primary, replica, "t");
                    replica.assertSql("SELECT count() n FROM t", "n\n10\n");
                }
            }
        });
    }

    @Test
    public void testPrimaryWithLapsedLeaseTouchesNothingAfterATakeover() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(
                    p, "primary",
                    PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_INTERVAL.getPropertyPath() + "=1s",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_DROPPED_TABLE_COOLOFF.getPropertyPath() + "=0"
            );
            final AtomicBoolean isStoreDown = new AtomicBoolean();
            final AtomicBoolean isRecording = new AtomicBoolean();
            final java.util.List<String> requests = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (isStoreDown.get()) {
                        throw ObjectStoreException.retryable("injected store outage", null);
                    }
                    if (isRecording.get()) {
                        requests.add(op + ' ' + key);
                    }
                }
            })) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                awaitUploaded(primary, "gone");
                // work is waiting when the store goes away: rows to upload, and a dropped table to clean
                isStoreDown.set(true);
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                primary.execute("DROP TABLE gone");
                TestUtils.assertEventually(() -> Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, role(primary)), 30);

                // during the outage another node takes the store over; then the store answers this node again
                writeForeignIndex();
                final long foreignSequence = readIndex().getSequence();
                isRecording.set(true);
                isStoreDown.set(false);
                awaitRole(primary, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason(), "lost its lease");

                // all it did was try to renew its lease, which the store refused: no upload, no deletion
                synchronized (requests) {
                    for (String request : requests) {
                        Assert.assertTrue(request, request.equals("put " + IndexClient.INDEX_KEY) || request.startsWith("get "));
                    }
                    Assert.assertTrue(requests.toString(), requests.contains("put " + IndexClient.INDEX_KEY));
                }
                final io.questdb.replication.codec.ReplicationIndex index = readIndex();
                Assert.assertEquals("another-primary", index.getOwnerInstanceId());
                Assert.assertEquals(foreignSequence, index.getSequence());
                try {
                    primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                    Assert.fail("a fenced primary must refuse writes");
                } catch (Throwable e) {
                    TestUtils.assertContains(String.valueOf(e.getMessage()) + e.getCause(), io.questdb.cairo.CairoException.READ_ONLY_ACCESS_MESSAGE);
                }
            }
            // nor did the stop: the store is as the other node left it
            Assert.assertEquals("another-primary", readIndex().getOwnerInstanceId());
            synchronized (requests) {
                for (String request : requests) {
                    Assert.assertTrue(request, request.equals("put " + IndexClient.INDEX_KEY) || request.startsWith("get "));
                }
            }
        });
    }

    @Test
    public void testPrimaryRestartUploadsItsLocalTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(primary, "t");
                // the insert waits for a 10-minute window: the crash leaves it local only
                primary.getReplicationService().crashForTesting();
            }
            writeConfiguration(p, "primary");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                // the lease is its own: no wait; the local tail goes up
                awaitRole(primary, EngineRole.PRIMARY);
                awaitSame(primary, replica, "t");
                replica.assertSql("SELECT count() n FROM t", "n\n1\n");
            }
        });
    }

    @Test
    public void testRestartedPrimaryListsNewTableOnlyWithItsCreateManifest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            // structure changes wait for the window too, and the window is ten minutes: a new table stays local
            final String[] settings = {
                    PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000",
                    PropertyKey.REPLICATION_PRIMARY_THROTTLE_NON_DATA.getPropertyPath() + "=true"
            };
            writeConfiguration(p, "primary", settings);
            writeConfiguration(r, "replica");
            try (TestServerMain replica = start(r)) {
                try (TestServerMain primary = start(p)) {
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                    awaitApplied(primary, "t");
                    Assert.assertEquals(0, readIndex().getTables().size());
                    primary.getReplicationService().crashForTesting();
                }
                // the restarted primary holds a table the store knows nothing about. Taking the lease
                // writes an index: it must not name the table before its create manifest is there, or the
                // replica would look for the manifest, miss it and give the table up
                writeConfiguration(p, "primary", settings);
                try (TestServerMain primary = start(p)) {
                    awaitRole(primary, EngineRole.PRIMARY);
                    final io.questdb.replication.codec.ReplicationIndex index = readIndex();
                    Assert.assertNotNull(index);
                    Assert.assertEquals(primary.getReplicationService().getInstanceId(), index.getOwnerInstanceId());
                    Assert.assertEquals(0, index.getTables().size());
                    // a keepalive commit later it still does not
                    final long sequence = index.getSequence();
                    TestUtils.assertEventually(() -> Assert.assertTrue(readIndex().getSequence() > sequence), 30);
                    Assert.assertEquals(0, readIndex().getTables().size());
                    Assert.assertNull(replica.getEngine().getTableTokenIfExists("t"));
                }
                // the graceful stop uploaded the manifest and the data, and the replica has the table
                Assert.assertEquals(1, readIndex().getTables().size());
                Assert.assertEquals(1, readIndex().getTables().getQuick(0).getLastTxn());
                TestUtils.assertEventually(() -> Assert.assertNotNull(replica.getEngine().getTableTokenIfExists("t")), 30);
                TestUtils.assertEventually(() -> replica.assertSql("SELECT v FROM t", "v\n1\n"), 30);
            }
        });
    }

    @Test
    public void testPrimaryStartsOnceTheStoreAnswers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "t");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(primary, "t");
                primary.getReplicationService().crashForTesting();
            }
            // the restart finds the store down: reads fail outright, then writes are refused
            final AtomicBoolean isStoreDown = new AtomicBoolean(true);
            final AtomicInteger refused = new AtomicInteger();
            writeConfiguration(p, "primary");
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (isStoreDown.get()) {
                        if (refused.incrementAndGet() % 2 == 0) {
                            throw ObjectStoreException.fatal("injected access denied");
                        }
                        throw ObjectStoreException.retryable("injected store outage", null);
                    }
                }
            }); TestServerMain replica = start(r)) {
                // it neither opens writes nor gives up: it stays read-only and says why
                TestUtils.assertEventually(() -> Assert.assertTrue(refused.get() >= 6), 30);
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, role(primary));
                TestUtils.assertContains(primary.getReplicationService().getStatus(), "object store error");
                try {
                    primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                    Assert.fail("a primary that has not taken the lease must refuse writes");
                } catch (Throwable e) {
                    TestUtils.assertContains(String.valueOf(e.getMessage()) + e.getCause(), io.questdb.cairo.CairoException.READ_ONLY_ACCESS_MESSAGE);
                }
                // the store comes back: the node takes its lease and uploads what the crash left local
                isStoreDown.set(false);
                awaitRole(primary, EngineRole.PRIMARY);
                Assert.assertNull(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason());
                awaitUploaded(primary, "t");
                awaitSame(primary, replica, "t");
                replica.assertSql("SELECT v FROM t", "v\n1\n");
            }
        });
    }

    @Test
    public void testSegmentClosedBeforeItsTableIsPickedUpLosesItsMarker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            // nothing uploads before the stop: a new table has no uploader until then
            writeConfiguration(
                    p, "primary",
                    PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000",
                    PropertyKey.REPLICATION_PRIMARY_THROTTLE_NON_DATA.getPropertyPath() + "=true"
            );
            final java.nio.file.Path tableDir;
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(primary, "t");
                final io.questdb.cairo.TableToken token = primary.getEngine().verifyTableName("t");
                tableDir = java.nio.file.Paths.get(primary.getEngine().getConfiguration().getDbRoot(), token.getDirName());
                Assert.assertEquals(1, markers(tableDir));
                // the writer closes its segment and its WAL while the table is still unknown to the uploads
                primary.getEngine().releaseAllWalWriters();
                Assert.assertEquals(-1, primary.getReplicationService().getUploadedTxn(token));
                Assert.assertEquals(1, markers(tableDir));
            }
            // the stop uploaded the table. The close was not forgotten: the marker is gone, and the purge
            // job is free to remove the segment once it is applied
            Assert.assertEquals(1, readIndex().getTables().size());
            Assert.assertEquals(1, readIndex().getTables().getQuick(0).getLastTxn());
            Assert.assertEquals(0, markers(tableDir));
        });
    }

    @Test
    public void testReplicaReadFaultsAreRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            final AtomicInteger indexFailures = new AtomicInteger(3);
            final AtomicInteger deltaFailures = new AtomicInteger(2);
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                awaitApplied(primary, "t");
                try (TestServerMain replica = startFaulty(r, new FaultyObjectStore.Hook() {
                    @Override
                    public void before(String op, String key) {
                        if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && indexFailures.getAndDecrement() > 0) {
                            throw ObjectStoreException.retryable("injected poll failure", null);
                        }
                        if (op.equals("get") && key.contains("/wal/") && deltaFailures.getAndDecrement() > 0) {
                            throw ObjectStoreException.retryable("injected download failure", null);
                        }
                    }
                })) {
                    awaitSame(primary, replica, "t");
                    Assert.assertTrue(indexFailures.get() < 0);
                    Assert.assertTrue(deltaFailures.get() < 0);
                }
            }
        });
    }

    @Test
    public void testUploadFaultsAreRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            final AtomicInteger deltaFailures = new AtomicInteger();
            final AtomicInteger roundFailures = new AtomicInteger();
            try (TestServerMain primary = startFaulty(p, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.contains("/wal/") && deltaFailures.getAndDecrement() > 0) {
                        throw ObjectStoreException.retryable("injected upload failure", null);
                    }
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && roundFailures.getAndDecrement() > 0) {
                        throw ObjectStoreException.fatal("injected fatal index failure");
                    }
                }
            }); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitApplied(primary, "t");
                deltaFailures.set(2);
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1, 'a'), ('2024-01-02', 2, 'b')");
                TestUtils.assertEventually(() -> Assert.assertTrue(deltaFailures.get() < 0), 30);
                roundFailures.set(1);
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'c')");
                TestUtils.assertEventually(() -> Assert.assertTrue(roundFailures.get() < 0), 30);
                awaitSame(primary, replica, "t");
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
            }
        });
    }

    @Test
    public void testViewsAndMaterializedViewsKeepNoUploadMarkers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            // a long window: whatever carries a marker keeps it for the length of the test
            writeConfiguration(p, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE base (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(primary, "base");
                primary.execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, max(v) m FROM base SAMPLE BY 1d");
                primary.execute("CREATE VIEW plain AS SELECT ts, v FROM base");
                primary.execute("INSERT INTO base VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                awaitApplied(primary, "base");
                TestUtils.assertEventually(() -> primary.assertSql("SELECT count() n FROM mv", "n\n2\n"), 30);
                final String dbRoot = primary.getEngine().getConfiguration().getDbRoot();
                // the table's unsent segment is pinned; views are not uploaded, so nothing of theirs is,
                // and the purge job stays free to remove their WAL
                Assert.assertTrue(allMarkers(java.nio.file.Paths.get(dbRoot, primary.getEngine().verifyTableName("base").getDirName())) > 0);
                Assert.assertEquals(0, allMarkers(java.nio.file.Paths.get(dbRoot, primary.getEngine().verifyTableName("mv").getDirName())));
                Assert.assertEquals(0, allMarkers(java.nio.file.Paths.get(dbRoot, primary.getEngine().verifyTableName("plain").getDirName())));
            }
        });
    }

    @Test
    public void testWorkerContextsAreReusedAndFreed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // forty upload rounds and as many replica rounds, one table at a time
                for (int i = 0; i < 40; i++) {
                    primary.execute("INSERT INTO t VALUES (" + (1_704_067_200_000_000L + i * 1_000_000L) + ", " + i + ')');
                    awaitUploaded(primary, "t");
                }
                awaitSame(primary, replica, "t");
                // an idle thread takes the next task: the coordinator's context and one or two more,
                // not one for every round
                final int primaryContexts = primary.getReplicationService().getWorkerContextCount();
                final int replicaContexts = replica.getReplicationService().getWorkerContextCount();
                Assert.assertTrue("primary contexts: " + primaryContexts, primaryContexts >= 1 && primaryContexts <= 3);
                Assert.assertTrue("replica contexts: " + replicaContexts, replicaContexts >= 1 && replicaContexts <= 3);

                // a thread that ends takes its context with it; what is left is the coordinator's own, if
                // it has needed one
                primary.getReplicationService().setWorkerKeepAliveForTesting(20);
                replica.getReplicationService().setWorkerKeepAliveForTesting(20);
                primary.execute("INSERT INTO t VALUES ('2024-02-01', 100)");
                awaitSame(primary, replica, "t");
                TestUtils.assertEventually(() -> {
                    Assert.assertTrue(primary.getReplicationService().getWorkerContextCount() <= 1);
                    Assert.assertTrue(replica.getReplicationService().getWorkerContextCount() <= 1);
                }, 30);

                // and replication carries on with fresh threads
                primary.execute("INSERT INTO t VALUES ('2024-02-02', 101)");
                awaitSame(primary, replica, "t");
                replica.assertSql("SELECT count() n FROM t", "n\n42\n");
            }
        });
    }

    private io.questdb.replication.codec.CheckpointHistory readCheckpointHistory(String instanceId) throws Exception {
        final byte[] stored = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(storeRoot, "_checkpoints", instanceId));
        try (io.questdb.replication.codec.NativeBuffer raw = new io.questdb.replication.codec.NativeBuffer();
             io.questdb.replication.codec.ObjectEnvelope envelope = new io.questdb.replication.codec.ObjectEnvelope()) {
            envelope.decode(stored, stored.length, raw);
            final byte[] text = new byte[(int) raw.size()];
            io.questdb.std.Unsafe.copyMemory(null, raw.ptr(), text, io.questdb.std.Unsafe.BYTE_OFFSET, text.length);
            return io.questdb.replication.codec.CheckpointHistory.decode(text, 0, text.length);
        }
    }

    // the number of upload.pending markers anywhere under a table's directory
    private static long allMarkers(java.nio.file.Path tableDir) throws Exception {
        try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(tableDir)) {
            return files.filter(f -> f.getFileName().toString().equals(io.questdb.replication.ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER)).count();
        }
    }

    // the number of upload.pending markers in the WAL segments of a table
    private static long markers(java.nio.file.Path tableDir) throws Exception {
        try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(tableDir)) {
            return files.filter(f -> f.getFileName().toString().equals(io.questdb.replication.ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER)
                    && f.getParent().getParent().getFileName().toString().startsWith("wal")).count();
        }
    }

    // the number of objects the store holds for a table
    private long tableObjects(String tableKeyPrefix) throws Exception {
        final java.nio.file.Path tablesDir = java.nio.file.Paths.get(storeRoot, "t");
        if (!java.nio.file.Files.exists(tablesDir)) {
            return 0;
        }
        try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(tablesDir)) {
            return files.filter(f -> java.nio.file.Files.isRegularFile(f)
                    && tablesDir.relativize(f).toString().startsWith(tableKeyPrefix)).count();
        }
    }

    private long seqParts(String tableKeyPrefix) throws Exception {
        final java.nio.file.Path tables = java.nio.file.Paths.get(storeRoot, "t");
        try (var dirs = java.nio.file.Files.list(tables)) {
            final java.nio.file.Path dir = dirs.filter(d -> d.getFileName().toString().startsWith(tableKeyPrefix)).findFirst().orElseThrow();
            try (var parts = java.nio.file.Files.list(dir.resolve("seq"))) {
                return parts.count();
            }
        }
    }

    private TestServerMain startFaulty(String serverRoot, FaultyObjectStore.Hook hook) {
        final TestServerMain server = new TestServerMain(Bootstrap.getServerMainArgs(serverRoot)) {
            @Override
            protected ObjectStore newReplicationObjectStore(ReplicationConfiguration replicationConfig) {
                return new FaultyObjectStore(super.newReplicationObjectStore(replicationConfig), hook);
            }
        };
        try {
            server.start();
            return server;
        } catch (Throwable th) {
            server.close();
            throw th;
        }
    }

    // commits an index owned by another instance, as a second primary would
    // a store whose WAL data uploads wait for the latch while isBlocking is set
    private static FaultyObjectStore.Hook blockingUploads(AtomicBoolean isBlocking, CountDownLatch blocked, CountDownLatch release) {
        return new FaultyObjectStore.Hook() {
            @Override
            public void before(String op, String key) {
                if (isBlocking.get() && op.equals("put") && key.contains("/wal/")) {
                    blocked.countDown();
                    try {
                        Assert.assertTrue("the test did not release the upload", release.await(120, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
    }

    private static long lastTxn(ReplicationIndex index, String tableName) {
        for (int i = 0, n = index.getTables().size(); i < n; i++) {
            if (index.getTables().getQuick(i).getTableName().equals(tableName)) {
                return index.getTables().getQuick(i).getLastTxn();
            }
        }
        return -1;
    }

    private void writeForeignIndex() {
        try (FsObjectStore store = new FsObjectStore(ObjectStoreSettings.parse("fs::root=" + storeRoot + ';'), new ReplicationConfigurationImpl());
             IndexClient client = new IndexClient(store, true)) {
            final ReplicationIndex idx = client.read();
            Assert.assertNotNull(idx);
            idx.setOwner("another-primary", idx.getEpoch() + 1, idx.getRenewedAt(), idx.getSequence() + 1, false);
            client.write(idx, 1);
        }
    }
}
