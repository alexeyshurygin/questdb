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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Restarts, promotion, lease takeover and divergence between two nodes.
 */
public class ReplicationFailoverTest extends AbstractReplicationTest {

    @Test
    public void testDivergedFormerPrimaryIsFenced() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            // a long throttle window keeps a's last insert local when it crashes
            writeConfiguration(a, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(b, "replica");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (TestServerMain nodeB = start(b)) {
                    awaitRole(nodeB, EngineRole.REPLICA);
                    TestUtils.assertEventually(() -> Assert.assertNotNull(nodeB.getEngine().getTableTokenIfExists("t")), 60);
                }
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(nodeA, "t");
                nodeA.getReplicationService().crashForTesting();
            }
            // b takes over once a's lease expires and writes a different transaction 2
            writeConfiguration(b, "primary");
            allowPromotion(b);
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                nodeB.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitApplied(nodeB, "t");
            }
            // a comes back as primary. The store is b's now: a is refused before it looks any further
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.FENCED);
                final String reason = ((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason();
                TestUtils.assertContains(reason, "the object store belongs to another primary");
                TestUtils.assertContains(reason, io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE);
            }
            // told to take over anyway, it finds that its transaction 2 differs from the store's
            writeConfiguration(a, "primary");
            allowPromotion(a);
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason(), "could not catch up");
                // a refused promotion keeps its marker: nothing was promoted
                Assert.assertTrue(java.nio.file.Files.exists(java.nio.file.Paths.get(a, io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE)));
                assertWriteRefused(nodeA, "INSERT INTO t VALUES ('2024-01-03', 3)");
            }
        });
    }

    @Test
    public void testCrashedPrimaryRestartsAndUploadsItsTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            // a long throttle window keeps the last inserts and the new table local when a crashes
            writeConfiguration(a, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(b, "replica");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (TestServerMain nodeB = start(b)) {
                    awaitRole(nodeB, EngineRole.REPLICA);
                    TestUtils.assertEventually(() -> Assert.assertNotNull(nodeB.getEngine().getTableTokenIfExists("t")), 60);
                }
                // structure changes upload at once; the inserts after them wait for the window
                nodeA.execute("ALTER TABLE t ADD COLUMN s STRING");
                nodeA.execute("CREATE TABLE late (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(nodeA, "t");
                awaitUploaded(nodeA, "late");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1, NULL)");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-02', 2, 'two')");
                nodeA.execute("INSERT INTO late VALUES ('2024-01-01', 7)");
                awaitApplied(nodeA, "t");
                awaitApplied(nodeA, "late");
                final io.questdb.cairo.TableToken t = nodeA.getEngine().verifyTableName("t");
                Assert.assertTrue(nodeA.getReplicationService().getUploadedTxn(t) < nodeA.getEngine().getTableSequencerAPI().lastTxn(t));
                nodeA.getReplicationService().crashForTesting();
            }
            // nobody else wrote to the store: a's own history extends it, so a comes back as primary
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a); TestServerMain nodeB = start(b)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                Assert.assertNull(((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason());
                awaitUploaded(nodeA, "t");
                awaitUploaded(nodeA, "late");
                awaitSame(nodeA, nodeB, "t");
                awaitSame(nodeA, nodeB, "late");
                nodeB.assertSql("SELECT v, s FROM t ORDER BY v", "v\ts\n1\t\n2\ttwo\n");
                nodeB.assertSql("SELECT v FROM late", "v\n7\n");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'three')");
                awaitSame(nodeA, nodeB, "t");
            }
        });
    }

    @Test
    public void testDatabaseOfAnotherStoreIsRefused() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String c = nodeRoot("c");
            final String mainStore = storeRoot;
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(nodeA, "t");
            }
            // c is the primary of another database, with a store of its own
            storeRoot = root + File.separator + "store2";
            TestUtils.createTestPath(storeRoot);
            writeConfiguration(c, "primary");
            allowPromotion(c);
            storeRoot = mainStore;
            try (TestServerMain nodeC = start(c)) {
                awaitRole(nodeC, EngineRole.PRIMARY);
                nodeC.execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitApplied(nodeC, "x");
            }
            // pointed at a's store, c refuses it as a primary and as a replica
            writeConfiguration(c, "primary");
            allowPromotion(c);
            try (TestServerMain nodeC = start(c)) {
                awaitRole(nodeC, EngineRole.FENCED);
                Assert.assertEquals("the object store belongs to another database (DataID differs)",
                        ((io.questdb.replication.ReplicatedCairoEngine) nodeC.getEngine()).getFenceReason());
                assertWriteRefused(nodeC, "INSERT INTO x VALUES ('2024-01-02', 2)");
                Assert.assertNull(nodeC.getEngine().getTableTokenIfExists("t"));
            }
            writeConfiguration(c, "replica");
            try (TestServerMain nodeC = start(c)) {
                TestUtils.assertEventually(() -> Assert.assertEquals(
                        "stopped: the object store belongs to another database (DataID differs)",
                        nodeC.getReplicationService().getStatus()), 30);
                Assert.assertEquals(EngineRole.REPLICA, role(nodeC));
                Assert.assertNull(nodeC.getEngine().getTableTokenIfExists("t"));
                nodeC.assertSql("SELECT count() n FROM x", "n\n0\n");
            }
            // the store is untouched: a still owns it and serves its data
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.assertSql("SELECT count() n FROM t", "n\n1\n");
            }
        });
    }

    @Test
    public void testPopulatedNodeWithoutDataIdIsRefused() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String c = nodeRoot("c");
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitApplied(nodeA, "t");
            }
            // c holds tables of its own but lost its database id, so it mints a new one at boot
            writeConfiguration(c, "none");
            try (TestServerMain nodeC = start(c)) {
                nodeC.execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeC.execute("INSERT INTO x VALUES ('2024-01-01', 1)");
                TestUtils.assertEventually(() -> nodeC.assertSql("SELECT count() n FROM x", "n\n1\n"), 30);
            }
            final java.nio.file.Path dataId = java.nio.file.Paths.get(c, "db", io.questdb.cairo.DataID.FILENAME);
            java.nio.file.Files.deleteIfExists(dataId);
            writeConfiguration(c, "primary");
            allowPromotion(c);
            try (TestServerMain nodeC = start(c)) {
                awaitRole(nodeC, EngineRole.FENCED);
                Assert.assertEquals("the object store belongs to another database (DataID differs)",
                        ((io.questdb.replication.ReplicatedCairoEngine) nodeC.getEngine()).getFenceReason());
                Assert.assertNull(nodeC.getEngine().getTableTokenIfExists("t"));
                nodeC.assertSql("SELECT count() n FROM x", "n\n1\n");
            }
        });
    }

    @Test
    public void testFormerPrimaryAheadOfDivergedStoreIsFenced() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            // a long throttle window keeps a's last two inserts local when it crashes
            writeConfiguration(a, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            writeConfiguration(b, "replica");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (TestServerMain nodeB = start(b)) {
                    awaitRole(nodeB, EngineRole.REPLICA);
                    TestUtils.assertEventually(() -> Assert.assertNotNull(nodeB.getEngine().getTableTokenIfExists("t")), 60);
                }
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitApplied(nodeA, "t");
                nodeA.getReplicationService().crashForTesting();
            }
            // b takes over and writes one transaction of its own: a different transaction 2
            writeConfiguration(b, "primary");
            allowPromotion(b);
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                nodeB.execute("INSERT INTO t VALUES ('2024-02-01', 100)");
                awaitUploaded(nodeB, "t");
            }
            // a holds more transactions than the store, but they are not the store's: it must not upload them
            writeConfiguration(a, "primary");
            allowPromotion(a);
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason(), "could not catch up");
                assertWriteRefused(nodeA, "INSERT INTO t VALUES ('2024-01-03', 3)");
            }
            // the store still holds b's history only
            writeConfiguration(b, "primary");
            allowPromotion(b);
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                nodeB.assertSql("SELECT v FROM t", "v\n100\n");
            }
        });
    }

    @Test
    public void testGracefulStopUploadsEveryBatchBeforeReleasing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            // a long throttle window leaves everything to the drain at shutdown
            writeConfiguration(a, "primary", PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=600000");
            final int txnCount = 12_000; // more than one upload batch of 10_000 transactions
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(nodeA, "t");
                final io.questdb.cairo.TableToken token = nodeA.getEngine().verifyTableName("t");
                try (io.questdb.cairo.wal.WalWriter writer = nodeA.getEngine().getWalWriter(token)) {
                    for (int i = 0; i < txnCount; i++) {
                        final io.questdb.cairo.TableWriter.Row row = writer.newRow(1_704_067_200_000_000L + i * 1_000_000L);
                        row.putLong(1, i);
                        row.append();
                        writer.commit();
                    }
                }
                Assert.assertEquals(txnCount, nodeA.getEngine().getTableSequencerAPI().lastTxn(token));
                Assert.assertEquals(0, nodeA.getReplicationService().getUploadedTxn(token));
                awaitApplied(nodeA, "t");
            }
            // the store holds all of it, and only then says the lease is free
            final io.questdb.replication.codec.ReplicationIndex index = readIndex();
            Assert.assertNotNull(index);
            Assert.assertEquals(1, index.getTables().size());
            Assert.assertEquals(txnCount, index.getTables().getQuick(0).getLastTxn());
            Assert.assertTrue(index.isReleased());

            // another node takes over without a wait and without missing a row
            writeConfiguration(b, "primary");
            allowPromotion(b);
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                awaitApplied(nodeB, "t");
                nodeB.assertSql("SELECT count() n, max(v) m FROM t", "n\tm\n" + txnCount + "\t" + (txnCount - 1) + "\n");
            }
        });
    }

    @Test
    public void testNodeBehindItsOwnStoreNeedsTheMigrationMarker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String old = nodeRoot("a-old");
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(nodeA, "t");
                awaitApplied(nodeA, "t");
            }
            // the node as it was: same instance id, one transaction
            copyTree(a, old);
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("INSERT INTO t VALUES ('2024-01-02', 2), ('2024-01-03', 3)");
                awaitUploaded(nodeA, "t");
            }
            final long sequence = readIndex().getSequence();
            // its disk is rolled back, as a power loss rolls back commits that were never flushed: the
            // store now holds more than the node. It owns the store, but it does not take it over unasked
            copyTree(old, a);
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.FENCED);
                final String reason = ((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason();
                TestUtils.assertContains(reason, "this node is behind the object store [table=t]");
                TestUtils.assertContains(reason, io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE);
                assertWriteRefused(nodeA, "INSERT INTO t VALUES ('2024-01-04', 4)");
                nodeA.assertSql("SELECT count() n FROM t", "n\n1\n");
            }
            Assert.assertEquals(sequence, readIndex().getSequence());
            // told to, it brings itself up to the store and carries on
            writeConfiguration(a, "primary");
            allowPromotion(a);
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                awaitApplied(nodeA, "t");
                nodeA.assertSql("SELECT count() n FROM t", "n\n3\n");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-04', 4)");
                awaitUploaded(nodeA, "t");
            }
        });
    }

    @Test
    public void testMarkerThatCannotBeRemovedStopsThePromotion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            writeConfiguration(a, "primary");
            writeConfiguration(b, "replica");
            try (TestServerMain nodeA = start(a); TestServerMain nodeB = start(b)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitSame(nodeA, nodeB, "t");
            }
            final io.questdb.replication.codec.ReplicationIndex released = readIndex();

            // a marker the server cannot delete: a directory that is not empty. It would allow every later
            // promotion too, so the node refuses this one and leaves the store alone
            final java.nio.file.Path marker = java.nio.file.Paths.get(b, io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE);
            java.nio.file.Files.createDirectory(marker);
            java.nio.file.Files.createFile(marker.resolve("keep"));
            writeConfiguration(b, "primary");
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.FENCED);
                final String reason = ((io.questdb.replication.ReplicatedCairoEngine) nodeB.getEngine()).getFenceReason();
                TestUtils.assertContains(reason, "could not remove " + marker);
                TestUtils.assertContains(reason, "remove it and restart this node");
                assertWriteRefused(nodeB, "INSERT INTO t VALUES ('2024-01-02', 2)");
            }
            Assert.assertTrue(java.nio.file.Files.exists(marker.resolve("keep")));
            final io.questdb.replication.codec.ReplicationIndex untouched = readIndex();
            Assert.assertEquals(released.getSequence(), untouched.getSequence());
            Assert.assertEquals(released.getOwnerInstanceId(), untouched.getOwnerInstanceId());
            Assert.assertEquals(released.getEpoch(), untouched.getEpoch());
            Assert.assertTrue(untouched.isReleased());

            // with a marker it can remove, the node is promoted
            java.nio.file.Files.delete(marker.resolve("keep"));
            java.nio.file.Files.delete(marker);
            allowPromotion(b);
            writeConfiguration(b, "primary");
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                Assert.assertFalse(java.nio.file.Files.exists(marker));
                Assert.assertEquals(nodeB.getReplicationService().getInstanceId(), readIndex().getOwnerInstanceId());
            }
        });
    }

    @Test
    public void testStoreThatGetsAnOwnerDuringStartupNeedsTheMarker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            writeConfiguration(a, "primary");
            final AtomicInteger decisions = new AtomicInteger();
            try (TestServerMain node = new TestServerMain(io.questdb.Bootstrap.getServerMainArgs(a))) {
                // the store is empty when the node first looks, so nothing stands in its way. Before it
                // takes the store, another primary creates the index and stops, releasing its lease
                node.getReplicationService().setTakeoverHookForTesting(() -> {
                    if (decisions.incrementAndGet() == 1) {
                        final io.questdb.cairo.DataID dataID = node.getEngine().getDataID();
                        try (io.questdb.replication.store.FsObjectStore store = new io.questdb.replication.store.FsObjectStore(
                                io.questdb.replication.store.ObjectStoreSettings.parse("fs::root=" + storeRoot + ';'),
                                new io.questdb.replication.ReplicationConfigurationImpl());
                             io.questdb.replication.IndexClient client = new io.questdb.replication.IndexClient(store, true)) {
                            Assert.assertNull(client.read());
                            final io.questdb.replication.codec.ReplicationIndex idx = new io.questdb.replication.codec.ReplicationIndex();
                            idx.setDataId(dataID.getHi(), dataID.getLo());
                            idx.setOwner("another-primary", 1, 1, 1, true);
                            client.write(idx, 1);
                        }
                    }
                });
                node.start();
                // a released lease is free to take, but not by a node the store does not belong to
                awaitRole(node, EngineRole.FENCED);
                final String reason = ((io.questdb.replication.ReplicatedCairoEngine) node.getEngine()).getFenceReason();
                TestUtils.assertContains(reason, "the object store belongs to another primary [owner=another-primary]");
                TestUtils.assertContains(reason, "create an empty " + io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE + " file in");
                Assert.assertEquals(1, decisions.get());
            }
            final io.questdb.replication.codec.ReplicationIndex untouched = readIndex();
            Assert.assertEquals("another-primary", untouched.getOwnerInstanceId());
            Assert.assertEquals(1, untouched.getSequence());
            Assert.assertEquals(1, untouched.getEpoch());
            Assert.assertTrue(untouched.isReleased());
        });
    }

    @Test
    public void testTakeoverStartsOverWhenTheOwnerWritesAgain() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            writeConfiguration(a, "primary");
            writeConfiguration(b, "replica");
            final String ownerA;
            try (TestServerMain nodeA = start(a); TestServerMain nodeB = start(b)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitSame(nodeA, nodeB, "t");
                ownerA = nodeA.getReplicationService().getInstanceId();
                // a dies: its lease stays held
                nodeA.getReplicationService().crashForTesting();
            }
            final long sequenceA = readIndex().getSequence();
            Assert.assertEquals(ownerA, readIndex().getOwnerInstanceId());
            Assert.assertFalse(readIndex().isReleased());

            writeConfiguration(b, "primary");
            allowPromotion(b);
            final AtomicInteger decisions = new AtomicInteger();
            final AtomicLong firstDecisionNanos = new AtomicLong();
            final AtomicLong secondDecisionNanos = new AtomicLong();
            final AtomicReference<String> ownerAtSecondDecision = new AtomicReference<>();
            final AtomicLong sequenceAtSecondDecision = new AtomicLong();
            try (TestServerMain nodeB = start(b)) {
                // b waits three keepalive intervals for a's lease, so the hook is in place in time
                nodeB.getReplicationService().setTakeoverHookForTesting(() -> {
                    final int decision = decisions.incrementAndGet();
                    if (decision == 1) {
                        // b saw the index unchanged for the whole wait and decided to take over. Right
                        // then a turns out to be alive: it renews its lease
                        firstDecisionNanos.set(System.nanoTime());
                        renewIndexAs(ownerA);
                    } else if (decision == 2) {
                        secondDecisionNanos.set(System.nanoTime());
                        final io.questdb.replication.codec.ReplicationIndex idx = readIndex();
                        ownerAtSecondDecision.set(idx.getOwnerInstanceId());
                        sequenceAtSecondDecision.set(idx.getSequence());
                    }
                });
                awaitRole(nodeB, EngineRole.PRIMARY);
                // b did not replace the renewed index: it waited for a's lease once more, in full
                Assert.assertEquals(2, decisions.get());
                Assert.assertEquals(ownerA, ownerAtSecondDecision.get());
                Assert.assertEquals(sequenceA + 1, sequenceAtSecondDecision.get());
                final long waitedMillis = TimeUnit.NANOSECONDS.toMillis(secondDecisionNanos.get() - firstDecisionNanos.get());
                Assert.assertTrue("waited " + waitedMillis + " ms", waitedMillis >= 3_000);
                Assert.assertEquals(nodeB.getReplicationService().getInstanceId(), readIndex().getOwnerInstanceId());
                nodeB.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitUploaded(nodeB, "t");
            }
        });
    }

    @Test
    public void testPromotionNeedsTheMigrationMarker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            writeConfiguration(a, "primary");
            writeConfiguration(b, "replica");
            try (TestServerMain nodeA = start(a); TestServerMain nodeB = start(b)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitSame(nodeA, nodeB, "t");
            }
            final io.questdb.replication.codec.ReplicationIndex released = readIndex();
            Assert.assertTrue(released.isReleased());
            final java.nio.file.Path marker = java.nio.file.Paths.get(b, io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE);

            // the replica's role is changed to primary and nothing else: a slip of the hand. It is caught
            // up and the lease is free, yet it leaves the store alone
            writeConfiguration(b, "primary");
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.FENCED);
                final String reason = ((io.questdb.replication.ReplicatedCairoEngine) nodeB.getEngine()).getFenceReason();
                TestUtils.assertContains(reason, "the object store belongs to another primary [owner=" + released.getOwnerInstanceId() + ']');
                TestUtils.assertContains(reason, "create an empty " + io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE + " file in");
                assertWriteRefused(nodeB, "INSERT INTO t VALUES ('2024-01-02', 2)");
                nodeB.assertSql("SELECT count() n FROM t", "n\n1\n");
            }
            final io.questdb.replication.codec.ReplicationIndex untouched = readIndex();
            Assert.assertEquals(released.getSequence(), untouched.getSequence());
            Assert.assertEquals(released.getOwnerInstanceId(), untouched.getOwnerInstanceId());
            Assert.assertEquals(released.getEpoch(), untouched.getEpoch());

            // the operator means it: the marker promotes the node once, and is gone afterwards
            writeConfiguration(b, "primary");
            allowPromotion(b);
            Assert.assertTrue(java.nio.file.Files.exists(marker));
            final String ownerB;
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                ownerB = nodeB.getReplicationService().getInstanceId();
                TestUtils.assertEventually(() -> Assert.assertFalse(java.nio.file.Files.exists(marker)), 30);
                nodeB.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitUploaded(nodeB, "t");
            }
            Assert.assertEquals(ownerB, readIndex().getOwnerInstanceId());
            // the store is its own now: it restarts as primary without being told again
            writeConfiguration(b, "primary");
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                nodeB.assertSql("SELECT count() n FROM t", "n\n2\n");
            }
            // and the former primary needs the marker to come back as primary
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason(),
                        "the object store belongs to another primary [owner=" + ownerB + ']');
            }
        });
    }

    @Test
    public void testIndexReplacedByAnotherWriterFencesPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitApplied(nodeA, "t");
                // another primary commits an index: a's next commit loses the compare-and-swap
                final java.nio.file.Path index = java.nio.file.Paths.get(storeRoot, "_index");
                TestUtils.assertEventually(() -> Assert.assertTrue(java.nio.file.Files.exists(index)), 30);
                final byte[] bytes = java.nio.file.Files.readAllBytes(index);
                bytes[bytes.length - 1] ^= 1;
                java.nio.file.Files.write(index, bytes);
                awaitRole(nodeA, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason(), "lost its lease");
                assertWriteRefused(nodeA, "INSERT INTO t VALUES ('2024-01-03', 3)");
            }
        });
    }

    @Test
    public void testLeaseTakeoverAfterCrash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            writeConfiguration(a, "primary");
            writeConfiguration(b, "primary");
            allowPromotion(b);
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(nodeA, "t");
                try (TestServerMain nodeB = start(b)) {
                    // a renews its lease every keepalive interval: b waits
                    Thread.sleep(2_000);
                    Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, role(nodeB));
                    assertWriteRefused(nodeB, "INSERT INTO t VALUES ('2024-01-02', 2)");
                    nodeA.getReplicationService().crashForTesting();
                    // three missed keepalives later b owns the store
                    awaitRole(nodeB, EngineRole.PRIMARY);
                    Assert.assertEquals(nodeA.getReplicationService().getEpoch() + 1, nodeB.getReplicationService().getEpoch());
                    awaitApplied(nodeB, "t");
                    nodeB.assertSql("SELECT count() n FROM t", "n\n1\n");
                    nodeB.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                    awaitApplied(nodeB, "t");
                }
            }
        });
    }

    @Test
    public void testPrimaryRestartKeepsReplicating() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            try (TestServerMain replica = start(r)) {
                awaitRole(replica, EngineRole.REPLICA);
                long epoch;
                try (TestServerMain primary = start(p)) {
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 1, 'a'), ('2024-01-02', 2, 'b')");
                    awaitSame(primary, replica, "t");
                    epoch = primary.getReplicationService().getEpoch();
                }
                // a graceful stop released the lease: the restart takes it at once, in a new epoch
                final long started = System.nanoTime();
                try (TestServerMain primary = start(p)) {
                    awaitRole(primary, EngineRole.PRIMARY);
                    Assert.assertTrue("lease should be free after a graceful stop", System.nanoTime() - started < 10_000_000_000L);
                    Assert.assertEquals(epoch + 1, primary.getReplicationService().getEpoch());
                    primary.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'c')");
                    primary.execute("ALTER TABLE t ADD COLUMN d DOUBLE");
                    primary.execute("INSERT INTO t VALUES ('2024-01-04', 4, 'a', 4.5)");
                    awaitSame(primary, replica, "t");
                }
            }
        });
    }

    @Test
    public void testPromotionAndFormerPrimaryRejoinsAsReplica() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = nodeRoot("a");
            final String b = nodeRoot("b");
            writeConfiguration(a, "primary");
            writeConfiguration(b, "replica");
            try (TestServerMain nodeA = start(a); TestServerMain nodeB = start(b)) {
                awaitRole(nodeA, EngineRole.PRIMARY);
                awaitRole(nodeB, EngineRole.REPLICA);
                nodeA.execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.execute("INSERT INTO t VALUES ('2024-01-01', 1, 'one'), ('2024-01-02', 2, 'two')");
                awaitSame(nodeA, nodeB, "t");
            }
            // promote b: restart it as primary
            writeConfiguration(b, "primary");
            allowPromotion(b);
            writeConfiguration(a, "replica");
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                nodeB.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'three')");
                nodeB.execute("ALTER TABLE t ADD COLUMN extra INT");
                nodeB.execute("INSERT INTO t VALUES ('2024-01-04', 4, 'four', 44)");
                try (TestServerMain nodeA = start(a)) {
                    awaitRole(nodeA, EngineRole.REPLICA);
                    awaitSame(nodeB, nodeA, "t");
                    assertWriteRefused(nodeA, "INSERT INTO t VALUES ('2024-01-05', 5, 'five', 55)");
                    nodeB.execute("INSERT INTO t VALUES ('2024-01-05', 5, 'five', 55)");
                    awaitSame(nodeB, nodeA, "t");
                }
            }
        });
    }

    @Test
    public void testReplicaRestartResumes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                try (TestServerMain replica = start(r)) {
                    awaitSame(primary, replica, "t");
                }
                for (int i = 2; i < 12; i++) {
                    primary.execute("INSERT INTO t VALUES ('2024-01-" + (10 + i) + "', " + i + ')');
                }
                primary.execute("CREATE TABLE u (ts TIMESTAMP, s STRING) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                primary.execute("INSERT INTO u VALUES ('2024-01-01T01', 'x'), ('2024-01-01T02', NULL)");
                try (TestServerMain replica = start(r)) {
                    awaitSame(primary, replica, "t");
                    awaitSame(primary, replica, "u");
                }
                // restarting with nothing new is a no-op
                try (TestServerMain replica = start(r)) {
                    awaitSame(primary, replica, "t");
                    awaitSame(primary, replica, "u");
                }
            }
        });
    }

    private static void assertWriteRefused(TestServerMain server, String sql) {
        try {
            server.execute(sql);
            Assert.fail("expected a read-only refusal");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        } catch (AssertionError e) {
            if (e.getCause() == null) {
                throw e;
            }
            TestUtils.assertContains(e.getCause().getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        }
    }

    // what a live owner's keepalive does: the same index again, under the next sequence
    private void renewIndexAs(String owner) {
        try (io.questdb.replication.store.FsObjectStore store = new io.questdb.replication.store.FsObjectStore(
                io.questdb.replication.store.ObjectStoreSettings.parse("fs::root=" + storeRoot + ';'),
                new io.questdb.replication.ReplicationConfigurationImpl());
             io.questdb.replication.IndexClient client = new io.questdb.replication.IndexClient(store, true)) {
            final io.questdb.replication.codec.ReplicationIndex idx = client.read();
            Assert.assertNotNull(idx);
            Assert.assertEquals(owner, idx.getOwnerInstanceId());
            idx.setOwner(owner, idx.getEpoch(), idx.getRenewedAt() + 1, idx.getSequence() + 1, false);
            client.write(idx, 1);
        }
    }
}
