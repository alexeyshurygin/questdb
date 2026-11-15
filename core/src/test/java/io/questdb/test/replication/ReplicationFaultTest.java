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
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
