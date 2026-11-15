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
            try (TestServerMain nodeB = start(b)) {
                awaitRole(nodeB, EngineRole.PRIMARY);
                nodeB.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitApplied(nodeB, "t");
            }
            // a comes back as primary: its transaction 2 differs from the store's
            writeConfiguration(a, "primary");
            try (TestServerMain nodeA = start(a)) {
                awaitRole(nodeA, EngineRole.FENCED);
                TestUtils.assertContains(((io.questdb.replication.ReplicatedCairoEngine) nodeA.getEngine()).getFenceReason(), "could not catch up");
                assertWriteRefused(nodeA, "INSERT INTO t VALUES ('2024-01-03', 3)");
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
}
