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
import io.questdb.cairo.CairoException;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * End to end: a primary and a replica server in one JVM sharing a filesystem object store.
 */
public class ReplicationServerTest extends AbstractBootstrapTest {
    private String primaryRoot;
    private String replicaRoot;
    private String storeRoot;

    @Before
    public void setUpRoots() {
        primaryRoot = root + File.separator + "primary";
        replicaRoot = root + File.separator + "replica";
        storeRoot = root + File.separator + "store";
        TestUtils.removeTestPath(primaryRoot);
        TestUtils.removeTestPath(replicaRoot);
        TestUtils.removeTestPath(storeRoot);
        new File(storeRoot).mkdirs();
    }

    @Test
    public void testReplicaFollowsPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeConfiguration(primaryRoot, "primary");
            writeConfiguration(replicaRoot, "replica");
            try (TestServerMain primary = start(primaryRoot)) {
                awaitRole(primary, ReplicatedCairoEngine.EngineRole.PRIMARY);
                primary.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, note VARCHAR, qty LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO trades VALUES ('2024-01-01T00:00:00.000000Z', 'A', 1.5, 'first', 10), ('2024-01-01T00:00:01.000000Z', 'B', 2.5, NULL, 20), ('2024-01-02T00:00:00.000000Z', 'A', 3.5, 'a much longer note than nine bytes', 30)");
                primary.execute("ALTER TABLE trades ADD COLUMN flag BOOLEAN");
                primary.execute("INSERT INTO trades VALUES ('2024-01-03T00:00:00.000000Z', 'C', 4.5, 'after alter', 40, true)");
                primary.execute("UPDATE trades SET qty = qty * 2 WHERE sym = 'A'");
                awaitApplied(primary, "trades");

                try (TestServerMain replica = start(replicaRoot)) {
                    awaitRole(replica, ReplicatedCairoEngine.EngineRole.REPLICA);
                    final String expected = select(primary, "trades");
                    TestUtils.assertEventually(() -> Assert.assertEquals(expected, select(replica, "trades")), 30);

                    // the replica refuses writes
                    try {
                        replica.execute("INSERT INTO trades VALUES ('2024-01-04T00:00:00.000000Z', 'D', 5.5, 'x', 50, false)");
                        Assert.fail("a replica must refuse writes");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
                    } catch (AssertionError e) {
                        TestUtils.assertContains(String.valueOf(e.getCause()), CairoException.READ_ONLY_ACCESS_MESSAGE);
                    }

                    // later writes on the primary reach the replica too
                    primary.execute("INSERT INTO trades VALUES ('2024-01-05T00:00:00.000000Z', 'E', 6.5, 'later', 60, false)");
                    primary.execute("RENAME TABLE trades TO fills");
                    awaitApplied(primary, "fills");
                    final String expected2 = select(primary, "fills");
                    TestUtils.assertEventually(() -> Assert.assertEquals(expected2, select(replica, "fills")), 30);

                    primary.execute("DROP TABLE fills");
                    TestUtils.assertEventually(() -> Assert.assertNull(replica.getEngine().getTableTokenIfExists("fills")), 30);
                }
            }
        });
    }

    private static void awaitApplied(TestServerMain server, String table) throws Exception {
        TestUtils.assertEventually(() -> {
            server.assertSql("SELECT writerTxn = sequencerTxn ok FROM wal_tables() WHERE name = '" + table + "'", "ok\ntrue\n");
        }, 30);
    }

    private static void awaitRole(TestServerMain server, ReplicatedCairoEngine.EngineRole role) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertEquals(role, ((ReplicatedCairoEngine) server.getEngine()).getRole()), 60);
    }

    private static int freePort() {
        try (var socket = new java.net.ServerSocket(0, 0, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        } catch (java.io.IOException e) {
            throw new AssertionError("could not obtain a free port", e);
        }
    }

    private static String select(TestServerMain server, String table) {
        final StringSink sink = new StringSink();
        try {
            TestUtils.printSql(server.getEngine(), server.getSqlExecutionContext(), "SELECT * FROM " + table + " ORDER BY ts", sink);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return sink.toString();
    }

    private static TestServerMain start(String serverRoot) {
        final TestServerMain server = new TestServerMain(Bootstrap.getServerMainArgs(serverRoot));
        try {
            server.start();
            return server;
        } catch (Throwable th) {
            server.close();
            throw th;
        }
    }

    private void writeConfiguration(String serverRoot, String role) throws Exception {
        final int httpPort = freePort();
        final int httpMinPort = freePort();
        final int pgPort = freePort();
        final int ilpPort = freePort();
        TestUtils.createTestPath(serverRoot);
        createDummyConfiguration(
                httpPort, httpMinPort, pgPort, ilpPort, serverRoot,
                PropertyKey.HTTP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + httpPort,
                PropertyKey.HTTP_MIN_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + httpMinPort,
                PropertyKey.PG_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + pgPort,
                PropertyKey.LINE_TCP_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort,
                PropertyKey.LINE_UDP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort,
                PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getPropertyPath() + "=true",
                PropertyKey.REPLICATION_ROLE.getPropertyPath() + '=' + role,
                PropertyKey.REPLICATION_OBJECT_STORE.getPropertyPath() + "=fs::root=" + storeRoot + ';',
                PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=100",
                PropertyKey.REPLICATION_PRIMARY_KEEPALIVE_INTERVAL.getPropertyPath() + "=1000",
                PropertyKey.REPLICATION_REPLICA_POLL_INTERVAL.getPropertyPath() + "=100"
        );
    }
}
