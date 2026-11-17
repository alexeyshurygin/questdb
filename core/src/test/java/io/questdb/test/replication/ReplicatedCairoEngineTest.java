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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CheckpointListener;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationRole;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Role-based write fencing of the replicated engine, driven through SQL and the engine API.
 */
public class ReplicatedCairoEngineTest extends AbstractTest {

    @Test
    public void testDataIdMintedOnlyByPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("dataid");
            try (ReplicatedCairoEngine primary = newEngine(dir, ReplicationRole.PRIMARY)) {
                Assert.assertTrue(primary.getDataID().isInitialized());
                Assert.assertTrue(primary.isDataIdMintedAtBoot());
            }
            try (ReplicatedCairoEngine again = newEngine(dir, ReplicationRole.PRIMARY)) {
                Assert.assertTrue(again.getDataID().isInitialized());
                Assert.assertFalse(again.isDataIdMintedAtBoot());
            }
            try (ReplicatedCairoEngine replica = newEngine(newDir("replica-dataid"), ReplicationRole.REPLICA)) {
                // a replica adopts the store's id later; it never mints one
                Assert.assertFalse(replica.getDataID().isInitialized());
                Assert.assertFalse(replica.isDataIdMintedAtBoot());
            }
        });
    }

    @Test
    public void testDisabledReplicationIsRejected() {
        try {
            newEngine(newDir("none"), ReplicationRole.NONE);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("replication is disabled", e.getMessage());
        }
    }

    @Test
    public void testRolesFenceWrites() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ReplicatedCairoEngine engine = newEngine(newDir("roles"), ReplicationRole.PRIMARY)) {
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, engine.getRole());
                Assert.assertTrue(engine.isReadOnlyMode());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP, engine.getQwpServerInfoProvider().role());
                Assert.assertFalse(engine.isForceAlterAllowed());
                assertRefused(engine, "CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

                final long epoch0 = engine.getQwpServerInfoProvider().getEpoch();
                engine.setRole(EngineRole.PRIMARY, null);
                // the primary role alone opens no writes: the node also needs a current lease
                Assert.assertTrue(engine.isPrimaryRole());
                Assert.assertTrue(engine.isLeaseLapsed());
                Assert.assertTrue(engine.isReadOnlyMode());
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, engine.getRole());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP, engine.getQwpServerInfoProvider().role());
                assertRefused(engine, "CREATE TABLE early (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // a lease that ran out a moment ago is as good as none, and renewals never move it back
                engine.renewLease(System.nanoTime() - 1);
                Assert.assertTrue(engine.isReadOnlyMode());
                engine.renewLease(System.nanoTime() + java.util.concurrent.TimeUnit.HOURS.toNanos(1));
                engine.renewLease(System.nanoTime() - 1);
                Assert.assertFalse(engine.isLeaseLapsed());
                Assert.assertEquals(EngineRole.PRIMARY, engine.getRole());
                Assert.assertFalse(engine.isReadOnlyMode());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_PRIMARY, engine.getQwpServerInfoProvider().role());
                Assert.assertEquals(epoch0 + 1, engine.getQwpServerInfoProvider().getEpoch());
                Assert.assertNull(engine.getFenceReason());
                engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("CREATE TABLE nw (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                engine.execute("INSERT INTO nw VALUES ('2024-01-01', 1)");
                try {
                    engine.execute("ALTER TABLE t REBASE WAL");
                    Assert.fail();
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "REBASE WAL is not supported when replication is enabled");
                }
                final TableToken t = engine.verifyTableName("t");
                // views stay node-local, and a writable primary changes them
                engine.execute("CREATE VIEW v AS SELECT ts, v FROM t");
                engine.execute("ALTER VIEW v AS SELECT ts FROM t");
                final TableToken view = engine.verifyTableName("v");
                engine.getViewWalWriter(view).close();
                engine.execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, max(v) m FROM t SAMPLE BY 1d");
                Assert.assertNotNull(engine.getTableTokenIfExists("mv"));

                engine.setRole(EngineRole.REPLICA, "ignored for this role");
                assertRefused(engine, "ALTER VIEW v AS SELECT v FROM t");
                assertRefused(engine, "CREATE VIEW v2 AS SELECT ts FROM t");
                assertRefused(engine, "CREATE MATERIALIZED VIEW mv2 AS SELECT ts, max(v) m FROM t SAMPLE BY 1d");
                Assert.assertNull(engine.getTableTokenIfExists("v2"));
                Assert.assertNull(engine.getTableTokenIfExists("mv2"));
                assertRefused(() -> engine.getViewWalWriter(view));
                Assert.assertNull(engine.getFenceReason());
                Assert.assertTrue(engine.isReadOnlyMode());
                Assert.assertTrue(engine.isForceAlterAllowed());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_REPLICA, engine.getQwpServerInfoProvider().role());
                assertRefused(engine, "INSERT INTO t VALUES ('2024-01-02', 2)");
                assertRefused(engine, "ALTER TABLE t ADD COLUMN x INT");
                assertRefused(engine, "RENAME TABLE t TO u");
                assertRefused(engine, "DROP TABLE t");
                assertRefused(engine, "CREATE TABLE t2 (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertRefused(() -> engine.getWalWriter(t));
                assertRefused(() -> engine.getTableWriterAPI(t, "test"));
                assertRefused(() -> engine.getTableWriterAPI("t", "test"));
                assertRefused(() -> engine.getWriter(t, "test"));
                // SQL refuses every INSERT on a read-only node; the engine itself still hands out writers
                // of node-local (non-WAL) tables, which query exports use
                assertRefused(engine, "INSERT INTO nw VALUES ('2024-01-02', 2)");
                engine.getTableWriterAPI("nw", "test").close();
                // the break-glass path stays open on a replica
                try (TableWriter w = engine.getWriter(t, OperationDispatcher.FORCE_OPERATION_APPLY_REASON)) {
                    Assert.assertNotNull(w);
                }

                engine.setRole(EngineRole.FENCED, "lost the lease");
                Assert.assertEquals("lost the lease", engine.getFenceReason());
                Assert.assertFalse(engine.isForceAlterAllowed());
                Assert.assertEquals(QwpEgressMsgKind.ROLE_REPLICA, engine.getQwpServerInfoProvider().role());
                assertRefused(() -> engine.getWriter(t, OperationDispatcher.FORCE_OPERATION_APPLY_REASON));
                // replication itself still drops tables
                try (Path path = new Path()) {
                    engine.dropReplicatedTable(path.of(engine.getConfiguration().getDbRoot()), t);
                }
                Assert.assertNull(engine.getTableTokenIfExists("t"));
                Assert.assertEquals("questdb", engine.getQwpServerInfoProvider().getClusterId().toString());
                Assert.assertNotNull(engine.getQwpServerInfoProvider().getNodeId());
                Assert.assertEquals(
                        engine.getConfiguration().getQwpServerInfoProvider().getCapabilities(),
                        engine.getQwpServerInfoProvider().getCapabilities()
                );
                Assert.assertNull(engine.getQwpServerInfoProvider().getZoneId());
                Assert.assertEquals(ReplicationRole.PRIMARY, engine.getReplicationConfiguration().getRole());
            }
        });
    }

    @Test
    public void testCheckpointListenerOverride() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ReplicatedCairoEngine engine = newEngine(newDir("ckpt"), ReplicationRole.REPLICA)) {
                final CheckpointListener configured = engine.getCheckpointListener();
                final CheckpointListener mine = new CheckpointListener() {
                    @Override
                    public void onCheckpointReleased(long timestampMicros, CharSequenceLongHashMap tableDirNamesToSeqTxn) {
                    }

                    @Override
                    public void onCheckpointRestoreComplete() {
                    }
                };
                engine.setCheckpointListener(mine);
                Assert.assertSame(mine, engine.getCheckpointListener());
                engine.setCheckpointListener(null);
                Assert.assertSame(configured, engine.getCheckpointListener());
                Assert.assertEquals(EngineRole.REPLICA, engine.getRole());
            }
        });
    }

    private static void assertRefused(ReplicatedCairoEngine engine, String sql) {
        try {
            engine.execute(sql);
            Assert.fail("expected a read-only refusal: " + sql);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        }
    }

    private static void assertRefused(ThrowingAction action) {
        try {
            final AutoCloseable c = action.run();
            if (c != null) {
                c.close();
            }
            Assert.fail("expected a read-only refusal");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static ReplicatedCairoEngine newEngine(String dir, ReplicationRole role) {
        final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(dir + File.separator + "db", dir);
        final ReplicatedCairoEngine engine = new ReplicatedCairoEngine(
                configuration,
                new QdbrWalLocker(),
                failure -> {
                },
                new ReplicationConfigurationImpl().setRole(role).setObjectStore("fs::root=" + dir + "/store;")
        );
        try {
            engine.completeInit();
            engine.load();
            engine.buildViewGraphs();
            return engine;
        } catch (Throwable th) {
            engine.close();
            throw th;
        }
    }

    private static String newDir(String name) {
        final String dir = root + File.separator + name;
        TestUtils.removeTestPath(dir);
        new File(dir, "db").mkdirs();
        return dir;
    }

    @FunctionalInterface
    private interface ThrowingAction {
        AutoCloseable run() throws Exception;
    }
}
