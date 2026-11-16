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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CheckpointListener;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlCompiler;
import io.questdb.replication.IndexClient;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationRole;
import io.questdb.replication.ReplicationService;
import io.questdb.replication.StoreKeys;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.FilesFacade;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The replication service of a node driven directly, engine and service built as the server builds
 * them, over an in-memory object store: how it stops, how interrupts reach it, and how a primary
 * handles a dropped table, a failed promotion flush, a lost index write and a failed lease renewal.
 */
public class ReplicationPrimaryEdgeCaseTest extends AbstractTest {
    private static final String COORDINATOR_THREAD = "qdb-replication";
    private static final String KEEPER_THREAD = "qdb-replication-lease";
    private static final int WAIT_SECONDS = 60;
    private static final String WORKER_THREAD = "qdb-replication-worker";

    @Test
    public void testCatchUpRefusesDroppedTableWhoseStoreHistoryWentOn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("diverged-a");
            final String b = newDir("diverged-b");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicBoolean isStoreDown = new AtomicBoolean();
            final FaultyObjectStore storeOfA = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (isStoreDown.get()) {
                        throw ObjectStoreException.retryable("injected store outage", null);
                    }
                }
            });
            // node a uploads transaction 1 of the table. Then the store goes away, a client drops the table,
            // which is transaction 2 on node a, and node a dies with the drop local
            try (Node nodeA = newNode(a, config(a, ReplicationRole.PRIMARY), storeOfA)) {
                nodeA.service.start();
                awaitRole(nodeA.engine, EngineRole.PRIMARY);
                nodeA.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(nodeA, "t", 1);
                isStoreDown.set(true);
                nodeA.engine.execute("DROP TABLE t");
                nodeA.service.crashForTesting();
            }
            Assert.assertEquals(1, entry(readIndex(memory), "t").getLastTxn());

            // node b takes the store over once the lease of node a expires, and writes transactions 2 and 3
            allowPromotion(b);
            try (Node nodeB = newNode(b, config(b, ReplicationRole.PRIMARY), memory)) {
                nodeB.service.start();
                awaitRole(nodeB.engine, EngineRole.PRIMARY);
                nodeB.engine.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                nodeB.engine.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                awaitUploaded(nodeB, "t", 3);
            }
            final ReplicationIndex ofB = readIndex(memory);
            Assert.assertTrue(ofB.isReleased());
            Assert.assertEquals(3, entry(ofB, "t").getLastTxn());

            // node a comes back, allowed to take the store over. Its drop would go up as transaction 2, which
            // the store holds as a write of node b: the store went on past the drop, and node a refuses it
            allowPromotion(a);
            final AtomicInteger partReads = new AtomicInteger();
            final FaultyObjectStore storeOfA2 = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("get") && key.contains("/seq/")) {
                        partReads.incrementAndGet();
                    }
                }
            });
            try (Node nodeA = newNode(a, config(a, ReplicationRole.PRIMARY), storeOfA2)) {
                nodeA.service.start();
                awaitRole(nodeA.engine, EngineRole.FENCED);
                Assert.assertEquals("tables could not catch up with the object store; see the log", nodeA.engine.getFenceReason());
                // the transaction numbers alone decide it: node a reads none of the store's history of the table
                Assert.assertEquals(0, partReads.get());
                // and it neither created the table again nor took the store
                Assert.assertNull(nodeA.engine.getTableTokenIfExists("t"));
                Assert.assertTrue(nodeA.engine.isReadOnlyMode());
            }
            // the store is as node b left it, and the refused promotion kept its marker
            final ReplicationIndex after = readIndex(memory);
            Assert.assertEquals(ofB.getOwnerInstanceId(), after.getOwnerInstanceId());
            Assert.assertEquals(ofB.getSequence(), after.getSequence());
            Assert.assertTrue(after.isReleased());
            Assert.assertFalse(entry(after, "t").isDropped());
            Assert.assertTrue(Files.exists(Paths.get(a, ReplicationService.MIGRATE_PRIMARY_FILE)));
        });
    }

    @Test
    public void testCheckpointWithHistoryOffIsNotRecorded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("history-off");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicInteger cleanerRuns = new AtomicInteger();
            final AtomicInteger historyWrites = new AtomicInteger();
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (key.startsWith(StoreKeys.CHECKPOINTS_PREFIX)) {
                        if (op.equals("list")) {
                            cleanerRuns.incrementAndGet();
                        } else if (op.equals("put")) {
                            historyWrites.incrementAndGet();
                        }
                    }
                }
            });
            final AtomicInteger releases = new AtomicInteger();
            final CheckpointListener configured = new CheckpointListener() {
                @Override
                public void onCheckpointReleased(long timestampMicros, CharSequenceLongHashMap tableDirNamesToSeqTxn) {
                    releases.incrementAndGet();
                }

                @Override
                public void onCheckpointRestoreComplete() {
                }
            };
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(dir + File.separator + "db", dir) {
                @Override
                public CheckpointListener getCheckpointListener() {
                    return configured;
                }

                @Override
                public int getCommitMode() {
                    return CommitMode.NOSYNC;
                }
            };
            // the cleaner runs every 100 ms, right after the housekeeping step that records checkpoints
            final ReplicationConfigurationImpl config = config(dir, ReplicationRole.PRIMARY)
                    .setCheckpointHistoryEnabled(false)
                    .setCleanerInterval(100);
            try (Node node = newNode(config, store, configuration, failure -> {
            })) {
                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                node.engine.execute("CHECKPOINT CREATE");
                node.engine.execute("CHECKPOINT RELEASE");
                // the listener the configuration names still hears of the checkpoint
                Assert.assertEquals(1, releases.get());
                // two cleaner runs later, each after a housekeeping step that would have recorded it, nothing is
                final int runs = cleanerRuns.get();
                TestUtils.assertEventually(() -> Assert.assertTrue(cleanerRuns.get() >= runs + 2), WAIT_SECONDS);
                Assert.assertEquals(0, historyWrites.get());
                Assert.assertTrue(memory.keys(k -> k.startsWith(StoreKeys.CHECKPOINTS_PREFIX)).isEmpty());
            }
        });
    }

    @Test
    public void testDroppedTableKeepsItsSequencerUntilStartupDecides() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("pins");
            final MemoryObjectStore store = new MemoryObjectStore();
            // nothing goes up before the stop, and the stop is a crash: both drops stay local
            final ReplicationConfigurationImpl config = config(dir, ReplicationRole.PRIMARY)
                    .setThrottleWindowDuration(600_000)
                    .setThrottleNonData(true);
            final String tableDir;
            final String viewDir;
            try (Node node = newNode(dir, config, store)) {
                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO gone VALUES ('2024-01-01', 1)");
                node.engine.execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, max(v) m FROM gone SAMPLE BY 1d");
                tableDir = node.engine.verifyTableName("gone").getDirName();
                viewDir = node.engine.verifyTableName("mv").getDirName();
                node.engine.execute("DROP MATERIALIZED VIEW mv");
                node.engine.execute("DROP TABLE gone");
                node.service.crashForTesting();
            }
            Assert.assertEquals(0, readIndex(store).getTables().size());

            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY), store)) {
                // until startup has looked at the store, the sequencer parts of the dropped table stay: its
                // drop may still have to go up
                Assert.assertTrue(node.service.isSeqPartPinned(tableDir, 0));
                // views do not replicate, and a directory no table had holds nothing to keep
                Assert.assertFalse(node.service.isSeqPartPinned(viewDir, 0));
                Assert.assertFalse(node.service.isSeqPartPinned("never~404", 0));

                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                // the store never listed the table, so startup gave its drop no uploader: the purge job may
                // remove it
                Assert.assertFalse(node.service.isSeqPartPinned(tableDir, 0));
            }
        });
    }

    @Test
    public void testDroppedTableWithUnreadableSequencerStillUploadsItsDrop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("unreadable");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicBoolean isArmed = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    // holds the coordinator in an index write while the test drops the table
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && isThread(COORDINATOR_THREAD) && isArmed.compareAndSet(true, false)) {
                        blocked.countDown();
                        holdRequest(release);
                    }
                }
            });
            // the adaptive commit mode, which every node that can become primary runs: a primary uploads only
            // what is durable locally
            try (Node node = newNode(config(dir, ReplicationRole.PRIMARY), store, newConfiguration(dir, CommitMode.ADAPTIVE, null), failure -> {
            })) {
                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                final TableToken token = node.engine.verifyTableName("t");
                awaitUploaded(node, "t", 1);
                try {
                    isArmed.set(true);
                    await(blocked);
                    // the table is dropped and its sequencer can no longer be opened: only its log is left
                    node.engine.execute("DROP TABLE t");
                    node.engine.releaseAllWalWriters();
                    node.engine.getTableSequencerAPI().releaseAll();
                    Files.delete(Paths.get(node.engine.getConfiguration().getDbRoot(), token.getDirName(), WalUtils.SEQ_DIR, TableUtils.META_FILE_NAME));
                } finally {
                    release.countDown();
                }
                // the uploader reads the last transaction from the log itself: the drop goes up
                TestUtils.assertEventually(() -> {
                    final IndexTable entry = entry(readIndex(memory), "t");
                    Assert.assertTrue(entry.isDropped());
                    Assert.assertEquals(2, entry.getLastTxn());
                }, WAIT_SECONDS);
                Assert.assertEquals(EngineRole.PRIMARY, node.engine.getRole());
            }
        });
    }

    @Test
    public void testEarlierIndexCommitThatLandedIsBuiltUpon() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("landed");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicReference<IndexWriteStage> stage = new AtomicReference<>(IndexWriteStage.TAKEOVER);
            final AtomicLong landedSequence = new AtomicLong(-1);
            final AtomicLong nextSequence = new AtomicLong(-1);
            final CountDownLatch rebuilt = new CountDownLatch(1);
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void after(String op, String key) {
                    if (!op.equals("put") || !key.equals(IndexClient.INDEX_KEY)) {
                        return;
                    }
                    if (stage.compareAndSet(IndexWriteStage.ARMED, IndexWriteStage.LANDED)) {
                        // the write landed and its answer got lost
                        landedSequence.set(readIndex(memory).getSequence());
                        throw ObjectStoreException.retryable("injected lost response", null);
                    }
                    if (stage.compareAndSet(IndexWriteStage.READ_FAILED, IndexWriteStage.DONE)) {
                        nextSequence.set(readIndex(memory).getSequence());
                        rebuilt.countDown();
                    }
                }

                @Override
                public void before(String op, String key) {
                    if (!key.equals(IndexClient.INDEX_KEY)) {
                        return;
                    }
                    if (op.equals("put") && stage.compareAndSet(IndexWriteStage.TAKEOVER, IndexWriteStage.RUNNING)) {
                        // the first index of the empty store fails before it reaches the store
                        throw ObjectStoreException.retryable("injected request failure", null);
                    }
                    if (op.equals("get") && stage.compareAndSet(IndexWriteStage.LANDED, IndexWriteStage.READ_FAILED)) {
                        // and the read that would find out that the lost write landed fails too
                        throw ObjectStoreException.retryable("injected read failure", null);
                    }
                }
            });
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY), store)) {
                node.service.start();
                // the store still had no index after the failed write: the node tried again
                awaitRole(node.engine, EngineRole.PRIMARY);
                Assert.assertEquals(IndexWriteStage.RUNNING, stage.get());
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");

                stage.set(IndexWriteStage.ARMED);
                await(rebuilt);
                // The next write after the lost one lost the comparison: the store held the lost one. The
                // node took it as its own and built on top of it, under the next sequence
                Assert.assertEquals(landedSequence.get() + 1, nextSequence.get());
                Assert.assertEquals(EngineRole.PRIMARY, node.engine.getRole());
                Assert.assertNull(node.engine.getFenceReason());

                // and it carries on committing
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(node, "t", 1);
                Assert.assertEquals(1, entry(readIndex(memory), "t").getLastTxn());
            }
        });
    }

    @Test
    public void testEngineCloseWaitsForACoordinatorThatOutlivesTheStop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("engine-waits");
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicReference<CairoEngine> engine = new AtomicReference<>();
            final AtomicBoolean isEngineOpenAtResume = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch never = new CountDownLatch(1);
            final CountDownLatch resume = new CountDownLatch(1);
            store.setFaultHook((op, key) -> {
                // the first poll of the index outlasts the stop's waits, and goes on when the test says
                if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && isThread(COORDINATOR_THREAD)
                        && coordinator.compareAndSet(null, Thread.currentThread())) {
                    blocked.countDown();
                    if (!holdRequest(never)) {
                        holdRequest(resume);
                    }
                    isEngineOpenAtResume.set(isEngineOpen(engine.get()));
                }
            });
            // a request may take 50 s with all its attempts: the engine's close waits that long for the coordinator
            final ReplicationConfigurationImpl config = config(dir, ReplicationRole.REPLICA).setRequestBaseTimeout(10_000);
            try (Node node = newNode(dir, config, store)) {
                engine.set(node.engine);
                writeIndex(store, "a-primary", node.engine.getDataID().getHi(), node.engine.getDataID().getLo());
                node.service.start();
                await(blocked);
                Assert.assertTrue(runInterrupted(node.service::close));
                Assert.assertTrue(coordinator.get().isAlive());

                // the server closes the engine next, and the coordinator goes on meanwhile
                final Thread closer = new Thread(() -> {
                    node.engine.close();
                    Path.clearThreadLocals();
                }, "test-engine-closer");
                closer.start();
                resume.countDown();
                closer.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(closer.isAlive());
                coordinator.get().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(coordinator.get().isAlive());
                // the close waited for the coordinator to end, then freed the engine
                Assert.assertTrue(isEngineOpenAtResume.get());
                Assert.assertFalse(isEngineOpen(node.engine));
                Assert.assertEquals(1, store.getCloseCount());
            }
        });
    }

    @Test
    public void testEngineStaysOpenWhileACoordinatorOutlivesTheStop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("engine-stays-open");
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicReference<CairoEngine> engine = new AtomicReference<>();
            final AtomicBoolean isEngineOpenAtResume = new AtomicBoolean();
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch never = new CountDownLatch(1);
            final CountDownLatch resume = new CountDownLatch(1);
            store.setFaultHook((op, key) -> {
                // the first poll of the index outlasts every wait, and goes on when the test says
                if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && isThread(COORDINATOR_THREAD)
                        && coordinator.compareAndSet(null, Thread.currentThread())) {
                    blocked.countDown();
                    if (!holdRequest(never)) {
                        holdRequest(resume);
                    }
                    isEngineOpenAtResume.set(isEngineOpen(engine.get()));
                }
            });
            // a request may take 200 ms with all its attempts: the engine's close waits that long for the coordinator
            final ReplicationConfigurationImpl config = config(dir, ReplicationRole.REPLICA)
                    .setRequestBaseTimeout(100)
                    .setRetryAttempts(0);
            try (Node node = newNode(dir, config, store)) {
                engine.set(node.engine);
                try {
                    writeIndex(store, "a-primary", node.engine.getDataID().getHi(), node.engine.getDataID().getLo());
                    node.service.start();
                    await(blocked);
                    Assert.assertTrue(runInterrupted(node.service::close));
                    Assert.assertTrue(coordinator.get().isAlive());

                    // the server closes the engine next: the close gives up on the coordinator and frees nothing
                    node.engine.close();
                    Assert.assertTrue(isEngineOpen(node.engine));
                } finally {
                    resume.countDown();
                }
                coordinator.get().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(coordinator.get().isAlive());
                Assert.assertTrue(isEngineOpenAtResume.get());
                Assert.assertEquals(1, store.getCloseCount());
                // the coordinator is gone: the next close frees the engine, and the leak check finds nothing left
            }
            Assert.assertFalse(isEngineOpen(engine.get()));
        });
    }

    @Test
    public void testFailedFlushAtPromotionStopsTheEngine() throws Exception {
        assertPromotionFlushFailure(true);
    }

    @Test
    public void testFailedOpenAtPromotionFencesWithoutStoppingTheEngine() throws Exception {
        assertPromotionFlushFailure(false);
    }

    @Test
    public void testIndexGoneAtWriteFencesPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("gone");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicBoolean isIndexDeleted = new AtomicBoolean();
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && isIndexDeleted.get()) {
                        // somebody deleted the index, and S3 answers a conditional replace of an object that
                        // is gone with "not found"
                        memory.delete(IndexClient.INDEX_KEY);
                        throw ObjectStoreException.notFound(key);
                    }
                }
            });
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY), store)) {
                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                isIndexDeleted.set(true);
                // that answer is a lost comparison, not an error to retry: the node fences itself
                awaitRole(node.engine, EngineRole.FENCED);
                Assert.assertEquals("the replication index is gone from the object store: this node lost its lease", node.engine.getFenceReason());
                Assert.assertTrue(node.engine.isReadOnlyMode());
            }
            // and writes no index of its own at the stop
            Assert.assertNull(readIndex(memory));
        });
    }

    @Test
    public void testIndexWithoutOwnerIsFreeToTake() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("no-owner");
            final MemoryObjectStore store = new MemoryObjectStore();
            allowPromotion(dir);
            // a lease somebody held would take three keepalive intervals to expire: three minutes
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY).setKeepaliveInterval(60_000), store)) {
                // an index that names no owner and is not released
                writeIndex(store, null, node.engine.getDataID().getHi(), node.engine.getDataID().getLo());
                final AtomicReference<String> statusAtDecision = new AtomicReference<>();
                node.service.setTakeoverHookForTesting(() -> statusAtDecision.set(node.service.getStatus()));
                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                // nobody holds it: the node decided to take it without waiting for a lease
                Assert.assertEquals("primary catching up", statusAtDecision.get());
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertEquals(2, index.getEpoch());
            }
        });
    }

    @Test
    public void testInstanceIdThatCannotBeLoadedStopsTheStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("instance");
            final MemoryObjectStore store = new MemoryObjectStore();
            // a file where the replication work directory belongs
            final java.nio.file.Path workDir = Paths.get(dir, ".replication");
            Files.write(workDir, new byte[0]);
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY), store)) {
                try {
                    node.service.start();
                    Assert.fail("the start must fail without an instance id");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not load the replication instance id [dir=" + workDir + ", error=");
                }
                // no coordinator runs: the node never asked the store anything and stays read-only
                Assert.assertNull(node.service.getInstanceId());
                Assert.assertEquals("starting", node.service.getStatus());
                Assert.assertEquals(0, store.getGetCount() + store.getPutCount() + store.getListCount());
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                // with no coordinator, a crash has nothing to wait for
                node.service.crashForTesting();
            }

            // an instance file left empty is replaced with a new id
            final String other = newDir("instance-empty");
            final java.nio.file.Path instanceFile = Paths.get(other, ".replication", "instance");
            Files.createDirectories(instanceFile.getParent());
            Files.write(instanceFile, " \n".getBytes(StandardCharsets.UTF_8));
            try (Node node = newNode(other, config(other, ReplicationRole.REPLICA), new MemoryObjectStore())) {
                node.service.start();
                final String id = node.service.getInstanceId();
                Assert.assertNotNull(id);
                Assert.assertEquals(id, UUID.fromString(id).toString());
                Assert.assertEquals(id, Files.readString(instanceFile, StandardCharsets.UTF_8));
            }
        });
    }

    @Test
    public void testInterruptedStopFreesTheIndexClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("interrupted-free");
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final CountDownLatch polled = new CountDownLatch(2);
            store.setFaultHook((op, key) -> {
                if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && isThread(COORDINATOR_THREAD)) {
                    coordinator.set(Thread.currentThread());
                    polled.countDown();
                }
            });
            try (Node node = newNode(dir, config(dir, ReplicationRole.REPLICA), store)) {
                writeIndex(store, "a-primary", node.engine.getDataID().getHi(), node.engine.getDataID().getLo());
                node.service.start();
                // the second poll began, so the first decoded the index into the index client's buffers
                await(polled);
                // a stop asked for on an interrupted thread
                Assert.assertTrue(runInterrupted(node.service::close));
                coordinator.get().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(coordinator.get().isAlive());
                // the store is closed once, and the leak check below finds the index client freed
                Assert.assertEquals(1, store.getCloseCount());
            }
        });
    }

    @Test
    public void testInterruptedStopsDoNotWaitForTheCoordinator() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("interrupted");
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicBoolean isCoordinatorInterrupted = new AtomicBoolean();
            final AtomicLong closesAtResume = new AtomicLong(-1);
            final CountDownLatch blocked = new CountDownLatch(1);
            final CountDownLatch never = new CountDownLatch(1);
            final CountDownLatch stopped = new CountDownLatch(1);
            store.setFaultHook((op, key) -> {
                // the first poll of the index never returns by itself
                if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && isThread(COORDINATOR_THREAD)
                        && coordinator.compareAndSet(null, Thread.currentThread())) {
                    blocked.countDown();
                    if (!holdRequest(never)) {
                        isCoordinatorInterrupted.set(true);
                        // interrupted, it goes on once the stop has returned: the poll reads the index from
                        // the store and decodes it into the index client
                        holdRequest(stopped);
                        closesAtResume.set(store.getCloseCount());
                    }
                }
            });
            try (Node node = newNode(dir, config(dir, ReplicationRole.REPLICA), store)) {
                // the index client holds native memory once it decoded this index
                writeIndex(store, "a-primary", node.engine.getDataID().getHi(), node.engine.getDataID().getLo());
                node.service.start();
                await(blocked);

                // a crash asked for on an interrupted thread does not wait for the coordinator, and the caller
                // keeps its interrupt
                Assert.assertTrue(runInterrupted(node.service::crashForTesting));
                Assert.assertTrue(coordinator.get().isAlive());
                Assert.assertFalse(isCoordinatorInterrupted.get());

                // neither does a stop: it interrupts the coordinator, and the caller keeps its interrupt
                Assert.assertTrue(runInterrupted(node.service::close));
                Assert.assertTrue(coordinator.get().isAlive());
                stopped.countDown();
                coordinator.get().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(coordinator.get().isAlive());
                Assert.assertTrue(isCoordinatorInterrupted.get());
                // The stop left the store and the index client to the coordinator, which still used them,
                // and the coordinator freed them as it ended: the store is closed once, after its last
                // request, and the leak check below finds nothing left.
                Assert.assertEquals(0, closesAtResume.get());
                Assert.assertEquals(1, store.getCloseCount());
            }
        });
    }

    @Test
    public void testLeaseKeeperSurvivesARenewalError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("keeper");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicBoolean isBlocking = new AtomicBoolean();
            final CountDownLatch uploadBlocked = new CountDownLatch(1);
            final CountDownLatch releaseUpload = new CountDownLatch(1);
            // -1 until the upload is blocked, 0 armed, 1 the renewal failed, 2 a later renewal landed
            final AtomicInteger keeperStage = new AtomicInteger(-1);
            final CountDownLatch renewalFailed = new CountDownLatch(1);
            final CountDownLatch renewedAgain = new CountDownLatch(1);
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void after(String op, String key) {
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && isThread(KEEPER_THREAD) && keeperStage.compareAndSet(1, 2)) {
                        renewedAgain.countDown();
                    }
                }

                @Override
                public void before(String op, String key) {
                    if (op.equals("put") && key.contains("/wal/") && isBlocking.get()) {
                        uploadBlocked.countDown();
                        holdRequest(releaseUpload);
                    }
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && isThread(KEEPER_THREAD) && keeperStage.compareAndSet(0, 1)) {
                        renewalFailed.countDown();
                        throw ObjectStoreException.fatal("injected renewal failure");
                    }
                }
            });
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY), store)) {
                try {
                    node.service.start();
                    awaitRole(node.engine, EngineRole.PRIMARY);
                    node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 0)");
                    awaitUploaded(node, "t", 1);

                    // the coordinator waits for an upload that does not end: the lease keeper renews for it
                    isBlocking.set(true);
                    node.engine.execute("INSERT INTO t VALUES ('2024-01-02', 1)");
                    await(uploadBlocked);
                    keeperStage.set(0);
                    // its renewal fails with an error that is not a lost comparison
                    await(renewalFailed);
                    // and it carries on: its next renewal lands, the node stays the writable primary
                    await(renewedAgain);
                    Assert.assertEquals(EngineRole.PRIMARY, node.engine.getRole());
                    Assert.assertNull(node.engine.getFenceReason());
                    Assert.assertEquals(1, entry(readIndex(memory), "t").getLastTxn());
                } finally {
                    releaseUpload.countDown();
                }
                awaitUploaded(node, "t", 2);
            }
        });
    }

    @Test
    public void testOwnerThatAppearsBeforeTheTakeoverIsWaitedFor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("owner-appears");
            final MemoryObjectStore store = new MemoryObjectStore();
            allowPromotion(dir);
            // the lease of the other primary lasts three keepalive intervals: half a minute
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY).setKeepaliveInterval(10_000), store)) {
                final long hi = node.engine.getDataID().getHi();
                final long lo = node.engine.getDataID().getLo();
                final AtomicInteger decisions = new AtomicInteger();
                // the store is empty when the node decides to take it; right then another primary creates the
                // index and holds it
                node.service.setTakeoverHookForTesting(() -> {
                    if (decisions.incrementAndGet() == 1) {
                        writeIndex(store, "another-primary", hi, lo);
                    }
                });
                node.service.start();
                // the node does not write over the new owner: it waits for that owner's lease
                TestUtils.assertEventually(() -> TestUtils.assertContains(node.service.getStatus(), "waiting for lease, owner=another-primary"), WAIT_SECONDS);
                node.service.close();
                Assert.assertEquals(1, decisions.get());
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals("another-primary", index.getOwnerInstanceId());
                Assert.assertEquals(1, index.getSequence());
            }
        });
    }

    @Test
    public void testRefusedReadAfterAFailedTakeoverWriteIsReportedAndRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("refused");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicInteger stage = new AtomicInteger();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicBoolean isInterruptedAtRetry = new AtomicBoolean();
            final CountDownLatch retried = new CountDownLatch(1);
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (!key.equals(IndexClient.INDEX_KEY)) {
                        return;
                    }
                    if (op.equals("put") && stage.compareAndSet(0, 1)) {
                        // the write that takes the store over fails without an answer
                        throw ObjectStoreException.retryable("injected lost response", null);
                    }
                    if (op.equals("get") && stage.compareAndSet(1, 2)) {
                        // and the store refuses the read that would tell whether it landed
                        coordinator.set(Thread.currentThread());
                        throw ObjectStoreException.fatal("injected access denied");
                    }
                    if (op.equals("get") && stage.compareAndSet(2, 3)) {
                        isInterruptedAtRetry.set(Thread.interrupted());
                        retried.countDown();
                    }
                }
            });
            // a store error during the takeover makes the node wait a retry interval: a minute here
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY).setRetryInterval(60_000), store)) {
                node.service.start();
                // the refusal is not something a retry of the read could fix: it reaches the takeover loop,
                // which reports it and stays read-only
                TestUtils.assertEventually(() -> TestUtils.assertContains(node.service.getStatus(), "primary catching up, object store error: injected access denied"), WAIT_SECONDS);
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());

                // an interrupt cuts the wait short, and the retry still carries it
                coordinator.get().interrupt();
                await(retried);
                Assert.assertTrue("the retry lost the interrupt", isInterruptedAtRetry.get());

                // the store answers now: the node takes it over
                awaitRole(node.engine, EngineRole.PRIMARY);
                Assert.assertNull(node.engine.getFenceReason());
                final ReplicationIndex index = readIndex(memory);
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertEquals(1, index.getEpoch());
            }
        });
    }

    @Test
    public void testReplicaPollsAtOnceWhenItsWaitIsInterrupted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("replica-wait");
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicInteger polls = new AtomicInteger();
            final AtomicBoolean isInterruptedAtPoll = new AtomicBoolean();
            final CountDownLatch firstPoll = new CountDownLatch(1);
            final CountDownLatch secondPoll = new CountDownLatch(1);
            store.setFaultHook((op, key) -> {
                if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && isThread(COORDINATOR_THREAD)) {
                    final int poll = polls.incrementAndGet();
                    if (poll == 1) {
                        coordinator.set(Thread.currentThread());
                        firstPoll.countDown();
                    } else if (poll == 2) {
                        isInterruptedAtPoll.set(Thread.interrupted());
                        secondPoll.countDown();
                    }
                }
            });
            // a replica that polls the index once a minute
            try (Node node = newNode(dir, config(dir, ReplicationRole.REPLICA).setReplicaPollInterval(60_000), store)) {
                node.service.start();
                await(firstPoll);
                // an interrupt ends the wait for the next poll, and the poll still carries it
                coordinator.get().interrupt();
                await(secondPoll);
                Assert.assertTrue("the poll lost the interrupt", isInterruptedAtPoll.get());
                Assert.assertEquals("replica", node.service.getStatus());
            }
        });
    }

    @Test
    public void testSecondStartIsIgnored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("restart");
            try (Node node = newNode(dir, config(dir, ReplicationRole.REPLICA), new MemoryObjectStore())) {
                final Set<Thread> before = coordinatorThreads();
                node.service.start();
                final Set<Thread> started = coordinatorThreads();
                started.removeAll(before);
                Assert.assertEquals(1, started.size());
                final String instanceId = node.service.getInstanceId();
                Assert.assertNotNull(instanceId);

                // the second start starts no second coordinator
                node.service.start();
                final Set<Thread> after = coordinatorThreads();
                after.removeAll(before);
                Assert.assertEquals(started, after);
                Assert.assertEquals(instanceId, node.service.getInstanceId());
            }
        });
    }

    @Test
    public void testStopDuringCatchUpReplaysNoFurtherTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("catch-up-a");
            final String b = newDir("catch-up-b");
            final MemoryObjectStore memory = new MemoryObjectStore();
            try (Node nodeA = newNode(a, config(a, ReplicationRole.PRIMARY), memory)) {
                nodeA.service.start();
                awaitRole(nodeA.engine, EngineRole.PRIMARY);
                nodeA.engine.execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.engine.execute("CREATE TABLE y (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                nodeA.engine.execute("INSERT INTO x VALUES ('2024-01-01', 1)");
                nodeA.engine.execute("INSERT INTO y VALUES ('2024-01-01', 1)");
            }
            final ReplicationIndex released = readIndex(memory);
            Assert.assertTrue(released.isReleased());
            Assert.assertEquals(2, released.getTables().size());

            allowPromotion(b);
            final AtomicReference<String> manifest = new AtomicReference<>();
            final CountDownLatch replaying = new CountDownLatch(1);
            final CountDownLatch stopping = new CountDownLatch(1);
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    // the catch-up downloads the create manifest of the first table, and the node is stopped
                    // meanwhile: this download ends once the stop has told the coordinator to stop
                    if (op.equals("get") && key.endsWith("/create") && isThread(COORDINATOR_THREAD) && manifest.compareAndSet(null, key)) {
                        replaying.countDown();
                        if (!holdRequest(stopping)) {
                            throw new AssertionError("interrupted before the stop began");
                        }
                    }
                }
            });
            try (Node nodeB = newNode(b, config(b, ReplicationRole.PRIMARY), store)) {
                final Thread closerThread = new Thread(nodeB.service::close, "test-closer");
                nodeB.service.setCloseHookForTesting(stopping::countDown);
                nodeB.service.start();
                await(replaying);
                closerThread.start();
                closerThread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(closerThread.isAlive());
                // the table it was replaying is there, and the stop kept it from the other one
                final boolean hasX = nodeB.engine.getTableTokenIfExists("x") != null;
                final boolean hasY = nodeB.engine.getTableTokenIfExists("y") != null;
                Assert.assertTrue("x: " + hasX + ", y: " + hasY, hasX ^ hasY);
                // nor did it go on to the lease
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, nodeB.engine.getRole());
                Assert.assertNull(nodeB.engine.getFenceReason());
            }
            final ReplicationIndex after = readIndex(memory);
            Assert.assertEquals(released.getOwnerInstanceId(), after.getOwnerInstanceId());
            Assert.assertEquals(released.getSequence(), after.getSequence());
            Assert.assertTrue(after.isReleased());
            Assert.assertTrue(Files.exists(Paths.get(b, ReplicationService.MIGRATE_PRIMARY_FILE)));
        });
    }

    @Test
    public void testStopThatGivesUpOnTheCoordinatorRefusesItsLaterUploads() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("giving-up");
            final MemoryObjectStore memory = new MemoryObjectStore();
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicReference<String> poolUpload = new AtomicReference<>();
            final AtomicReference<String> coordinatorUpload = new AtomicReference<>();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicBoolean isCoordinatorInterrupted = new AtomicBoolean();
            final AtomicLong closesAtResume = new AtomicLong(-1);
            final CountDownLatch poolInterrupted = new CountDownLatch(1);
            final CountDownLatch never = new CountDownLatch(1);
            final CountDownLatch stopped = new CountDownLatch(1);
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    if (!isArmed.get() || !op.equals("put") || !key.startsWith("t/")) {
                        return;
                    }
                    if (isThread(WORKER_THREAD) && poolUpload.compareAndSet(null, key)) {
                        // the only pool thread waits for the stop to shut the pool down, which interrupts it
                        if (!holdRequest(never)) {
                            poolInterrupted.countDown();
                        }
                    } else if (isThread(COORDINATOR_THREAD) && coordinatorUpload.compareAndSet(null, key)) {
                        // the pool is busy, so the coordinator uploads the next table itself, and takes longer
                        // than the stop waits for it
                        coordinator.set(Thread.currentThread());
                        if (!holdRequest(never)) {
                            isCoordinatorInterrupted.set(true);
                        }
                        // its upload ends once the stop has returned
                        holdRequest(stopped);
                        closesAtResume.set(memory.getCloseCount());
                    }
                }
            });
            // one pool thread; nothing goes up before the stop, which uploads the three new tables
            final ReplicationConfigurationImpl config = config(dir, ReplicationRole.PRIMARY)
                    .setMaxConcurrentRequests(1)
                    .setThrottleWindowDuration(600_000)
                    .setThrottleNonData(true);
            try (Node node = newNode(dir, config, store)) {
                node.service.start();
                awaitRole(node.engine, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE a (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("CREATE TABLE b (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("CREATE TABLE c (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                isArmed.set(true);
                final Thread closer = new Thread(node.service::close, "test-closer");
                closer.start();
                closer.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(closer.isAlive());
                // the stop shut the pool down
                Assert.assertEquals(0, poolInterrupted.getCount());
                // it gave up waiting for the coordinator, interrupted it and returned while it still runs
                Assert.assertTrue(isCoordinatorInterrupted.get());
                Assert.assertTrue(coordinator.get().isAlive());
                stopped.countDown();
                coordinator.get().join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
                Assert.assertFalse(coordinator.get().isAlive());

                Assert.assertNotNull(poolUpload.get());
                Assert.assertNotNull(coordinatorUpload.get());
                Assert.assertNotEquals(poolUpload.get(), coordinatorUpload.get());
                // The stop left the store and the coordinator's buffers to the coordinator, which went on
                // uploading with them, and freed them as it ended: the store is closed once, after the
                // coordinator's last request, and the leak check below finds nothing left.
                Assert.assertEquals(0, closesAtResume.get());
                Assert.assertEquals(1, memory.getCloseCount());
                Assert.assertEquals(0, node.service.getWorkerContextCount());
                // the third table came after the pool was shut down: refused, it never went up
                final List<String> manifests = memory.keys(k -> k.startsWith("t/") && k.endsWith("/create"));
                Assert.assertEquals(2, manifests.size());
                Assert.assertTrue(manifests.contains(poolUpload.get()));
                Assert.assertTrue(manifests.contains(coordinatorUpload.get()));
                // the drain neither committed nor released: the lease is left to expire
                final ReplicationIndex index = readIndex(memory);
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(0, index.getTables().size());
            }
        });
    }

    @Test
    public void testStopWhileWaitingForTheLeaseLeavesTheStoreAlone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("waiting");
            final MemoryObjectStore store = new MemoryObjectStore();
            allowPromotion(dir);
            // the lease of the other primary lasts three keepalive intervals: half a minute
            try (Node node = newNode(dir, config(dir, ReplicationRole.PRIMARY).setKeepaliveInterval(10_000), store)) {
                final long hi = node.engine.getDataID().getHi();
                final long lo = node.engine.getDataID().getLo();
                // a live primary owns the store; its database id differs from this node's in the low half only
                writeIndex(store, "another-primary", hi, lo + 1);
                final long sequence = readIndex(store).getSequence();
                node.service.start();
                TestUtils.assertEventually(() -> TestUtils.assertContains(node.service.getStatus(), "waiting for lease, owner=another-primary"), WAIT_SECONDS);
                // the empty node joined the database of the store
                Assert.assertEquals(hi, node.engine.getDataID().getHi());
                Assert.assertEquals(lo + 1, node.engine.getDataID().getLo());

                // stopped while it waits, it stops waiting: no takeover, no fence
                node.service.close();
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                Assert.assertNull(node.engine.getFenceReason());
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals("another-primary", index.getOwnerInstanceId());
                Assert.assertEquals(sequence, index.getSequence());
                Assert.assertFalse(index.isReleased());
                // the promotion the marker allows did not happen, so the marker is still there
                Assert.assertTrue(Files.exists(Paths.get(dir, ReplicationService.MIGRATE_PRIMARY_FILE)));
            }
        });
    }

    @Test
    public void testSyncReplicaOnceAppliesWhatTheIndexNames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = newDir("sync-primary");
            final String r = newDir("sync-replica");
            final MemoryObjectStore store = new MemoryObjectStore();
            // under the sync commit mode a primary uploads what is durable locally, which is every commit
            try (Node primary = newNode(config(p, ReplicationRole.PRIMARY), store, newConfiguration(p, CommitMode.SYNC, null), failure -> {
            })) {
                primary.service.start();
                awaitRole(primary.engine, EngineRole.PRIMARY);
                primary.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                primary.engine.execute("ALTER TABLE t ADD COLUMN s VARCHAR");
                primary.engine.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'x')");
                awaitUploaded(primary, "t", 3);
            }
            // the graceful stop released the lease over all three transactions
            final ReplicationIndex index = readIndex(store);
            Assert.assertTrue(index.isReleased());
            Assert.assertEquals(3, entry(index, "t").getLastTxn());

            try (Node replica = newNode(r, config(r, ReplicationRole.REPLICA), store)) {
                // a replica that is not started: one forced round applies everything the index names, and the
                // next finds nothing new
                Assert.assertEquals(3, replica.service.syncReplicaOnce());
                Assert.assertEquals(0, replica.service.syncReplicaOnce());
                final TableToken token = replica.engine.verifyTableName("t");
                Assert.assertEquals(3, replica.engine.getTableSequencerAPI().lastTxn(token));
                TestUtils.drainWalQueue(replica.engine);
                try (TableReader reader = replica.engine.getReader(token)) {
                    Assert.assertEquals(3, reader.size());
                    Assert.assertEquals(3, reader.getMetadata().getColumnCount());
                }
            }
        });
    }

    private static void allowPromotion(String installRoot) throws Exception {
        Files.createFile(Paths.get(installRoot, ReplicationService.MIGRATE_PRIMARY_FILE));
    }

    // a database from before replication, whose promotion flush fails: by the flush, or by a file it cannot open
    private static void assertPromotionFlushFailure(boolean isSyncFailure) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir(isSyncFailure ? "flush-sync" : "flush-open");
            try (CairoEngine engine = new CairoEngine(newConfiguration(dir, CommitMode.NOSYNC, null))) {
                engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                TestUtils.drainWalQueue(engine);
            }
            final FlushFaultFilesFacade ff = new FlushFaultFilesFacade(isSyncFailure);
            final AtomicInteger durabilityFailures = new AtomicInteger();
            final MemoryObjectStore memory = new MemoryObjectStore();
            final FaultyObjectStore store = new FaultyObjectStore(memory, new FaultyObjectStore.Hook() {
                @Override
                public void after(String op, String key) {
                    // the store accepted the index that takes it over; the flush that admits writes comes
                    // next, and from here on the flushes fail
                    if (op.equals("put") && key.equals(IndexClient.INDEX_KEY)) {
                        ff.arm();
                    }
                }
            });
            try (Node node = newNode(config(dir, ReplicationRole.PRIMARY), store, newConfiguration(dir, CommitMode.ADAPTIVE, ff), failure -> durabilityFailures.incrementAndGet())) {
                node.service.start();
                awaitRole(node.engine, EngineRole.FENCED);
                final String reason = node.engine.getFenceReason();
                TestUtils.assertContains(reason, "replication coordinator failed: ");
                TestUtils.assertContains(reason, isSyncFailure ? "injected fsync failure" : "could not open adaptive file for fsync");
                Assert.assertTrue(ff.getFaultCount() > 0);
                // it took the store, but the flush that admits writes did not complete
                Assert.assertEquals(-1, node.service.getPromotionFlushedTableCount());
                Assert.assertTrue(node.engine.isReadOnlyMode());
                Assert.assertEquals(node.service.getInstanceId(), readIndex(memory).getOwnerInstanceId());
                // a failed flush stops the engine; a file the flush could not open fails the promotion only
                Assert.assertEquals(isSyncFailure, node.engine.isDurabilityFailed());
                Assert.assertEquals(isSyncFailure ? 1 : 0, durabilityFailures.get());
            }
            // nor does the stop release a lease the node never acted on: it is left to expire
            Assert.assertFalse(readIndex(memory).isReleased());
        });
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assert.assertTrue("timed out", latch.await(WAIT_SECONDS, TimeUnit.SECONDS));
    }

    private static void awaitRole(ReplicatedCairoEngine engine, EngineRole role) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertEquals(role, engine.getRole()), WAIT_SECONDS);
    }

    // waits until the index holds the table up to the transaction
    private static void awaitUploaded(Node node, String table, long txn) throws Exception {
        final TableToken token = node.engine.verifyTableName(table);
        TestUtils.assertEventually(() -> Assert.assertEquals(txn, node.service.getUploadedTxn(token)), WAIT_SECONDS);
    }

    private static ReplicationConfigurationImpl config(String installRoot, ReplicationRole role) {
        return new ReplicationConfigurationImpl()
                .setRole(role)
                // names a store; the tests hand the service their own
                .setObjectStore("fs::root=" + installRoot + File.separator + "store;")
                .setKeepaliveInterval(1_000)
                .setReplicaPollInterval(50)
                // a stop waits five seconds for the coordinator: three request timeouts, five seconds at least
                .setRequestBaseTimeout(1_000)
                .setThrottleWindowDuration(100);
    }

    private static Set<Thread> coordinatorThreads() {
        final Set<Thread> threads = new HashSet<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.isAlive() && COORDINATOR_THREAD.equals(t.getName())) {
                threads.add(t);
            }
        }
        return threads;
    }

    // the first entry of the table in the index; a name can be listed twice, a dropped table and its successor
    private static IndexTable entry(ReplicationIndex index, String tableName) {
        Assert.assertNotNull("the store holds no index", index);
        for (int i = 0, n = index.getTables().size(); i < n; i++) {
            final IndexTable t = index.getTables().getQuick(i);
            if (t.getTableName().equals(tableName)) {
                return t;
            }
        }
        throw new AssertionError("the index does not list " + tableName);
    }

    // holds a store request until the latch opens; false when the thread was interrupted meanwhile
    private static boolean holdRequest(CountDownLatch latch) {
        try {
            if (!latch.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("the test did not release the request");
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    // a closed engine refuses to hand out a compiler
    private static boolean isEngineOpen(CairoEngine engine) {
        try (SqlCompiler ignore = engine.getSqlCompiler()) {
            return true;
        } catch (PoolClosedException e) {
            return false;
        }
    }

    private static boolean isThread(String name) {
        return name.equals(Thread.currentThread().getName());
    }

    private static DefaultTestCairoConfiguration newConfiguration(String installRoot, int commitMode, @Nullable FilesFacade ff) {
        return new DefaultTestCairoConfiguration(installRoot + File.separator + "db", installRoot) {
            @Override
            public int getCommitMode() {
                return commitMode;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff != null ? ff : super.getFilesFacade();
            }
        };
    }

    private static String newDir(String name) {
        final String dir = root + File.separator + name;
        TestUtils.removeTestPath(dir);
        new File(dir, "db").mkdirs();
        return dir;
    }

    private static Node newNode(String installRoot, ReplicationConfigurationImpl replicationConfig, ObjectStore store) {
        return newNode(replicationConfig, store, newConfiguration(installRoot, CommitMode.NOSYNC, null), failure -> {
        });
    }

    // builds the engine and the service in the order the server does: the service hooks in before the
    // engine completes its initialization
    private static Node newNode(
            ReplicationConfigurationImpl replicationConfig,
            ObjectStore store,
            CairoConfiguration configuration,
            CairoEngine.DurabilityFailureHandler durabilityFailureHandler
    ) {
        final ReplicatedCairoEngine engine = new ReplicatedCairoEngine(
                configuration,
                new QdbrWalLocker(),
                durabilityFailureHandler,
                replicationConfig
        );
        ReplicationService service = null;
        try {
            service = new ReplicationService(engine, store);
            service.installHooks();
            engine.completeInit();
            engine.load();
            engine.buildViewGraphs();
            return new Node(engine, service);
        } catch (Throwable th) {
            Misc.free(service);
            engine.close();
            throw th;
        }
    }

    private static ReplicationIndex readIndex(ObjectStore store) {
        try (IndexClient client = new IndexClient(store, true)) {
            return client.read();
        }
    }

    // runs the action on a thread whose interrupt is set; true when the interrupt is still set afterwards
    private static boolean runInterrupted(Runnable action) throws InterruptedException {
        final AtomicBoolean isStillInterrupted = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                action.run();
            } catch (Throwable th) {
                failure.set(th);
            } finally {
                isStillInterrupted.set(Thread.interrupted());
                Path.clearThreadLocals();
            }
        }, "test-interrupted-caller");
        thread.start();
        thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
        Assert.assertFalse(thread.isAlive());
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        return isStillInterrupted.get();
    }

    // creates the index of an empty store, held by the owner, as another node would
    private static void writeIndex(ObjectStore store, @Nullable String owner, long dataIdHi, long dataIdLo) {
        try (IndexClient client = new IndexClient(store, true)) {
            Assert.assertNull(client.read());
            final ReplicationIndex idx = new ReplicationIndex();
            idx.setDataId(dataIdHi, dataIdLo);
            idx.setOwner(owner, 1, 1, 1, false);
            client.write(idx, 1);
        }
    }

    private enum IndexWriteStage {
        TAKEOVER, RUNNING, ARMED, LANDED, READ_FAILED, DONE
    }

    // fails the flushes of the promotion once armed: each flush itself, or the opens of the files to flush
    private static final class FlushFaultFilesFacade extends TestFilesFacadeImpl {
        private final AtomicInteger faultCount = new AtomicInteger();
        private final boolean isSyncFailure;
        private volatile boolean isArmed;

        private FlushFaultFilesFacade(boolean isSyncFailure) {
            this.isSyncFailure = isSyncFailure;
        }

        @Override
        public void fsync(long fd) {
            if (isArmed && isSyncFailure) {
                faultCount.incrementAndGet();
                throw CairoException.dataSyncFailureOfFd(this, fd, 5, "fsync", "injected fsync failure");
            }
            super.fsync(fd);
        }

        @Override
        public boolean isSyncfsFileSystemWide() {
            // the flush goes file by file on every platform
            return false;
        }

        @Override
        public long openRWExisting(LPSZ name) {
            if (isArmed && !isSyncFailure) {
                faultCount.incrementAndGet();
                return -1;
            }
            return super.openRWExisting(name);
        }

        private void arm() {
            isArmed = true;
        }

        private int getFaultCount() {
            return faultCount.get();
        }
    }

    // a replicated engine with its replication service
    private static final class Node implements QuietCloseable {
        private final ReplicatedCairoEngine engine;
        private final ReplicationService service;

        private Node(ReplicatedCairoEngine engine, ReplicationService service) {
            this.engine = engine;
            this.service = service;
        }

        @Override
        public void close() {
            // as the server stops: replication first, then the engine
            Misc.free(service);
            Misc.free(engine);
        }
    }
}
