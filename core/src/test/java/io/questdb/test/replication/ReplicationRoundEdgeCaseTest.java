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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.replication.IndexClient;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationRole;
import io.questdb.replication.ReplicationService;
import io.questdb.replication.StoreKeys;
import io.questdb.replication.codec.CheckpointHistory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.ObjectEnvelope;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.FilesFacade;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * The rounds of the replication coordinator at their edges: a stop that arrives at each step of a
 * takeover, a lease that lapses and comes back, a shutdown drain that is deposed, refused, cut short
 * or outrun by a new table, a primary that is behind its own store, a replica round that fails in an
 * unforeseen way, and the WAL cleaner when its commit fails, a stop cuts it short or a rate paces it.
 * <p>
 * Each node is an engine and its replication service on a directory of its own, wired as ServerMain
 * wires them, and the nodes of a test share an in-memory object store. The store's fault hook stands
 * in for its failures; it tells the coordinator from the lease keeper by the names of their threads.
 */
public class ReplicationRoundEdgeCaseTest extends AbstractTest {
    private static final String COORDINATOR_THREAD = "qdb-replication";
    private static final long DATA_ID_HI = 0x5eed;
    private static final long DATA_ID_LO = 0xd1ce;
    private static final String FOREIGN_OWNER = "another-primary";
    private static final String LEASE_KEEPER_THREAD = "qdb-replication-lease";
    private static final String SEQUENCER_LOG = '/' + WalUtils.SEQ_DIR + '/' + WalUtils.TXNLOG_FILE_NAME;

    @Test
    public void testCleanerCommitsItsPlanBeforeItDeletesAnything() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            // the dropped table the test watches, as "t/<key>/", and its name
            final AtomicReference<String> watchedPrefix = new AtomicReference<>();
            final AtomicReference<String> watchedTable = new AtomicReference<>();
            // the coordinator's index writes to refuse once the cleaner has planned the table's removal
            final AtomicInteger refusalsToMake = new AtomicInteger();
            final AtomicBoolean isRefusalFatal = new AtomicBoolean();
            final AtomicInteger retryableRefusals = new AtomicInteger();
            final AtomicInteger fatalRefusals = new AtomicInteger();
            final List<String> deletedWhileNamed = Collections.synchronizedList(new ArrayList<>());
            store.setFaultHook((op, key) -> {
                final String prefix = watchedPrefix.get();
                if (prefix == null) {
                    return;
                }
                final boolean isCoordinator = COORDINATOR_THREAD.equals(Thread.currentThread().getName());
                if (op.equals("list") && key.equals(prefix) && isCoordinator) {
                    // the cleaner lists a dropped table's objects right before it commits an index without it
                    refusalsToMake.set(isRefusalFatal.get() ? 1 : 2);
                } else if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && isCoordinator && refusalsToMake.getAndDecrement() > 0) {
                    if (isRefusalFatal.get()) {
                        fatalRefusals.incrementAndGet();
                        throw ObjectStoreException.fatal("injected index write refusal");
                    }
                    retryableRefusals.incrementAndGet();
                    throw ObjectStoreException.retryable("injected index write timeout", null);
                } else if (op.equals("delete") && key.startsWith(prefix) && entry(readIndex(store), watchedTable.get()) != null) {
                    deletedWhileNamed.add(key);
                }
            });
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setSequencerPartTxnCount(2)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20)
                    .setCleanerCheckpointSource(false)
                    .setCleanerDroppedTableCooloff(0);
            try (Node node = newNode("p", config, store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE keep (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 1; i <= 4; i++) {
                    node.engine.execute("INSERT INTO keep VALUES ('2024-01-0" + i + "', " + i + ')');
                }
                awaitUploaded(node, store, "keep", 4);
                final String keepSeq = StoreKeys.tablePrefix(entry(readIndex(store), "keep").getKey()) + "seq/";
                Assert.assertEquals(2, countKeys(store, keepSeq));
                // a backup holds every transaction of the table; with checkpoints as the cleaner's source,
                // the cleaner would free all of its history
                putCheckpointHistory(store, checkpoint(node.engine.verifyTableName("keep").getDirName(), 4));

                // g0: the commit that removes the table from the index fails, and so does its retry in the
                // next run; g1: the commit fails with an error the cleaner does not expect
                final List<String> prefixes = new ArrayList<>();
                for (int round = 0; round < 2; round++) {
                    final String table = "g" + round;
                    node.engine.execute("CREATE TABLE " + table + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    node.engine.execute("INSERT INTO " + table + " VALUES ('2024-01-01', 1)");
                    awaitUploaded(node, store, table, 1);
                    final String prefix = StoreKeys.tablePrefix(entry(readIndex(store), table).getKey());
                    prefixes.add(prefix);
                    Assert.assertTrue(countKeys(store, prefix) >= 3);
                    isRefusalFatal.set(round == 1);
                    watchedTable.set(table);
                    watchedPrefix.set(prefix);
                    node.engine.execute("DROP TABLE " + table);
                    TestUtils.assertEventually(() -> Assert.assertEquals(0, countKeys(store, prefix)), 30);
                    Assert.assertNull(entry(readIndex(store), table));
                    watchedPrefix.set(null);
                    // the pooled writers close their segments only now, after the cleaner forgot g0: the next
                    // upload round meets a closed segment of a table it no longer tracks
                    node.engine.releaseAllWalWriters();
                }
                // the cleaner retried the refused commits before any object went, and no object went while
                // an index in the store still named its table
                Assert.assertEquals(2, retryableRefusals.get());
                Assert.assertEquals(1, fatalRefusals.get());
                Assert.assertEquals("[]", deletedWhileNamed.toString());
                // the late segment closes brought neither table back
                node.engine.execute("INSERT INTO keep VALUES ('2024-01-05', 5)");
                awaitUploaded(node, store, "keep", 5);
                for (int i = 0, n = prefixes.size(); i < n; i++) {
                    Assert.assertEquals(0, countKeys(store, prefixes.get(i)));
                    Assert.assertNull(entry(readIndex(store), "g" + i));
                }
                // with checkpoints off as its source, the cleaner frees no history of a live table
                Assert.assertEquals(3, countKeys(store, keepSeq));
                Assert.assertEquals(1, entry(readIndex(store), "keep").getFirstTxn());
                Assert.assertEquals(EngineRole.PRIMARY, node.engine.getRole());
            }
        });
    }

    @Test
    public void testCleanerFreesOnlyTheHistoryAKeptCheckpointCovers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setSequencerPartTxnCount(2)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20)
                    .setCleanerBackupWindowCount(1);
            try (Node node = newNode("p", config, store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                final String[] tables = {"covered", "empty", "uncovered"};
                for (String table : tables) {
                    node.engine.execute("CREATE TABLE " + table + " (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    for (int i = 1; i <= 5; i++) {
                        node.engine.execute("INSERT INTO " + table + " VALUES ('2024-01-0" + i + "', " + i + ')');
                    }
                    awaitUploaded(node, store, table, 5);
                }
                // transactions 1..5 of each table sit in three parts of two
                final ReplicationIndex uploaded = readIndex(store);
                for (String table : tables) {
                    Assert.assertEquals(3, countKeys(store, seqPrefix(uploaded, table)));
                }
                // a dropped table cools off for an hour, the default, before its objects go
                node.engine.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO gone VALUES ('2024-01-01', 1)");
                awaitUploaded(node, store, "gone", 1);
                final String gonePrefix = StoreKeys.tablePrefix(entry(readIndex(store), "gone").getKey());
                node.engine.execute("DROP TABLE gone");
                TestUtils.assertEventually(() -> Assert.assertTrue(entry(readIndex(store), "gone").isDropped()), 30);
                final int goneObjects = countKeys(store, gonePrefix);
                // The only backup holds "covered" at transaction 5. When the backup ran, "empty" had no
                // transaction yet and "uncovered" did not exist.
                final CheckpointHistory.Checkpoint backup = checkpoint(node.engine.verifyTableName("covered").getDirName(), 5);
                backup.addTable(node.engine.verifyTableName("empty").getDirName(), 0);
                putCheckpointHistory(store, backup);

                // the backup holds transactions 1..4 of "covered": its first two parts go
                TestUtils.assertEventually(() -> Assert.assertEquals(1, countKeys(store, seqPrefix(uploaded, "covered"))), 30);
                final ReplicationIndex cleaned = readIndex(store);
                Assert.assertEquals(5, entry(cleaned, "covered").getFirstTxn());
                // a table the backup lacks, or holds with no transaction, keeps all of its history
                Assert.assertEquals(3, countKeys(store, seqPrefix(uploaded, "empty")));
                Assert.assertEquals(3, countKeys(store, seqPrefix(uploaded, "uncovered")));
                Assert.assertEquals(1, entry(cleaned, "empty").getFirstTxn());
                Assert.assertEquals(1, entry(cleaned, "uncovered").getFirstTxn());
                // the same runs left the dropped table alone: it is still cooling off
                Assert.assertTrue(entry(cleaned, "gone").isDropped());
                Assert.assertEquals(goneObjects, countKeys(store, gonePrefix));
            }
        });
    }

    @Test
    public void testCleanerLeavesATableWithAnUncommittedBatchAlone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicBoolean isCommitRefused = new AtomicBoolean();
            final AtomicInteger cutoffReads = new AtomicInteger();
            store.setFaultHook((op, key) -> {
                if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && COORDINATOR_THREAD.equals(Thread.currentThread().getName())
                        && isCommitRefused.compareAndSet(true, false)) {
                    throw ObjectStoreException.retryable("injected index write timeout", null);
                }
                if (op.equals("list") && key.equals(StoreKeys.CHECKPOINTS_PREFIX)) {
                    // the cleaner reads the checkpoint histories once a run
                    cutoffReads.incrementAndGet();
                }
            });
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setSequencerPartTxnCount(2)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20)
                    .setCleanerBackupWindowCount(1);
            try (Node node = newNode("p", config, store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 1; i <= 4; i++) {
                    node.engine.execute("INSERT INTO t VALUES ('2024-01-0" + i + "', " + i + ')');
                }
                awaitUploaded(node, store, "t", 4);
                final String seq = seqPrefix(readIndex(store), "t");
                // transaction 5 goes up, and the index commit that would name it times out
                isCommitRefused.set(true);
                node.engine.execute("INSERT INTO t VALUES ('2024-01-05', 5)");
                TestUtils.assertEventually(() -> Assert.assertFalse(isCommitRefused.get()), 30);
                TestUtils.assertEventually(() -> Assert.assertEquals(3, countKeys(store, seq)), 30);
                Assert.assertEquals(4, entry(readIndex(store), "t").getLastTxn());
                // A backup holds transactions 1..4, which would free parts 0 and 1. The cleaner leaves the table
                // alone while it has a batch that no index names.
                putCheckpointHistory(store, checkpoint(node.engine.verifyTableName("t").getDirName(), 4));
                final int readsAtBackup = cutoffReads.get();
                TestUtils.assertEventually(() -> Assert.assertTrue(cutoffReads.get() >= readsAtBackup + 2), 30);
                Assert.assertEquals(3, countKeys(store, seq));
                Assert.assertEquals(1, entry(readIndex(store), "t").getFirstTxn());
                // the next upload commits the batch too, and then the cleaner frees the two parts
                node.engine.execute("INSERT INTO t VALUES ('2024-01-06', 6)");
                awaitUploaded(node, store, "t", 6);
                TestUtils.assertEventually(() -> Assert.assertEquals(1, countKeys(store, seq)), 30);
                Assert.assertEquals(5, entry(readIndex(store), "t").getFirstTxn());
            }
        });
    }

    @Test
    public void testCleanerPacesItsDeletes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> watchedPrefix = new AtomicReference<>();
            // the node's clock, which paces the cleaner, runs this far ahead of System.nanoTime()
            final AtomicLong clockOffset = new AtomicLong();
            final AtomicLong listedNanos = new AtomicLong();
            final List<Long> deleteNanos = Collections.synchronizedList(new ArrayList<>());
            final AtomicBoolean isFirstDelete = new AtomicBoolean(true);
            store.setFaultHook((op, key) -> {
                final String prefix = watchedPrefix.get();
                if (prefix == null) {
                    return;
                }
                if (op.equals("list") && key.equals(prefix)) {
                    // the cleaner lists a dropped table's objects before it deletes the first of them
                    listedNanos.set(System.nanoTime() + clockOffset.get());
                } else if (op.equals("delete") && key.startsWith(prefix)) {
                    if (isFirstDelete.compareAndSet(true, false)) {
                        // the first batch takes 500 ms, longer than the 400 ms its four requests are due
                        clockOffset.addAndGet(TimeUnit.MILLISECONDS.toNanos(500));
                    }
                    deleteNanos.add(System.nanoTime() + clockOffset.get());
                }
            });
            // ten delete requests a second, four at a time
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setSequencerPartTxnCount(2)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20)
                    .setCleanerDroppedTableCooloff(0)
                    .setCleanerDeleteConcurrency(4)
                    .setCleanerMaxRequestsPerSecond(10);
            try (Node node = newNode("p", config, store)) {
                node.engine.setNanoClockForTesting(() -> System.nanoTime() + clockOffset.get());
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 1; i <= 6; i++) {
                    node.engine.execute("INSERT INTO gone VALUES ('2024-01-0" + i + "', " + i + ')');
                    awaitUploaded(node, store, "gone", i);
                }
                final String prefix = StoreKeys.tablePrefix(entry(readIndex(store), "gone").getKey());
                Assert.assertTrue(countKeys(store, prefix) > 8);
                watchedPrefix.set(prefix);
                node.engine.execute("DROP TABLE gone");
                // a batch slower than its share of the rate leaves nothing to wait for, and the run goes on
                TestUtils.assertEventually(() -> Assert.assertEquals(0, countKeys(store, prefix)), 30);
                // batch k of four starts no earlier than 4k requests at ten a second allow, counted from
                // before the first batch on the node's clock
                synchronized (deleteNanos) {
                    // the create manifest, the log parts with the drop and the deltas: three batches
                    Assert.assertTrue(deleteNanos.size() > 8);
                    for (int i = 4, n = deleteNanos.size(); i < n; i += 4) {
                        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(deleteNanos.get(i) - listedNanos.get());
                        Assert.assertTrue("delete " + i + " after " + elapsedMillis + " ms", elapsedMillis >= i * 100L);
                    }
                }
            }
        });
    }

    @Test
    public void testFailedSetupOfANewTableIsRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final SequencerLogFacade ff = new SequencerLogFacade();
            try (Node node = newNode("p", primaryConfig(), store, ff, false)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                // the round that picks the new table up cannot open its sequencer log, once
                ff.failNextOpen("/late~");
                node.engine.execute("CREATE TABLE late (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO late VALUES ('2024-01-01', 1)");
                // a later round sets the table up and uploads it
                awaitUploaded(node, store, "late", 1);
                Assert.assertEquals(1, ff.openFailures.get());
                Assert.assertEquals(EngineRole.PRIMARY, node.engine.getRole());
            }
            final ReplicationIndex index = readIndex(store);
            Assert.assertTrue(index.isReleased());
            Assert.assertEquals(1, entry(index, "late").getLastTxn());
        });
    }

    @Test
    public void testFailedSetupOfANewTableKeepsTheLeaseAtTheStop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final SequencerLogFacade ff = new SequencerLogFacade();
            try (Node node = newNode("p", primaryConfig(), store, ff, false)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                // the new table's sequencer log cannot be opened, round after round
                ff.failOpens("/late~");
                node.engine.execute("CREATE TABLE late (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO late VALUES ('2024-01-01', 1)");
                TestUtils.assertEventually(() -> Assert.assertTrue(ff.openFailures.get() > 0), 30);
                node.service.close();
                // the stop could not set the table up either: it leaves the lease to expire
                final ReplicationIndex index = readIndex(store);
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertNull(entry(index, "late"));
                Assert.assertEquals(0, entry(index, "t").getLastTxn());
            }
            // the next start uploads the table
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                awaitUploaded(node, store, "late", 1);
            }
        });
    }

    @Test
    public void testForcedReplicaRoundBeforeTheStoreHasAnIndexAppliesNothing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            try (Node replica = newNode("r", replicaConfig(), store)) {
                // the service does not run: the test drives its rounds
                Assert.assertEquals(0, replica.service.syncReplicaOnce());
                Assert.assertEquals(0, store.size());
                Assert.assertFalse(replica.engine.getDataID().isInitialized());
                try (Node primary = newNode("p", primaryConfig(), store)) {
                    primary.service.start();
                    awaitRole(primary, EngineRole.PRIMARY);
                    primary.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                    primary.engine.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                    awaitUploaded(primary, store, "t", 2);
                }
                // with an index in the store, the same round applies everything it names, once
                Assert.assertEquals(2, replica.service.syncReplicaOnce());
                Assert.assertEquals(2, replica.engine.getTableSequencerAPI().lastTxn(replica.engine.verifyTableName("t")));
                Assert.assertEquals(0, replica.service.syncReplicaOnce());
                Assert.assertTrue(replica.engine.getDataID().isInitialized());
            }
        });
    }

    @Test
    public void testLapsedLeaseIsRenewedAfterStoreErrorsAndUnexpectedFailures() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            // 0: the store takes every index write; 1: it refuses them all; 2: the coordinator's next index
            // write fails in a way nobody foresaw; 3: the next one takes longer than the lease lasts, and
            // lands; 4: the next one lands at once, and so do all later ones. The lease keeper's writes are
            // refused from 1 on, so the coordinator renews the lapsed lease itself.
            final AtomicInteger phase = new AtomicInteger();
            // how far the node's clock runs ahead of System.nanoTime()
            final AtomicLong clockOffset = new AtomicLong();
            final AtomicInteger coordinatorRefusals = new AtomicInteger();
            final AtomicInteger unexpectedFailures = new AtomicInteger();
            final AtomicReference<ReplicatedCairoEngine> engine = new AtomicReference<>();
            final AtomicReference<EngineRole> roleAfterSlowRenewal = new AtomicReference<>();
            store.setFaultHook((op, key) -> {
                if (!op.equals("put") || !key.equals(IndexClient.INDEX_KEY) || phase.get() == 0) {
                    return;
                }
                final String thread = Thread.currentThread().getName();
                if (LEASE_KEEPER_THREAD.equals(thread)) {
                    throw ObjectStoreException.fatal("injected refusal of the lease keeper");
                }
                if (!COORDINATOR_THREAD.equals(thread)) {
                    return;
                }
                switch (phase.get()) {
                    case 1 -> {
                        // a refusal and a timeout, by turns
                        if (coordinatorRefusals.incrementAndGet() % 2 == 0) {
                            throw ObjectStoreException.fatal("injected store outage");
                        }
                        throw ObjectStoreException.retryable("injected store timeout", null);
                    }
                    case 2 -> {
                        phase.set(3);
                        unexpectedFailures.incrementAndGet();
                        throw new IllegalStateException("injected unexpected failure");
                    }
                    case 3 -> {
                        // the lease counts from when the node sent the write, and lasts 2.5 s: the node's
                        // clock moves 3 s on while the write is in flight
                        phase.set(4);
                        clockOffset.addAndGet(TimeUnit.SECONDS.toNanos(3));
                    }
                    case 4 -> {
                        phase.set(5);
                        roleAfterSlowRenewal.set(engine.get().getRole());
                    }
                    default -> {
                    }
                }
            });
            // the lease lasts two and a half keepalive intervals: 2.5 s
            try (Node node = newNode("p", primaryConfig().setKeepaliveInterval(1_000), store)) {
                engine.set(node.engine);
                node.engine.setNanoClockForTesting(() -> System.nanoTime() + clockOffset.get());
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                final long sequenceBefore = readIndex(store).getSequence();

                phase.set(1);
                // nothing renews the lease: it lapses, and the node refuses writes while it keeps trying
                TestUtils.assertEventually(() -> Assert.assertEquals(
                        "primary, lease lapsed: read-only until the object store accepts a renewal", node.service.getStatus()), 30);
                // renewals of the lapsed lease meet a refusal and a timeout, and are retried
                final int refusalsAtLapse = coordinatorRefusals.get();
                TestUtils.assertEventually(() -> Assert.assertTrue(coordinatorRefusals.get() >= refusalsAtLapse + 2), 30);
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                Assert.assertTrue(node.engine.isLeaseLapsed());
                Assert.assertNull(node.engine.getFenceReason());
                assertWriteRefused(node.engine, "INSERT INTO t VALUES ('2024-01-01', 1)");

                // An unforeseen failure of a renewal does not end the coordinator either. The renewal after
                // it lands too late to count: the node stays read-only until the next one lands.
                phase.set(2);
                awaitRole(node, EngineRole.PRIMARY);
                Assert.assertEquals(5, phase.get());
                Assert.assertEquals(1, unexpectedFailures.get());
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, roleAfterSlowRenewal.get());
                TestUtils.assertEventually(() -> Assert.assertEquals("primary, epoch 1", node.service.getStatus()), 30);
                Assert.assertNull(node.engine.getFenceReason());
                final ReplicationIndex renewed = readIndex(store);
                Assert.assertEquals(node.service.getInstanceId(), renewed.getOwnerInstanceId());
                Assert.assertEquals(1, renewed.getEpoch());
                Assert.assertTrue(renewed.getSequence() > sequenceBefore);
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(node, store, "t", 1);
            }
        });
    }

    @Test
    public void testLeaseWaitAfterACatchUpOfAStructureChangeKeepsItsPace() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            // node a uploads a table and a change of its structure, and dies holding the lease
            try (Node a = newNode("a", primaryConfig(), store)) {
                a.service.start();
                awaitRole(a, EngineRole.PRIMARY);
                a.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                a.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                a.engine.execute("ALTER TABLE t ADD COLUMN s VARCHAR");
                awaitUploaded(a, store, "t", 2);
                a.service.crashForTesting();
            }
            final AtomicReference<ReplicationService> service = new AtomicReference<>();
            final AtomicInteger readsWhileWaiting = new AtomicInteger();
            store.setFaultHook((op, key) -> {
                final ReplicationService s = service.get();
                if (s != null && op.equals("get") && key.equals(IndexClient.INDEX_KEY)
                        && COORDINATOR_THREAD.equals(Thread.currentThread().getName())
                        && s.getStatus().startsWith("waiting for lease")) {
                    readsWhileWaiting.incrementAndGet();
                }
            });
            // node b replays the change as it catches up, then waits for the lease of node a: three
            // keepalive intervals of 1 s
            try (Node b = newNode("b", primaryConfig().setKeepaliveInterval(1_000), store)) {
                allowPromotion(b);
                service.set(b.service);
                b.service.start();
                awaitRole(b, EngineRole.PRIMARY);
                Assert.assertEquals(2, b.engine.getTableSequencerAPI().lastTxn(b.engine.verifyTableName("t")));
                // it read the index once a keepalive interval while it waited, and once more as it took
                // the lease: about four reads, not one after another for three seconds
                final int reads = readsWhileWaiting.get();
                Assert.assertTrue("index reads while waiting for the lease: " + reads, reads <= 10);
            }
        });
    }

    @Test
    public void testMarkerWithdrawnDuringATakeoverThatNeedsNoMarker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicInteger takeovers = new AtomicInteger();
            // An operator allows a promotion the node does not need, and takes the permission back once the
            // node decided to take the store over. The store is empty: the node creates it.
            try (Node node = newNode("p", primaryConfig(), store)) {
                allowPromotion(node);
                node.service.setTakeoverHookForTesting(() -> {
                    takeovers.incrementAndGet();
                    withdrawPromotion(node);
                });
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                Assert.assertEquals(1, takeovers.get());
                Assert.assertFalse(Files.exists(node.marker()));
                Assert.assertEquals(node.service.getInstanceId(), readIndex(store).getOwnerInstanceId());
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(node, store, "t", 1);
                Assert.assertEquals(1, node.service.getEpoch());
            }
            takeovers.set(0);
            // the same again on the node's next start: the store is its own now
            try (Node node = newNode("p", primaryConfig(), store)) {
                allowPromotion(node);
                node.service.setTakeoverHookForTesting(() -> {
                    takeovers.incrementAndGet();
                    withdrawPromotion(node);
                });
                node.service.start();
                // it is the store's own primary and holds all the store holds: it needs no permission
                awaitRole(node, EngineRole.PRIMARY);
                Assert.assertEquals(1, takeovers.get());
                Assert.assertNull(node.engine.getFenceReason());
                Assert.assertEquals(2, node.service.getEpoch());
                Assert.assertFalse(Files.exists(node.marker()));
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertEquals(2, index.getEpoch());
                Assert.assertFalse(index.isReleased());
                node.engine.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                awaitUploaded(node, store, "t", 2);
            }
        });
    }

    @Test
    public void testNextPrimaryDeletesWhatAStoppedCleanerLeft() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> watchedPrefix = new AtomicReference<>();
            final AtomicReference<ReplicationService> service = new AtomicReference<>();
            final AtomicReference<Thread> closer = new AtomicReference<>();
            store.setFaultHook((op, key) -> {
                final String prefix = watchedPrefix.get();
                if (op.equals("delete") && prefix != null && key.startsWith(prefix)) {
                    synchronized (closer) {
                        if (closer.get() == null) {
                            // the node is told to stop while the cleaner's first deletes are in flight
                            closer.set(closeInBackground(service.get()));
                        }
                    }
                }
            });
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setSequencerPartTxnCount(2)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20)
                    .setCleanerDroppedTableCooloff(0)
                    .setCleanerDeleteConcurrency(4);
            final String prefix;
            try (Node a = newNode("a", config, store)) {
                service.set(a.service);
                a.service.start();
                awaitRole(a, EngineRole.PRIMARY);
                a.engine.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 1; i <= 6; i++) {
                    a.engine.execute("INSERT INTO gone VALUES ('2024-01-0" + i + "', " + i + ')');
                    awaitUploaded(a, store, "gone", i);
                }
                prefix = StoreKeys.tablePrefix(entry(readIndex(store), "gone").getKey());
                Assert.assertTrue(countKeys(store, prefix) > 8);
                watchedPrefix.set(prefix);
                a.engine.execute("DROP TABLE gone");
                TestUtils.assertEventually(() -> Assert.assertNotNull(closer.get()), 30);
                closer.get().join(60_000);
                Assert.assertFalse(closer.get().isAlive());
            }
            // the stop cut the cleaner short: no index names the objects it had not deleted yet
            watchedPrefix.set(null);
            Assert.assertTrue(countKeys(store, prefix) > 0);
            Assert.assertNull(entry(readIndex(store), "gone"));

            // the next primary, another node, deletes them
            try (Node b = newNode("b", config, store)) {
                allowPromotion(b);
                b.service.start();
                awaitRole(b, EngineRole.PRIMARY);
                TestUtils.assertEventually(() -> Assert.assertEquals(0, countKeys(store, prefix)), 30);
                Assert.assertEquals(EngineRole.PRIMARY, b.engine.getRole());
            }
        });
    }

    @Test
    public void testNodeThatLostATableIsBehindItsOwnStore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
            }
            // the node as it was: the same instance, without the table it creates next
            copyTree(nodeDir("p"), nodeDir("p-then"));
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE u (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "u", 0);
            }
            final long sequence = readIndex(store).getSequence();
            // Its disk is rolled back. It owns the store, but the store names a table the node neither has
            // nor dropped: it does not take the store over unasked.
            copyTree(nodeDir("p-then"), nodeDir("p"));
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.FENCED);
                final String reason = node.engine.getFenceReason();
                TestUtils.assertContains(reason, "this node is behind the object store [table=u]");
                TestUtils.assertContains(reason, "create an empty " + ReplicationService.MIGRATE_PRIMARY_FILE + " file in");
                Assert.assertNull(node.engine.getTableTokenIfExists("u"));
                Assert.assertEquals(sequence, readIndex(store).getSequence());
            }
        });
    }

    @Test
    public void testNodeThatStillHoldsATableTheStoreDroppedIsBehindIt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(node, store, "t", 1);
            }
            // the node as it was: the same instance, with the table it drops next
            copyTree(nodeDir("p"), nodeDir("p-then"));
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("DROP TABLE t");
                TestUtils.assertEventually(() -> Assert.assertTrue(entry(readIndex(store), "t").isDropped()), 30);
            }
            final long sequence = readIndex(store).getSequence();
            // its disk is rolled back: it still holds a table whose drop is in the store
            copyTree(nodeDir("p-then"), nodeDir("p"));
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.FENCED);
                TestUtils.assertContains(node.engine.getFenceReason(), "this node is behind the object store [table=t]");
                Assert.assertNotNull(node.engine.getTableTokenIfExists("t"));
                Assert.assertEquals(sequence, readIndex(store).getSequence());
            }
            // the cleaner was off on every start: nothing listed the store, and the dropped table's objects stay
            Assert.assertEquals(0, store.getListCount());
            Assert.assertTrue(entry(readIndex(store), "t").isDropped());
        });
    }

    @Test
    public void testPrimaryDeposedDuringItsShutdownDrainUploadsNothingMore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> deposingPrefix = new AtomicReference<>();
            final AtomicLong foreignSequence = new AtomicLong();
            final AtomicBoolean isDeposed = new AtomicBoolean();
            final AtomicBoolean isCommitTried = new AtomicBoolean();
            final List<String> putsAfterCommit = Collections.synchronizedList(new ArrayList<>());
            store.setFaultHook((op, key) -> {
                if (!op.equals("put")) {
                    return;
                }
                if (isCommitTried.get()) {
                    putsAfterCommit.add(key);
                } else if (isDeposed.get()) {
                    // the first index write after the takeover is the coordinator's commit of the drain round
                    isCommitTried.set(key.equals(IndexClient.INDEX_KEY));
                } else {
                    final String prefix = deposingPrefix.get();
                    if (prefix != null && key.startsWith(prefix)) {
                        // another node takes the store over while this one drains, and this upload fails
                        final ReplicationIndex current = readIndex(store);
                        current.setOwner(FOREIGN_OWNER, current.getEpoch() + 1, current.getRenewedAt(), current.getSequence() + 1, false);
                        putIndex(store, current);
                        foreignSequence.set(current.getSequence());
                        isDeposed.set(true);
                        throw ObjectStoreException.fatal("injected upload failure");
                    }
                }
            });
            // rows wait for a ten-minute window, so the stop drains them
            try (Node node = newNode("p", primaryConfig().setThrottleWindowDuration(600_000), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE a (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("CREATE TABLE b (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "a", 0);
                awaitUploaded(node, store, "b", 0);
                node.engine.execute("INSERT INTO a VALUES ('2024-01-01', 1)");
                node.engine.execute("INSERT INTO b VALUES ('2024-01-01', 1)");
                deposingPrefix.set(StoreKeys.tablePrefix(entry(readIndex(store), "b").getKey()) + "wal/");
                node.service.close();
                // The drain round uploaded a, and its commit found the other node's index: the node fenced
                // itself and drained no further. It neither uploaded b again nor touched the index.
                Assert.assertEquals(EngineRole.FENCED, node.engine.getRole());
                Assert.assertEquals("another primary replaced the replication index: this node lost its lease", node.engine.getFenceReason());
                Assert.assertTrue(isCommitTried.get());
                Assert.assertEquals("[]", putsAfterCommit.toString());
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(FOREIGN_OWNER, index.getOwnerInstanceId());
                Assert.assertEquals(foreignSequence.get(), index.getSequence());
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(0, entry(index, "a").getLastTxn());
            }
        });
    }

    @Test
    public void testPrimaryWithoutADatabaseIdIsRefused() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            putIndex(store, foreignIndex(3, true));
            // A read-only instance mints no database id. The server refuses that setting together with a
            // replication role; an engine that other code builds meets this check before any takeover.
            try (Node node = newNode("b", primaryConfig(), store, TestFilesFacadeImpl.INSTANCE, true)) {
                allowPromotion(node);
                node.service.start();
                awaitRole(node, EngineRole.FENCED);
                Assert.assertEquals("this primary has no database id", node.engine.getFenceReason());
                Assert.assertFalse(node.engine.getDataID().isInitialized());
                Assert.assertEquals(0, store.getPutCount());
                Assert.assertTrue(Files.exists(node.marker()));
                Assert.assertEquals(3, readIndex(store).getSequence());
            }
        });
    }

    @Test
    public void testPromotedNodeThatCatchesUpADropUploadsNothingOfTheTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> watchedPrefix = new AtomicReference<>();
            final AtomicInteger watchedPuts = new AtomicInteger();
            store.setFaultHook((op, key) -> {
                final String prefix = watchedPrefix.get();
                if (op.equals("put") && prefix != null && key.startsWith(prefix)) {
                    watchedPuts.incrementAndGet();
                }
            });
            try (Node a = newNode("a", primaryConfig(), store)) {
                a.service.start();
                awaitRole(a, EngineRole.PRIMARY);
                a.engine.execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                a.engine.execute("INSERT INTO x VALUES ('2024-01-01', 1)");
                awaitUploaded(a, store, "x", 1);
                // a replica follows the table, and stops before the primary drops it
                try (Node b = newNode("b", replicaConfig(), store)) {
                    Assert.assertEquals(1, b.service.syncReplicaOnce());
                }
                a.engine.execute("DROP TABLE x");
                TestUtils.assertEventually(() -> Assert.assertTrue(entry(readIndex(store), "x").isDropped()), 30);
            }
            final String prefix = StoreKeys.tablePrefix(entry(readIndex(store), "x").getKey());
            final int objects = countKeys(store, prefix);
            watchedPrefix.set(prefix);
            // the replica is promoted: it catches the drop up, which marks the table it drops as changed
            try (Node b = newNode("b", primaryConfig(), store)) {
                allowPromotion(b);
                b.service.start();
                awaitRole(b, EngineRole.PRIMARY);
                Assert.assertNull(b.engine.getTableTokenIfExists("x"));
                // a later round has passed the changed table by: the new table is in the store
                b.engine.execute("CREATE TABLE y (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(b, store, "y", 0);
                Assert.assertEquals(0, watchedPuts.get());
                Assert.assertTrue(entry(readIndex(store), "x").isDropped());
            }
            // nor did the stop upload anything of it
            Assert.assertEquals(0, watchedPuts.get());
            Assert.assertEquals(objects, countKeys(store, prefix));
            Assert.assertTrue(readIndex(store).isReleased());
            Assert.assertTrue(entry(readIndex(store), "x").isDropped());
        });
    }

    @Test
    public void testReleaseRefusedByTheStoreLeavesTheLeaseHeld() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicBoolean isRefusing = new AtomicBoolean();
            final AtomicInteger refusals = new AtomicInteger();
            store.setFaultHook((op, key) -> {
                if (isRefusing.get() && op.equals("put") && key.equals(IndexClient.INDEX_KEY)) {
                    refusals.incrementAndGet();
                    throw ObjectStoreException.retryable("injected index write timeout", null);
                }
            });
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitUploaded(node, store, "t", 1);
                final ReplicationIndex before = readIndex(store);
                isRefusing.set(true);
                node.service.close();
                // the node uploaded everything, but the one write that would release the lease did not land:
                // the lease stays held until it expires, as after a crash
                Assert.assertEquals(1, refusals.get());
                final ReplicationIndex after = readIndex(store);
                Assert.assertFalse(after.isReleased());
                Assert.assertEquals(before.getSequence(), after.getSequence());
                Assert.assertEquals(node.service.getInstanceId(), after.getOwnerInstanceId());
                Assert.assertEquals(1, entry(after, "t").getLastTxn());
            }
        });
    }

    @Test
    public void testReplicaRetriesAfterAnUnexpectedFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicInteger failures = new AtomicInteger(3);
            final FaultyObjectStore replicaStore = new FaultyObjectStore(store, new FaultyObjectStore.Hook() {
                @Override
                public void before(String op, String key) {
                    // not a store error: a failure nobody foresaw, in the replica's poll of the index
                    if (op.equals("get") && key.equals(IndexClient.INDEX_KEY) && failures.getAndDecrement() > 0) {
                        throw new IllegalStateException("injected unexpected failure");
                    }
                }
            });
            try (Node primary = newNode("p", primaryConfig(), store)) {
                primary.service.start();
                awaitRole(primary, EngineRole.PRIMARY);
                primary.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                awaitUploaded(primary, store, "t", 1);
                try (Node replica = newNode("r", replicaConfig(), replicaStore)) {
                    replica.service.start();
                    // the failed polls did not stop it: it replays the table and keeps following
                    awaitReplayed(replica, "t", 1);
                    Assert.assertTrue(failures.get() < 0);
                    Assert.assertEquals("replica", replica.service.getStatus());
                    primary.engine.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                    awaitUploaded(primary, store, "t", 2);
                    awaitReplayed(replica, "t", 2);
                    Assert.assertEquals(EngineRole.REPLICA, replica.engine.getRole());
                }
            }
        });
    }

    @Test
    public void testStopDuringTheCleanersDeletesEndsThemAfterTheBatchInFlight() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> watchedPrefix = new AtomicReference<>();
            final AtomicReference<ReplicationService> service = new AtomicReference<>();
            final AtomicReference<Thread> closer = new AtomicReference<>();
            final AtomicInteger deletes = new AtomicInteger();
            store.setFaultHook((op, key) -> {
                final String prefix = watchedPrefix.get();
                if (op.equals("delete") && prefix != null && key.startsWith(prefix)) {
                    deletes.incrementAndGet();
                    synchronized (closer) {
                        if (closer.get() == null) {
                            // the node is told to stop while the cleaner's first deletes are in flight
                            closer.set(closeInBackground(service.get()));
                        }
                    }
                }
            });
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setSequencerPartTxnCount(2)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20)
                    .setCleanerDroppedTableCooloff(0)
                    .setCleanerDeleteConcurrency(4);
            try (Node node = newNode("p", config, store)) {
                service.set(node.service);
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 1; i <= 6; i++) {
                    node.engine.execute("INSERT INTO gone VALUES ('2024-01-0" + i + "', " + i + ')');
                    awaitUploaded(node, store, "gone", i);
                }
                final String prefix = StoreKeys.tablePrefix(entry(readIndex(store), "gone").getKey());
                // the create manifest, three log parts and six deltas: more than one batch of four deletes
                Assert.assertTrue(countKeys(store, prefix) > 8);
                watchedPrefix.set(prefix);
                node.engine.execute("DROP TABLE gone");
                TestUtils.assertEventually(() -> Assert.assertNotNull(closer.get()), 30);
                closer.get().join(60_000);
                Assert.assertFalse(closer.get().isAlive());
                // the batch in flight completed, and no batch started after the stop
                Assert.assertEquals(4, deletes.get());
                // the cleaner had taken the table out of the index before it deleted anything; the stop
                // then released the lease
                final ReplicationIndex index = readIndex(store);
                Assert.assertNull(entry(index, "gone"));
                Assert.assertTrue(index.isReleased());
            }
        });
    }

    @Test
    public void testStopRenewsALapsedLeaseAndDrains() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicBoolean isStoreDown = new AtomicBoolean();
            final AtomicReference<ReplicationService> service = new AtomicReference<>();
            final AtomicReference<Thread> closer = new AtomicReference<>();
            store.setFaultHook((op, key) -> {
                if (!isStoreDown.get() && closer.get() == null) {
                    return;
                }
                final boolean isIndexWrite = op.equals("put") && key.equals(IndexClient.INDEX_KEY);
                final String thread = Thread.currentThread().getName();
                if (isIndexWrite && LEASE_KEEPER_THREAD.equals(thread)) {
                    // from the outage on, the lease keeper renews nothing: the stop renews the lease itself
                    throw ObjectStoreException.fatal("injected refusal of the lease keeper");
                }
                if (!isStoreDown.get()) {
                    return;
                }
                if (isIndexWrite && COORDINATOR_THREAD.equals(thread) && service.get().getStatus().startsWith("primary, lease lapsed")) {
                    // the node is told to stop while it tries to renew its lapsed lease; the store answers
                    // again from the next request on
                    closer.set(closeInBackground(service.get()));
                    isStoreDown.set(false);
                }
                throw ObjectStoreException.fatal("injected store outage");
            });
            // rows wait for a ten-minute window; the lease lasts 2.5 s
            try (Node node = newNode("p", primaryConfig().setKeepaliveInterval(1_000).setThrottleWindowDuration(600_000), store)) {
                service.set(node.service);
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                isStoreDown.set(true);
                TestUtils.assertEventually(() -> Assert.assertNotNull(closer.get()), 30);
                closer.get().join(60_000);
                Assert.assertFalse(closer.get().isAlive());
                // the stop renewed the lease, uploaded the row and released the lease
                final ReplicationIndex index = readIndex(store);
                Assert.assertTrue(index.isReleased());
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertEquals(1, entry(index, "t").getLastTxn());
            }
        });
    }

    @Test
    public void testStopRightAfterTheOwnerWroteAgainEndsTheTakeover() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            putIndex(store, foreignIndex(7, false));
            final AtomicInteger takeovers = new AtomicInteger();
            final AtomicLong getsAtTakeover = new AtomicLong();
            final AtomicReference<Thread> closer = new AtomicReference<>();
            // the owner looks gone after three keepalive intervals of 100 ms
            try (Node node = newNode("b", primaryConfig().setKeepaliveInterval(100), store)) {
                allowPromotion(node);
                node.service.setTakeoverHookForTesting(() -> {
                    takeovers.incrementAndGet();
                    // Before the node reads the index its takeover is based on, the owner turns out to be
                    // alive and renews its lease, and the node is told to stop.
                    putIndex(store, foreignIndex(8, false));
                    closer.set(closeInBackground(node.service));
                    getsAtTakeover.set(store.getGetCount());
                });
                node.service.start();
                TestUtils.assertEventually(() -> Assert.assertNotNull(closer.get()), 30);
                closer.get().join(60_000);
                Assert.assertFalse(closer.get().isAlive());
                // it read the owner's new index once and stopped instead of waiting for the lease again
                Assert.assertEquals(1, takeovers.get());
                Assert.assertEquals(1, store.getGetCount() - getsAtTakeover.get());
                Assert.assertEquals("waiting for lease, owner=" + FOREIGN_OWNER, node.service.getStatus());
                Assert.assertEquals(0, store.getPutCount());
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                Assert.assertTrue(Files.exists(node.marker()));
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(FOREIGN_OWNER, index.getOwnerInstanceId());
                Assert.assertEquals(8, index.getSequence());
            }
        });
    }

    @Test
    public void testStopThatGivesUpOnADrainUploadKeepsTheLease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> stuckPrefix = new AtomicReference<>();
            final AtomicReference<Thread> coordinator = new AtomicReference<>();
            final AtomicReference<String> uploadThread = new AtomicReference<>();
            final CountDownLatch never = new CountDownLatch(1);
            store.setFaultHook((op, key) -> {
                if (COORDINATOR_THREAD.equals(Thread.currentThread().getName())) {
                    coordinator.set(Thread.currentThread());
                }
                final String prefix = stuckPrefix.get();
                if (op.equals("put") && prefix != null && key.startsWith(prefix) && stuckPrefix.compareAndSet(prefix, null)) {
                    uploadThread.set(Thread.currentThread().getName());
                    try {
                        // a request that does not return by itself
                        if (!never.await(60, TimeUnit.SECONDS)) {
                            throw new AssertionError("the stop did not interrupt the upload");
                        }
                    } catch (InterruptedException e) {
                        // The stop gave up and shut the pool down. The upload fails only once the
                        // coordinator is gone, which decides on the lease while it still runs.
                        try {
                            coordinator.get().join(60_000);
                        } catch (InterruptedException ignore) {
                            Thread.currentThread().interrupt();
                        }
                        throw ObjectStoreException.retryable("injected request interrupted", e);
                    }
                }
            });
            // rows wait for a ten-minute window, so the stop drains them; it waits 5 s for the coordinator
            try (Node node = newNode("p", primaryConfig().setThrottleWindowDuration(600_000), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                stuckPrefix.set(StoreKeys.tablePrefix(entry(readIndex(store), "t").getKey()) + "wal/");
                node.service.close();
                // the row never reached the store: the stop left the lease to expire
                final ReplicationIndex index = readIndex(store);
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertEquals(0, entry(index, "t").getLastTxn());
                // the drain round uploaded on a pool thread, and the coordinator, which waited for it, is gone
                Assert.assertEquals("qdb-replication-worker", uploadThread.get());
                Assert.assertFalse(coordinator.get().isAlive());
            }
            // the node kept the row for its next start, which uploads it
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                awaitUploaded(node, store, "t", 1);
            }
        });
    }

    @Test
    public void testStopThatGivesUpOnAStuckUploadKeepsTheLease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> stuckPrefix = new AtomicReference<>();
            final CountDownLatch stuck = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            store.setFaultHook((op, key) -> {
                final String prefix = stuckPrefix.get();
                if (op.equals("put") && prefix != null && key.startsWith(prefix)) {
                    stuck.countDown();
                    try {
                        // a request that does not return; the interrupt of the stop ends it
                        if (!release.await(60, TimeUnit.SECONDS)) {
                            throw ObjectStoreException.retryable("injected request that never returned", null);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw ObjectStoreException.retryable("injected request interrupted", e);
                    }
                }
            });
            // no keepalive is due during the test, and the stop waits 5 s for the coordinator
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                stuckPrefix.set(StoreKeys.tablePrefix(entry(readIndex(store), "t").getKey()) + "wal/");
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                Assert.assertTrue(stuck.await(30, TimeUnit.SECONDS));
                // the coordinator waits for the upload; the stop gives up on it and interrupts it
                node.service.close();
                Assert.assertEquals(0, node.service.getUploadedTxn(node.engine.verifyTableName("t")));
                final ReplicationIndex index = readIndex(store);
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
                Assert.assertEquals(0, entry(index, "t").getLastTxn());
            } finally {
                release.countDown();
            }
            // the node kept the transaction for its next start, which uploads it
            stuckPrefix.set(null);
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                awaitUploaded(node, store, "t", 1);
                Assert.assertEquals(1, entry(readIndex(store), "t").getLastTxn());
            }
        });
    }

    @Test
    public void testStopWhileTheTakeoverCommitFailsEndsTheTakeover() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<ReplicationService> service = new AtomicReference<>();
            final AtomicReference<Thread> closer = new AtomicReference<>();
            store.setFaultHook((op, key) -> {
                if (op.equals("put") && key.equals(IndexClient.INDEX_KEY) && closer.get() == null) {
                    // the node is told to stop while its takeover commit is in flight, and the store refuses it
                    closer.set(closeInBackground(service.get()));
                    throw ObjectStoreException.fatal("injected refusal");
                }
            });
            try (Node node = newNode("p", primaryConfig(), store)) {
                service.set(node.service);
                node.service.start();
                TestUtils.assertEventually(() -> Assert.assertNotNull(closer.get()), 30);
                closer.get().join(60_000);
                Assert.assertFalse(closer.get().isAlive());
                // one attempt, and no other after the stop
                Assert.assertEquals(1, store.getPutCount());
                Assert.assertFalse(store.contains(IndexClient.INDEX_KEY));
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                Assert.assertNull(node.engine.getFenceReason());
                TestUtils.assertContains(node.service.getStatus(), "primary catching up, object store error: ");
                TestUtils.assertContains(node.service.getStatus(), "injected refusal");
            }
        });
    }

    @Test
    public void testStopWhileWaitingForTheLeaseLeavesTheStoreAlone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            // another primary holds the lease and renewed it a moment ago: the node waits three keepalive
            // intervals, three minutes here, before it takes the store over
            putIndex(store, foreignIndex(7, false));
            final AtomicInteger takeovers = new AtomicInteger();
            try (Node node = newNode("b", primaryConfig(), store)) {
                allowPromotion(node);
                node.service.setTakeoverHookForTesting(takeovers::incrementAndGet);
                node.service.start();
                TestUtils.assertEventually(() -> Assert.assertEquals("waiting for lease, owner=" + FOREIGN_OWNER, node.service.getStatus()), 30);
                node.service.close();
                // it stopped waiting and went no further: no takeover, nothing written, the marker kept
                Assert.assertEquals(0, takeovers.get());
                Assert.assertEquals(0, store.getPutCount());
                Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole());
                Assert.assertNull(node.engine.getFenceReason());
                Assert.assertTrue(Files.exists(node.marker()));
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(FOREIGN_OWNER, index.getOwnerInstanceId());
                Assert.assertEquals(7, index.getSequence());
                Assert.assertFalse(index.isReleased());
            }
        });
    }

    @Test
    public void testStopWithALapsedLeaseUploadsNothing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicBoolean isStoreDown = new AtomicBoolean();
            final AtomicBoolean isRecording = new AtomicBoolean();
            final List<String> requests = Collections.synchronizedList(new ArrayList<>());
            store.setFaultHook((op, key) -> {
                if (isRecording.get()) {
                    requests.add(op + ' ' + key);
                }
                if (isStoreDown.get()) {
                    throw ObjectStoreException.fatal("injected store outage");
                }
            });
            // rows wait for a ten-minute window; the lease lasts 2.5 s
            try (Node node = newNode("p", primaryConfig().setKeepaliveInterval(1_000).setThrottleWindowDuration(600_000), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                isStoreDown.set(true);
                TestUtils.assertEventually(() -> Assert.assertEquals(EngineRole.PRIMARY_CATCHUP, node.engine.getRole()), 30);
                final ReplicationIndex atLapse = readIndex(store);
                isRecording.set(true);
                node.service.close();
                // the stop tried to renew the lease and, refused, uploaded nothing and released nothing
                synchronized (requests) {
                    Assert.assertFalse(requests.isEmpty());
                    for (String request : requests) {
                        Assert.assertEquals("put " + IndexClient.INDEX_KEY, request);
                    }
                }
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(atLapse.getSequence(), index.getSequence());
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(0, entry(index, "t").getLastTxn());
            }
            // the node kept the row for its next start, which uploads it
            isRecording.set(false);
            isStoreDown.set(false);
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                awaitUploaded(node, store, "t", 1);
            }
        });
    }

    @Test
    public void testTableCreatedWhileTheNodeDrainsKeepsTheLeaseHeld() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicReference<String> drainPrefix = new AtomicReference<>();
            final AtomicReference<ReplicatedCairoEngine> engine = new AtomicReference<>();
            final AtomicReference<Throwable> createFailure = new AtomicReference<>();
            store.setFaultHook((op, key) -> {
                final String prefix = drainPrefix.get();
                if (op.equals("put") && prefix != null && key.startsWith(prefix) && drainPrefix.compareAndSet(prefix, null)) {
                    // a client creates a table while the stop uploads the last rows
                    final Thread client = new Thread(() -> {
                        try {
                            engine.get().execute("CREATE TABLE late (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                        } catch (Throwable th) {
                            createFailure.set(th);
                        } finally {
                            io.questdb.std.str.Path.clearThreadLocals();
                        }
                    }, "test-client");
                    client.start();
                    try {
                        client.join();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            // rows wait for a ten-minute window, so the stop drains them
            try (Node node = newNode("p", primaryConfig().setThrottleWindowDuration(600_000), store)) {
                engine.set(node.engine);
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                awaitUploaded(node, store, "t", 0);
                node.engine.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                drainPrefix.set(StoreKeys.tablePrefix(entry(readIndex(store), "t").getKey()) + "wal/");
                node.service.close();
                Assert.assertNull(createFailure.get());
                Assert.assertNotNull(node.engine.getTableTokenIfExists("late"));
                // the stop uploaded the row, but not the table created meanwhile: it does not release the lease
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(1, entry(index, "t").getLastTxn());
                Assert.assertNull(entry(index, "late"));
                Assert.assertFalse(index.isReleased());
                Assert.assertEquals(node.service.getInstanceId(), index.getOwnerInstanceId());
            }
            // the next start uploads the table
            try (Node node = newNode("p", primaryConfig(), store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                awaitUploaded(node, store, "late", 0);
            }
        });
    }

    @Test
    public void testTakeoverFencesWhenTheStoreChangedToATableItCannotCatchUpWith() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            // the store's previous primary stopped and released its lease
            putIndex(store, foreignIndex(3, true));
            final AtomicInteger takeovers = new AtomicInteger();
            try (Node node = newNode("b", primaryConfig(), store)) {
                allowPromotion(node);
                node.service.setTakeoverHookForTesting(() -> {
                    if (takeovers.incrementAndGet() == 1) {
                        // Before the node reads the index its takeover is based on, the previous primary comes
                        // back, commits a table that it had before an operator enabled replication, and stops
                        // again. The store holds that table from transaction 5: no node can build it from the store.
                        final ReplicationIndex idx = foreignIndex(4, true);
                        final IndexTable legacy = new IndexTable().of(999, 1_700_000_000_000_000L, "legacy~999", "legacy", IndexTable.KIND_TABLE, 1_000);
                        legacy.setFirstTxn(5);
                        legacy.setLastTxn(7);
                        idx.addTable(legacy);
                        putIndex(store, idx);
                    }
                });
                node.service.start();
                awaitRole(node, EngineRole.FENCED);
                Assert.assertEquals("tables could not catch up with the object store; see the log", node.engine.getFenceReason());
                Assert.assertEquals(1, takeovers.get());
                // it wrote nothing, kept its marker for a later attempt, and created no table
                Assert.assertEquals(0, store.getPutCount());
                Assert.assertTrue(Files.exists(node.marker()));
                Assert.assertNull(node.engine.getTableTokenIfExists("legacy"));
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(FOREIGN_OWNER, index.getOwnerInstanceId());
                Assert.assertEquals(4, index.getSequence());
                Assert.assertTrue(index.isReleased());
                assertWriteRefused(node.engine, "CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            }
        });
    }

    @Test
    public void testUnreadableCreateTimestampBecomesZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final SequencerLogFacade ff = new SequencerLogFacade();
            try (Node node = newNode("p", primaryConfig(), store, ff, false)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                // the header of the new table's sequencer log reads as no timestamp
                ff.makeNextUnreadable("/late~");
                node.engine.execute("CREATE TABLE late (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                node.engine.execute("INSERT INTO late VALUES ('2024-01-01', 1)");
                awaitUploaded(node, store, "late", 1);
                Assert.assertEquals(1, ff.unreadableReads.get());
                // the store keys the table by create timestamp 0
                final IndexTable late = entry(readIndex(store), "late");
                Assert.assertEquals(0, late.getCreateTimestamp());
                final TableToken token = node.engine.verifyTableName("late");
                Assert.assertEquals(IndexTable.key(token.getTableId(), 0), late.getKey());
                Assert.assertTrue(store.contains(StoreKeys.create(late.getKey())));
            }
        });
    }

    @Test
    public void testUnreadableSequencerLogDuringTheTakeoverFencesTheNode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            final AtomicBoolean isCounting = new AtomicBoolean();
            final AtomicInteger cleanerRuns = new AtomicInteger();
            store.setFaultHook((op, key) -> {
                if (isCounting.get() && op.equals("list") && key.equals(StoreKeys.CHECKPOINTS_PREFIX)) {
                    cleanerRuns.incrementAndGet();
                }
            });
            // Structure changes wait for the window too, and the window is ten minutes: a new table stays
            // local, pass after pass of the coordinator. Each pass ends with a run of the cleaner, which
            // reads the checkpoint histories.
            final ReplicationConfigurationImpl config = primaryConfig()
                    .setKeepaliveInterval(1_000)
                    .setThrottleWindowDuration(600_000)
                    .setThrottleNonData(true)
                    .setCleanerEnabled(true)
                    .setCleanerInterval(20);
            try (Node node = newNode("p", config, store)) {
                node.service.start();
                awaitRole(node, EngineRole.PRIMARY);
                node.engine.execute("CREATE TABLE late (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                isCounting.set(true);
                // the second run ends a pass that began after the table existed
                TestUtils.assertEventually(() -> Assert.assertTrue(cleanerRuns.get() >= 2), 30);
                Assert.assertNull(entry(readIndex(store), "late"));
                Assert.assertEquals(-1, node.service.getUploadedTxn(node.engine.verifyTableName("late")));
                node.service.crashForTesting();
            }
            final ReplicationIndex atCrash = readIndex(store);
            Assert.assertEquals(0, atCrash.getTables().size());
            // the restarted node registers the table the store lacks, and cannot open its sequencer log
            final SequencerLogFacade ff = new SequencerLogFacade();
            ff.failNextOpen("/late~");
            try (Node node = newNode("p", primaryConfig(), store, ff, false)) {
                node.service.start();
                awaitRole(node, EngineRole.FENCED);
                final String reason = node.engine.getFenceReason();
                TestUtils.assertContains(reason, "replication coordinator failed: ");
                TestUtils.assertContains(reason, "could not open sequencer log [path=");
                TestUtils.assertContains(reason, "/late~");
                Assert.assertEquals(1, ff.openFailures.get());
                // it never took the store over
                final ReplicationIndex index = readIndex(store);
                Assert.assertEquals(atCrash.getSequence(), index.getSequence());
                Assert.assertEquals(0, index.getTables().size());
            }
        });
    }

    // lets the node take over a store it does not own or is behind, on its next start
    private static void allowPromotion(Node node) throws IOException {
        Files.createFile(node.marker());
    }

    private static void assertWriteRefused(ReplicatedCairoEngine engine, String sql) throws Exception {
        try {
            engine.execute(sql);
            Assert.fail("expected a read-only refusal: " + sql);
        } catch (CairoException | SqlException e) {
            TestUtils.assertContains(((FlyweightMessageContainer) e).getFlyweightMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        }
    }

    private static void awaitReplayed(Node replica, String table, long txn) throws Exception {
        TestUtils.assertEventually(() -> {
            final TableToken token = replica.engine.getTableTokenIfExists(table);
            Assert.assertNotNull(token);
            Assert.assertEquals(txn, replica.engine.getTableSequencerAPI().lastTxn(token));
        }, 30);
    }

    private static void awaitRole(Node node, EngineRole role) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertEquals(role, node.engine.getRole()), 30);
    }

    // Waits until the index in the store names the table's transactions up to txn. The uploader of a new
    // table reports transaction 0 before the store holds the table, so the index is what counts.
    private static void awaitUploaded(Node node, MemoryObjectStore store, String table, long txn) throws Exception {
        TestUtils.assertEventually(() -> {
            final IndexTable entry = entry(readIndex(store), table);
            Assert.assertNotNull(entry);
            Assert.assertEquals(txn, entry.getLastTxn());
            Assert.assertEquals(txn, node.service.getUploadedTxn(node.engine.verifyTableName(table)));
        }, 30);
    }

    private static CheckpointHistory.Checkpoint checkpoint(String tableDir, long seqTxn) {
        final CheckpointHistory.Checkpoint c = new CheckpointHistory.Checkpoint(seqTxn * 1_000_000L);
        c.addTable(tableDir, seqTxn);
        return c;
    }

    // runs close() on a thread of its own and returns once close() has told the coordinator to stop
    private static Thread closeInBackground(ReplicationService service) {
        final CountDownLatch stopping = new CountDownLatch(1);
        service.setCloseHookForTesting(stopping::countDown);
        final Thread closer = new Thread(() -> {
            try {
                service.close();
            } finally {
                io.questdb.std.str.Path.clearThreadLocals();
            }
        }, "test-closer");
        closer.start();
        try {
            if (!stopping.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("close() did not begin");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for close() to begin", e);
        }
        return closer;
    }

    private static void copyTree(String from, String to) throws IOException {
        TestUtils.removeTestPath(to);
        final java.nio.file.Path src = Paths.get(from);
        final java.nio.file.Path dst = Paths.get(to);
        try (Stream<java.nio.file.Path> files = Files.walk(src)) {
            for (java.nio.file.Path f : (Iterable<java.nio.file.Path>) files::iterator) {
                final java.nio.file.Path target = dst.resolve(src.relativize(f).toString());
                if (Files.isDirectory(f)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(f, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static int countKeys(MemoryObjectStore store, String prefix) {
        return store.keys(k -> k.startsWith(prefix)).size();
    }

    // the entry of the oldest table of that name, or null, also when there is no index
    private static IndexTable entry(ReplicationIndex index, String tableName) {
        if (index == null) {
            return null;
        }
        for (int i = 0, n = index.getTables().size(); i < n; i++) {
            final IndexTable t = index.getTables().getQuick(i);
            if (t.getTableName().equals(tableName)) {
                return t;
            }
        }
        return null;
    }

    // the store's object for the bytes, as the replication services write them
    private static byte[] envelope(byte[] text) {
        try (NativeBuffer raw = new NativeBuffer(); ObjectEnvelope envelope = new ObjectEnvelope()) {
            raw.putBytes(text, 0, text.length);
            final ByteSink sink = new ByteSink();
            envelope.encode(raw.ptr(), raw.size(), 1, false, sink);
            return sink.toByteArray();
        }
    }

    // an index that another node wrote: its lease, and the database the store belongs to
    private static ReplicationIndex foreignIndex(long sequence, boolean isReleased) {
        final ReplicationIndex idx = new ReplicationIndex();
        idx.setDataId(DATA_ID_HI, DATA_ID_LO);
        idx.setOwner(FOREIGN_OWNER, 3, 1_700_000_000_000_000L, sequence, isReleased);
        return idx;
    }

    private static Node newNode(String name, ReplicationConfigurationImpl config, ObjectStore store) {
        return newNode(name, config, store, TestFilesFacadeImpl.INSTANCE, false);
    }

    // an engine and its replication service, wired as ServerMain wires them; the caller starts the service
    private static Node newNode(String name, ReplicationConfigurationImpl config, ObjectStore store, FilesFacade ff, boolean isReadOnly) {
        final String dir = nodeDir(name);
        TestUtils.createTestPath(dir + File.separator + "db");
        final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(dir + File.separator + "db", dir) {
            @Override
            public int getCommitMode() {
                return CommitMode.NOSYNC;
            }

            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public boolean isReadOnlyInstance() {
                return isReadOnly;
            }
        };
        final ReplicatedCairoEngine engine = new ReplicatedCairoEngine(configuration, new QdbrWalLocker(), failure -> {
        }, config);
        ReplicationService service = null;
        try {
            service = new ReplicationService(engine, store);
            // before completeInit(): no sequencer may open without the replication WAL policy
            service.installHooks();
            engine.completeInit();
            engine.load();
            engine.buildViewGraphs();
            return new Node(dir, engine, service);
        } catch (Throwable th) {
            Misc.free(service);
            engine.close();
            throw th;
        }
    }

    private static String nodeDir(String name) {
        return root + File.separator + name;
    }

    // a primary that writes the index only when it has something to commit: within a test, no keepalive
    // is due and the lease keeper renews nothing, unless the test shortens the interval; the stop waits
    // 5 s for its coordinator
    private static ReplicationConfigurationImpl primaryConfig() {
        return new ReplicationConfigurationImpl()
                .setRole(ReplicationRole.PRIMARY)
                .setKeepaliveInterval(60_000)
                .setThrottleWindowDuration(10)
                .setRetryInterval(10)
                .setRequestBaseTimeout(100)
                .setCleanerEnabled(false);
    }

    // the checkpoint history of a backup instance, which keeps the given checkpoints
    private static void putCheckpointHistory(MemoryObjectStore store, CheckpointHistory.Checkpoint... checkpoints) {
        final CheckpointHistory history = new CheckpointHistory();
        for (CheckpointHistory.Checkpoint c : checkpoints) {
            history.add(c);
        }
        store.putRaw(StoreKeys.checkpointHistory("backup"), envelope(history.encode()));
    }

    // replaces the index as another writer would, past the store's fault hook and conditions
    private static void putIndex(MemoryObjectStore store, ReplicationIndex idx) {
        store.putRaw(IndexClient.INDEX_KEY, envelope(idx.encode()));
    }

    // the index the store holds, read past its fault hook
    private static ReplicationIndex readIndex(MemoryObjectStore store) {
        final byte[] stored = store.getBytes(IndexClient.INDEX_KEY);
        if (stored == null) {
            return null;
        }
        try (NativeBuffer raw = new NativeBuffer(); ObjectEnvelope envelope = new ObjectEnvelope()) {
            envelope.decode(stored, stored.length, raw);
            final byte[] text = new byte[(int) raw.size()];
            Unsafe.copyMemory(null, raw.ptr(), text, Unsafe.BYTE_OFFSET, text.length);
            return ReplicationIndex.decode(text, 0, text.length);
        }
    }

    private static ReplicationConfigurationImpl replicaConfig() {
        return new ReplicationConfigurationImpl()
                .setRole(ReplicationRole.REPLICA)
                .setReplicaPollInterval(10)
                .setRetryInterval(10)
                .setRequestBaseTimeout(100);
    }

    // the transaction log parts of a table in the store
    private static String seqPrefix(ReplicationIndex index, String tableName) {
        return StoreKeys.tablePrefix(entry(index, tableName).getKey()) + "seq/";
    }

    // the operator deletes the marker, and takes back the promotion it allowed
    private static void withdrawPromotion(Node node) {
        try {
            Files.delete(node.marker());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class Node implements QuietCloseable {
        private final String dir;
        private final ReplicatedCairoEngine engine;
        private final ReplicationService service;

        private Node(String dir, ReplicatedCairoEngine engine, ReplicationService service) {
            this.dir = dir;
            this.engine = engine;
            this.service = service;
        }

        @Override
        public void close() {
            // ServerMain frees the service first: the engine outlives it
            Misc.free(service);
            Misc.free(engine);
        }

        // the file that allows one promotion
        private java.nio.file.Path marker() {
            return Paths.get(dir, ReplicationService.MIGRATE_PRIMARY_FILE);
        }
    }

    // fails, or makes unreadable, the next sequencer log that the replication coordinator opens for
    // reading in a table directory whose name holds the given text; or fails every such open
    private static final class SequencerLogFacade extends TestFilesFacadeImpl {
        private final AtomicInteger openFailures = new AtomicInteger();
        private final AtomicInteger unreadableReads = new AtomicInteger();
        private volatile boolean isOpenFailing;
        private volatile boolean isOpenFailingAlways;
        private volatile String tableDirPart;
        private volatile long unreadableFd = -1;

        @Override
        public boolean close(long fd) {
            if (fd == unreadableFd) {
                unreadableFd = -1;
            }
            return super.close(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            final String part = tableDirPart;
            if (part == null || !COORDINATOR_THREAD.equals(Thread.currentThread().getName())
                    || !Utf8s.containsAscii(name, part) || !Utf8s.endsWithAscii(name, SEQUENCER_LOG)) {
                return super.openRO(name);
            }
            if (!isOpenFailingAlways) {
                tableDirPart = null;
            }
            if (isOpenFailing) {
                openFailures.incrementAndGet();
                return -1;
            }
            final long fd = super.openRO(name);
            unreadableFd = fd;
            return fd;
        }

        @Override
        public long readNonNegativeLong(long fd, long offset) {
            if (fd == unreadableFd) {
                unreadableReads.incrementAndGet();
                return -1;
            }
            return super.readNonNegativeLong(fd, offset);
        }

        private void failNextOpen(String tableDirPart) {
            isOpenFailing = true;
            isOpenFailingAlways = false;
            this.tableDirPart = tableDirPart;
        }

        private void failOpens(String tableDirPart) {
            isOpenFailing = true;
            isOpenFailingAlways = true;
            this.tableDirPart = tableDirPart;
        }

        private void makeNextUnreadable(String tableDirPart) {
            isOpenFailing = false;
            isOpenFailingAlways = false;
            this.tableDirPart = tableDirPart;
        }
    }
}
