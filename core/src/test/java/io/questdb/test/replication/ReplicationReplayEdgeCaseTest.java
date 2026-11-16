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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableSequencerImpl;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.replication.ReplicaSegmentPins;
import io.questdb.replication.ReplicaTableApplier;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationException;
import io.questdb.replication.ReplicationRole;
import io.questdb.replication.ReplicationWalDirectoryPolicy;
import io.questdb.replication.ReplicationWalListener;
import io.questdb.replication.ReplicationWorker;
import io.questdb.replication.SegmentDeltaBuilder;
import io.questdb.replication.SegmentDeltaWriter;
import io.questdb.replication.StoreKeys;
import io.questdb.replication.TableReplayer;
import io.questdb.replication.TableUploader;
import io.questdb.replication.TxnLogReader;
import io.questdb.replication.WalCleaner;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.ObjectEnvelope;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.replication.codec.TableCreateManifest;
import io.questdb.replication.codec.TxnPart;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.replication.store.ByteSink;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Edge cases of the replica's replay and apply, including the repair of an unapplied WAL tail; the
 * primary's uploader, WAL listener and directory policy; the transaction log reader, the WAL cleaner
 * and the segment delta builder; the sequencer hooks replication uses; and the engine's role fencing.
 */
public class ReplicationReplayEdgeCaseTest extends AbstractTest {
    private static final long EPOCH = 1;
    // what uploadSample() writes, as every node serves it once applied
    private static final String SAMPLE_ROWS = """
            ts\tv\ts\tx
            2024-01-01T00:00:00.000000Z\t1\ta\tnull
            2024-01-02T00:00:00.000000Z\t2\t\tnull
            2024-01-03T00:00:00.000000Z\t3\tc\t3
            """;

    @Test
    public void testApplierRefusesStaleTokensAndOutOfPlaceTransactions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("stale-a");
            final String b = newDir("stale-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                primary.execute("ALTER TABLE t ADD COLUMN x INT");
                primary.execute("RENAME TABLE t TO u");
                primary.execute("INSERT INTO u VALUES ('2024-01-02', 2, 2)");
                final IndexTable entry = upload(primary, pw, "u", "t", 100);
                Assert.assertEquals(4, entry.getLastTxn());

                // the replica replays the insert, the new column and the rename
                final TableReplayer replayer = new TableReplayer(entry.getKey());
                Assert.assertEquals(3, replayer.replay(replica, entry, rw, null, 3));
                final TableToken renamed = replica.verifyTableName("u");
                final TableSequencerAPI api = replica.getTableSequencerAPI();
                Assert.assertEquals(3, api.lastTxn(renamed));
                final TxnPart part = decodePart(store.getBytes(StoreKeys.seqPart(entry.getKey(), 0, EPOCH)));
                final ReplicaTableApplier applier = rw.getTableApplier();

                // the new column shipped again after the log moved past it: the sequencer's fence refuses it
                final TxnRecord addColumn = part.getRecord(2);
                Assert.assertTrue(addColumn.isStructural());
                assertReplication(() -> applier.apply(renamed, addColumn), ReplicationException.Kind.DIVERGENCE,
                        "replica transaction log does not end before the shipped transaction [table=u, txn=2, local=3]");
                Assert.assertEquals(3, api.lastTxn(renamed));

                // a token from before the rename: the sequencer knows the table by its new name only
                final TxnRecord insert = part.getRecord(4);
                Assert.assertTrue(insert.isData());
                try {
                    applier.apply(renamed.renamed("t"), insert);
                    Assert.fail("expected the stale token to be refused");
                } catch (TableReferenceOutOfDateException e) {
                    TestUtils.assertContains(e.getMessage(), "[table=t]");
                }
                Assert.assertEquals(3, api.lastTxn(renamed));

                // the replay carries on from where the log ends, and the table holds the primary's rows, with
                // the new column once only
                Assert.assertEquals(1, replayer.replay(replica, entry, rw, null, 100));
                Assert.assertNull(replayer.getStaleReason());
                Assert.assertEquals(4, api.lastTxn(renamed));
                TestUtils.drainWalQueue(replica);
                Assert.assertFalse(api.isSuspended(renamed));
                Assert.assertEquals("""
                        ts\tv\tx
                        2024-01-01T00:00:00.000000Z\t1\tnull
                        2024-01-02T00:00:00.000000Z\t2\t2
                        """, rows(replica, "u", "ts"));

                // the next transaction under a structure version the table never had
                final TxnRecord unknownVersion = new TxnRecord().ofData(5, insert.getWalId(), insert.getSegmentId(),
                        insert.getSegmentTxn() + 1, 99, insert.getCommitTimestamp(), Long.MAX_VALUE, -1, 0);
                assertReplication(() -> applier.apply(renamed, unknownVersion), ReplicationException.Kind.DIVERGENCE,
                        "structure version differs from the primary's [table=u, txn=5, structureVersion=99]");
                Assert.assertEquals(4, api.lastTxn(renamed));

                // Long.MIN_VALUE - 1 wraps around to Long.MAX_VALUE, which the sequencer reads as "after any
                // last transaction": it appends, and the applier refuses the number the sequencer hands back
                final TxnRecord wrapped = new TxnRecord().ofData(Long.MIN_VALUE, insert.getWalId(), insert.getSegmentId(),
                        insert.getSegmentTxn() + 1, insert.getStructureVersion(), insert.getCommitTimestamp(), Long.MAX_VALUE, -1, 0);
                assertReplication(() -> applier.apply(renamed, wrapped), ReplicationException.Kind.DIVERGENCE,
                        "transaction number differs from the primary's [table=u, expected=" + Long.MIN_VALUE + ", actual=5]");
            }
        });
    }

    @Test
    public void testCorruptDeltaMakesTheTableStale() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("corrupt-delta-a");
            final String b = newDir("corrupt-delta-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                final IndexTable entry = uploadSample(primary, pw);
                final String key = entry.getKey();
                final TxnRecord first = decodePart(store.getBytes(StoreKeys.seqPart(key, 0, EPOCH))).getRecord(1);
                final int walId = first.getWalId();
                final int segmentId = first.getSegmentId();
                final int lo = first.getDeltaFirstSegmentTxn();
                final int hi = first.getDeltaLastSegmentTxn();
                final String deltaKey = StoreKeys.segmentDelta(key, walId, segmentId, lo, hi, first.getDeltaEpoch());
                final byte[] goodDelta = store.getBytes(deltaKey);
                final String corrupt = "corrupt WAL delta [key=" + deltaKey + ", error=";

                // a header that does not decode
                store.putRaw(deltaKey, envelope(raw -> raw.putInt(0)));
                assertStale(replica, entry, rw, corrupt + "not a segment delta");
                // a header that matches the record, then bytes that no range accounts for
                store.putRaw(deltaKey, envelope(raw -> {
                    SegmentDelta.begin(raw, walId, segmentId, lo, hi, 0);
                    raw.putByte((byte) 0);
                }));
                assertStale(replica, entry, rw, corrupt + "trailing bytes in segment delta");
                // a range whose file name leaves the segment directory
                store.putRaw(deltaKey, envelope(raw -> {
                    SegmentDelta.begin(raw, walId, segmentId, lo, hi, 0);
                    SegmentDelta.appendRange(raw, "../" + WalUtils.EVENT_FILE_NAME, 0, 0);
                }));
                assertStale(replica, entry, rw, corrupt + "invalid file name in segment delta");
                store.putRaw(deltaKey, goodDelta);

                // none of them left anything behind: a replay of the intact store brings the whole table
                final TableReplayer ok = new TableReplayer(key);
                Assert.assertEquals(3, ok.replay(replica, entry, rw, null, 100));
                Assert.assertNull(ok.getStaleReason());
                TestUtils.drainWalQueue(replica);
                Assert.assertEquals(SAMPLE_ROWS, rows(replica, "t", "ts"));
            }
        });
    }

    @Test
    public void testCreateTableWaitsForTheTableCreateLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("create-lock-a");
            final String b = newDir("create-lock-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 CreateLockWatchingEngine replica = start(new CreateLockWatchingEngine(new NodeConfiguration(b), b), ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableCreateManifest manifest = uploadCreate(primary, pw, store, "t");
                replica.watchedDir = manifest.getDirName();

                // another thread creating a table of the same name holds the engine's create lock
                final TableToken holder = new TableToken("t", "holder~1", null, 1_000, true, false, false);
                Assert.assertTrue(replica.lockTableCreate(holder));
                final AtomicReference<TableToken> created = new AtomicReference<>();
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread thread = new Thread(() -> {
                    try {
                        created.set(rw.getTableApplier().createTable(manifest));
                    } catch (Throwable th) {
                        failure.set(th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                });
                thread.start();
                try {
                    // two refused attempts: the applier waited for the lock in between, and registered nothing
                    Assert.assertTrue(replica.refusals.await(30, TimeUnit.SECONDS));
                    Assert.assertNull(replica.getTableTokenByDirName(manifest.getDirName()));
                } finally {
                    replica.unlockTableCreate(holder);
                    thread.join(60_000);
                }
                Assert.assertFalse(thread.isAlive());
                Assert.assertNull(String.valueOf(failure.get()), failure.get());
                Assert.assertEquals(created.get(), replica.getTableTokenByDirName(manifest.getDirName()));
                Assert.assertEquals("ts,v", columnNames(replica, "t"));
                // and released the lock once the table was registered
                final TableToken next = new TableToken("t", "next~2", null, 1_001, true, false, false);
                Assert.assertTrue(replica.lockTableCreate(next));
                replica.unlockTableCreate(next);
            }
        });
    }

    @Test
    public void testDegeneratePathsAreNeverPinned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("policy");
            // only WAL tables replicate: never a non-WAL, system, view, materialized view or live view table
            Assert.assertTrue(ReplicationWalDirectoryPolicy.isReplicated(token("t", "t~1", 1, TableToken.Type.TABLE, true, false)));
            Assert.assertFalse(ReplicationWalDirectoryPolicy.isReplicated(token("nw", "nw~2", 2, TableToken.Type.TABLE, false, false)));
            Assert.assertFalse(ReplicationWalDirectoryPolicy.isReplicated(token("sys.t", "sys.t~3", 3, TableToken.Type.TABLE, true, true)));
            Assert.assertFalse(ReplicationWalDirectoryPolicy.isReplicated(token("v", "v~4", 4, TableToken.Type.VIEW, true, false)));
            Assert.assertFalse(ReplicationWalDirectoryPolicy.isReplicated(token("mv", "mv~5", 5, TableToken.Type.MAT_VIEW, true, false)));
            Assert.assertFalse(ReplicationWalDirectoryPolicy.isReplicated(token("lv", "lv~6", 6, TableToken.Type.LIVE_VIEW, true, false)));

            Assert.assertTrue(new File(dir, "t~1/wal1/0").mkdirs());
            try (Path path = new Path();
                 ReplicationWalDirectoryPolicy primary = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, (tableDir, partId) -> true, null)) {
                // a directory created without its table's token gets no marker
                path.of(dir).concat("t~1").concat("wal1").concat("0");
                primary.initDirectory(path, null);
                Assert.assertFalse(primary.isInUse(path));
                Assert.assertFalse(new File(dir, "t~1/wal1/0/" + ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER).exists());
                // a sequencer path that names no table directory is never pinned, whatever the uploader needs
                Assert.assertFalse(primary.isSeqPartInUse(path.of("/"), 0));
            }
            final ReplicaSegmentPins pins = new ReplicaSegmentPins(60_000);
            try (Path path = new Path();
                 ReplicationWalDirectoryPolicy replica = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, null, pins)) {
                final long now = System.currentTimeMillis();
                pins.onDownloaded("t~1", 3, 2, now);
                // the empty table directory name that a path without one would resolve to
                pins.onDownloaded("", 3, 2, now);
                Assert.assertTrue(replica.isInUse(path.of(dir).concat("t~1").concat("wal3").concat("2")));
                // paths that name no table directory: a root, and a segment with nothing above its WAL
                Assert.assertFalse(replica.isInUse(path.of("/")));
                Assert.assertFalse(replica.isInUse(path.of("wal3/2")));
                Assert.assertFalse(replica.isInUse(path.of("/wal3/2")));
            } finally {
                pins.close();
            }
        });
    }

    @Test
    public void testEmptySystemPrefixKeepsReplicatedTablesReadOnly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final NodeConfiguration configuration = new NodeConfiguration(newDir("empty-prefix-writes"));
            configuration.systemTableNamePrefix = "";
            try (ReplicatedCairoEngine engine = newEngine(configuration, ReplicationRole.PRIMARY);
                 Path path = new Path()) {
                engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken t = engine.verifyTableName("t");
                // every name starts with an empty prefix, so the token of a client table reads as a system table
                Assert.assertTrue(t.isSystem());

                engine.setRole(EngineRole.REPLICA, null);
                // the read-only node still refuses every client write to it
                assertReadOnly(() -> engine.getWalWriter(t).close());
                assertReadOnly(() -> engine.getTableWriterAPI(t, "test").close());
                assertReadOnly(() -> engine.getTableWriterAPI("t", "test").close());
                assertReadOnly(() -> engine.getWriter(t, "test").close());
                assertReadOnly(() -> engine.dropTableOrViewOrMatView(path.of(engine.getConfiguration().getDbRoot()), t));
                Assert.assertEquals(t, engine.getTableTokenIfExists("t"));
            }
        });
    }

    @Test
    public void testEmptySystemPrefixMakesNoTableNodeLocal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final NodeConfiguration configuration = new NodeConfiguration(newDir("empty-prefix"));
            configuration.systemTableNamePrefix = "";
            try (ReplicatedCairoEngine replica = newEngine(configuration, ReplicationRole.REPLICA);
                 MemoryMARW mem = Vm.getCMARWInstance();
                 Path path = new Path()) {
                // every name starts with an empty prefix: the engine takes none of them for a system table
                assertReadOnly(() -> replica.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, walModel(replica, "sys.local"), false));
                Assert.assertNull(replica.getTableTokenIfExists("sys.local"));
            }
        });
    }

    @Test
    public void testFailedLogFlushDoesNotHideTheFailureThatStoppedTheBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("flush-a");
            final String b = newDir("flush-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            final FaultyFilesFacade ff = new FaultyFilesFacade();
            final NodeConfiguration replicaConfiguration = new NodeConfiguration(b);
            // adaptive: the sequencer pins what the replay appends until the batch flushes the log
            replicaConfiguration.commitMode = CommitMode.ADAPTIVE;
            replicaConfiguration.ff = ff;
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(replicaConfiguration, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                // one transaction per part
                final IndexTable entry = upload(primary, pw, "t", "t", 1);
                final String part1 = StoreKeys.seqPart(entry.getKey(), 1, EPOCH);
                store.putRaw(part1, envelope(raw -> raw.putInt(0)));
                // the flush that ends the batch fails as well, after the corrupt part stopped the batch
                store.setFaultHook((op, key) -> {
                    if (op.equals("get") && key.equals(part1)) {
                        ff.isFdatasyncFailing = true;
                    }
                });

                final TableReplayer replayer = new TableReplayer(entry.getKey());
                Assert.assertEquals(0, replayer.replay(replica, entry, rw, null, 100));
                Assert.assertEquals(1, ff.fdatasyncFailures.get());
                // the table stops replicating for the corrupt part, not for the flush that failed after it
                Assert.assertTrue(replayer.isStale());
                TestUtils.assertContains(replayer.getStaleReason(), "corrupt transaction log part [key=" + part1 + ", error=");
                // the batch appended the first transaction before the second part stopped it
                Assert.assertEquals(1, replica.getTableSequencerAPI().lastTxn(replica.verifyTableName("t")));
            }
        });
    }

    @Test
    public void testFailedUploadMarkerFailsTheDirectory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("marker-failure");
            final AtomicBoolean isMarkerFailing = new AtomicBoolean();
            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (isMarkerFailing.get() && Utf8s.endsWithAscii(name, ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER)) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };
            final String failure = "could not create upload marker [path=";
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicationWalDirectoryPolicy policy = new ReplicationWalDirectoryPolicy(ff, (tableDir, partId) -> false, null)) {
                primary.setWalDirectoryPolicy(policy);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken t = primary.verifyTableName("t");
                final java.nio.file.Path tableDir = Paths.get(primary.getConfiguration().getDbRoot().toString(), t.getDirName());
                Assert.assertTrue(Files.exists(tableDir.resolve(WalUtils.SEQ_DIR).resolve(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER)));

                isMarkerFailing.set(true);
                // a WAL segment the purge job could delete before its upload is never written
                try {
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                    Assert.fail("expected the segment without its marker to fail the write");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), failure);
                }
                Assert.assertEquals(0, primary.getTableSequencerAPI().lastTxn(t));
                // and neither is a table whose sequencer the purge job could delete
                try {
                    primary.execute("CREATE TABLE u (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    Assert.fail("expected the sequencer without its marker to fail the create");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getMessage(), failure);
                }
                Assert.assertNull(primary.getTableTokenIfExists("u"));

                // once markers can be created again, the table takes writes, every segment protected
                isMarkerFailing.set(false);
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                TestUtils.drainWalQueue(primary);
                Assert.assertEquals("ts\tv\n2024-01-01T00:00:00.000000Z\t1\n", rows(primary, "t", "ts"));
                try (Stream<java.nio.file.Path> files = Files.walk(tableDir)) {
                    files.filter(p -> p.getFileName().toString().equals(WalUtils.EVENT_FILE_NAME))
                            .forEach(event -> Assert.assertTrue(String.valueOf(event.getParent()),
                                    Files.exists(event.resolveSibling(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER))));
                }
            }
        });
    }

    @Test
    public void testLiveViewStaysWritableOnReadOnlyNode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ReplicatedCairoEngine engine = newEngine(newDir("live-view"), ReplicationRole.PRIMARY);
                 Path path = new Path()) {
                engine.execute("CREATE TABLE base (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS (SELECT ts, x, count(*) OVER (PARTITION BY x ORDER BY ts "
                        + "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rn FROM base)");
                final TableToken base = engine.verifyTableName("base");
                final TableToken lv = engine.verifyTableName("lv");
                Assert.assertTrue(lv.isLiveView());
                Assert.assertTrue(lv.isWal());

                engine.setRole(EngineRole.REPLICA, null);
                // a replicated table stays fenced, the node's own live view does not
                assertReadOnly(() -> engine.dropTableOrViewOrMatView(path.of(engine.getConfiguration().getDbRoot()), base));
                engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE);
                Assert.assertNull(engine.getTableTokenIfExists("lv"));
                Assert.assertEquals(base, engine.getTableTokenIfExists("base"));
            }
        });
    }

    @Test
    public void testReadOnlyInstanceLockdownOverridesRoleAndLease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final NodeConfiguration primaryConfiguration = new NodeConfiguration(newDir("lockdown-primary"));
            primaryConfiguration.isReadOnly = true;
            try (ReplicatedCairoEngine primary = newEngine(primaryConfiguration, ReplicationRole.PRIMARY)) {
                // a primary holding a current lease is still read-only under the readonly lockdown
                Assert.assertTrue(primary.isPrimaryRole());
                Assert.assertFalse(primary.isLeaseLapsed());
                Assert.assertEquals(EngineRole.PRIMARY, primary.getRole());
                Assert.assertTrue(primary.isReadOnlyMode());
                // and it mints no database id
                Assert.assertFalse(primary.getDataID().isInitialized());
                Assert.assertFalse(primary.isDataIdMintedAtBoot());
            }
            final NodeConfiguration replicaConfiguration = new NodeConfiguration(newDir("lockdown-replica"));
            replicaConfiguration.isReadOnly = true;
            try (ReplicatedCairoEngine replica = newEngine(replicaConfiguration, ReplicationRole.REPLICA)) {
                // the break-glass alters a replica permits stay closed under the lockdown
                Assert.assertEquals(EngineRole.REPLICA, replica.getRole());
                Assert.assertFalse(replica.isForceAlterAllowed());
            }
        });
    }

    @Test
    public void testReadOnlyNodeCreatesAndWritesOnlyNodeLocalTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ReplicatedCairoEngine replica = newEngine(newDir("node-local"), ReplicationRole.REPLICA);
                 MemoryMARW mem = Vm.getCMARWInstance();
                 Path path = new Path()) {
                // the node's system tables and the tables a parquet export materializes are its own
                final TableToken sys = replica.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, walModel(replica, "sys.local"), false);
                Assert.assertTrue(sys.isSystem());
                Assert.assertTrue(sys.isWal());
                replica.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, walModel(replica, "zzz.copy.1"), false,
                        TableUtils.TABLE_KIND_TEMP_PARQUET_EXPORT);
                Assert.assertNotNull(replica.getTableTokenIfExists("zzz.copy.1"));
                // a client table is refused, and so is a structure that names no table at all
                assertReadOnly(() -> replica.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, walModel(replica, "client"), false));
                assertReadOnly(() -> replica.createTable(AllowAllSecurityContext.INSTANCE, mem, path, false, walModel(replica, null), false));
                Assert.assertNull(replica.getTableTokenIfExists("client"));

                // the node writes its system table
                try (WalWriter w = replica.getWalWriter(sys)) {
                    final TableWriter.Row row = w.newRow(1_000_000L);
                    row.putLong(0, 42);
                    row.append();
                    w.commit();
                }
                TestUtils.drainWalQueue(replica);
                Assert.assertEquals("v\tts\n42\t1970-01-01T00:00:01.000000Z\n", rows(replica, "sys.local", "ts"));

                // a table that does not exist is reported as such, not refused as a write
                try {
                    replica.getTableWriterAPI("missing", "test");
                    Assert.fail("expected the missing table to be reported");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "table does not exist [table=missing]");
                }
                // a replica holds no lease that could lapse
                Assert.assertFalse(replica.isLeaseLapsed());
                // setting the role it has changes nothing, and clients still see a new epoch
                final long epoch = replica.getQwpServerInfoProvider().getEpoch();
                replica.setRole(EngineRole.REPLICA, "ignored for this role");
                Assert.assertEquals(epoch + 1, replica.getQwpServerInfoProvider().getEpoch());
                Assert.assertEquals(EngineRole.REPLICA, replica.getRole());
                Assert.assertNull(replica.getFenceReason());
            }
        });
    }

    @Test
    public void testReplayBatchEndingInDropRecordsNothingForTheDroppedTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("drop-a");
            final String b = newDir("drop-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            final NodeConfiguration replicaConfiguration = new NodeConfiguration(b);
            // adaptive: a batch leaves durability pins behind for its end to release
            replicaConfiguration.commitMode = CommitMode.ADAPTIVE;
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(replicaConfiguration, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                primary.execute("CREATE TABLE kept (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO kept VALUES ('2024-01-01', 1)");
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken token = primary.verifyTableName("t");
                final TableUploader u = new TableUploader(token, new IndexTable().of(token.getTableId(), 7, token.getDirName(), "t",
                        IndexTable.KIND_TABLE, 100), false);
                // the store first holds the table without a transaction, then its insert and its drop
                Assert.assertEquals(0, u.upload(primary, pw, EPOCH, 0, 100));
                u.onCommitted();
                final IndexTable created = copy(u.getEntry());
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                primary.execute("DROP TABLE t");
                Assert.assertEquals(2, u.upload(primary, pw, EPOCH, 2, 100));
                u.onCommitted();
                Assert.assertTrue(u.isDropCommitted());
                final IndexTable dropped = copy(u.getEntry());

                // the replica creates the table while the store names it alive
                final TableReplayer replayer = new TableReplayer(created.getKey());
                Assert.assertEquals(0, replayer.replay(replica, created, rw, null, 100));
                final TableToken replicaToken = replica.verifyTableName("t");
                // a zero budget replays nothing
                Assert.assertEquals(0, replayer.replay(replica, dropped, rw, null, 0));
                Assert.assertEquals(0, replica.getTableSequencerAPI().lastTxn(replicaToken));
                // one batch brings the insert and the drop
                Assert.assertEquals(2, replayer.replay(replica, dropped, rw, null, 100));
                Assert.assertTrue(replayer.isDone());
                Assert.assertNull(replayer.getStaleReason());
                Assert.assertNull(replica.getTableTokenIfExists("t"));
                // no download statistics for a table that is gone
                Assert.assertNull(replica.getRecentWriteTracker().getWriteStats(replicaToken));
                Assert.assertEquals(0, replayer.replay(replica, dropped, rw, null, 100));

                // a batch that leaves its table alive records them
                final IndexTable keptEntry = upload(primary, pw, "kept", "kept", 100);
                final TableReplayer keptReplayer = new TableReplayer(keptEntry.getKey());
                Assert.assertEquals(1, keptReplayer.replay(replica, keptEntry, rw, null, 100));
                Assert.assertEquals(1, replica.getRecentWriteTracker().getSequencerTxn(replica.verifyTableName("kept")));
            }
        });
    }

    @Test
    public void testReplayRefusesDeltasThatDoNotMatchTheirRecord() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("delta-a");
            final String b = newDir("delta-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                final IndexTable entry = uploadSample(primary, pw);
                final String key = entry.getKey();
                final String part0 = StoreKeys.seqPart(key, 0, EPOCH);
                final byte[] goodPart = store.getBytes(part0);
                final TxnPart decoded = decodePart(goodPart);
                final TxnRecord first = decoded.getRecord(1);
                Assert.assertTrue(first.isData());
                final long epoch = first.getDeltaEpoch();
                final int lo = first.getDeltaFirstSegmentTxn();
                final int hi = first.getDeltaLastSegmentTxn();
                final int segmentTxn = first.getSegmentTxn();
                final String unnamed = "data transaction does not name its WAL delta [table=t, txn=1]";

                // no epoch, then an epoch without a first segment transaction
                store.putRaw(part0, envelope(withDelta(entry, decoded, -1, lo, hi)::encode));
                assertStale(replica, entry, rw, unnamed);
                store.putRaw(part0, envelope(withDelta(entry, decoded, epoch, -1, hi)::encode));
                assertStale(replica, entry, rw, unnamed);
                // a delta that starts after the transaction's segment transaction
                store.putRaw(part0, envelope(withDelta(entry, decoded, epoch, segmentTxn + 1, segmentTxn + 1)::encode));
                assertStale(replica, entry, rw, unnamed);
                // a delta that ends before it
                store.putRaw(part0, envelope(withDelta(entry, decoded, epoch, segmentTxn, segmentTxn - 1)::encode));
                assertStale(replica, entry, rw, unnamed);
                store.putRaw(part0, goodPart);

                // a delta object of another WAL, another segment of the same WAL, or other segment transactions
                final String deltaKey = StoreKeys.segmentDelta(key, first.getWalId(), first.getSegmentId(), lo, hi, epoch);
                final byte[] goodDelta = store.getBytes(deltaKey);
                final String mismatch = "WAL delta does not match its key [key=" + deltaKey + ']';
                store.putRaw(deltaKey, deltaHeader(first.getWalId() + 1, first.getSegmentId(), lo, hi));
                assertStale(replica, entry, rw, mismatch);
                store.putRaw(deltaKey, deltaHeader(first.getWalId(), first.getSegmentId() + 1, lo, hi));
                assertStale(replica, entry, rw, mismatch);
                store.putRaw(deltaKey, deltaHeader(first.getWalId(), first.getSegmentId(), lo + 1, hi + 1));
                assertStale(replica, entry, rw, mismatch);
                store.putRaw(deltaKey, deltaHeader(first.getWalId(), first.getSegmentId(), lo, hi + 1));
                assertStale(replica, entry, rw, mismatch);
                store.putRaw(deltaKey, goodDelta);

                // none of them left anything behind: a replay of the intact store brings the whole table
                final TableReplayer ok = new TableReplayer(key);
                Assert.assertEquals(3, ok.replay(replica, entry, rw, null, 100));
                Assert.assertNull(ok.getStaleReason());
                TestUtils.drainWalQueue(replica);
                Assert.assertEquals(SAMPLE_ROWS, rows(replica, "t", "ts"));
            }
        });
    }

    @Test
    public void testReplayRefusesManifestsAndPartsOfAnotherTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("foreign-a");
            final String b = newDir("foreign-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                final IndexTable entry = uploadSample(primary, pw);
                final String key = entry.getKey();

                // a create manifest of another table id, of a table created at another moment, or in another
                // directory
                final String createKey = StoreKeys.create(key);
                final byte[] goodManifest = store.getBytes(createKey);
                final TableCreateManifest manifest = decodeManifest(goodManifest);
                final String foreignManifest = "table create manifest does not match its table [key=" + createKey + ']';
                store.putRaw(createKey, envelope(new TableCreateManifest().of(entry.getTableId() + 1, entry.getCreateTimestamp(),
                        IndexTable.KIND_TABLE, entry.getPartTxnCount(), entry.getDirName(), "t", manifest.getMeta0(), null)::encode));
                assertStale(replica, entry, rw, foreignManifest);
                store.putRaw(createKey, envelope(new TableCreateManifest().of(entry.getTableId(), entry.getCreateTimestamp() + 1,
                        IndexTable.KIND_TABLE, entry.getPartTxnCount(), entry.getDirName(), "t", manifest.getMeta0(), null)::encode));
                assertStale(replica, entry, rw, foreignManifest);
                store.putRaw(createKey, envelope(new TableCreateManifest().of(entry.getTableId(), entry.getCreateTimestamp(),
                        IndexTable.KIND_TABLE, entry.getPartTxnCount(), "t~other", "t", manifest.getMeta0(), null)::encode));
                assertStale(replica, entry, rw, foreignManifest);
                Assert.assertNull(replica.getTableTokenByDirName(entry.getDirName()));
                Assert.assertNull(replica.getTableTokenByDirName("t~other"));
                store.putRaw(createKey, goodManifest);

                // a transaction log part of another table, of another incarnation of the table, of another
                // part, or cut into parts of another size
                final String part0 = StoreKeys.seqPart(key, 0, EPOCH);
                final byte[] goodPart = store.getBytes(part0);
                final String foreignPart = "transaction log part does not match its table [key=" + part0 + ']';
                store.putRaw(part0, envelope(new TxnPart().of(entry.getTableId() + 1, entry.getCreateTimestamp(), 0, entry.getPartTxnCount())::encode));
                assertStale(replica, entry, rw, foreignPart);
                store.putRaw(part0, envelope(new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp() + 1, 0, entry.getPartTxnCount())::encode));
                assertStale(replica, entry, rw, foreignPart);
                store.putRaw(part0, envelope(new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp(), 1, entry.getPartTxnCount())::encode));
                assertStale(replica, entry, rw, foreignPart);
                store.putRaw(part0, envelope(new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp(), 0, entry.getPartTxnCount() + 1)::encode));
                assertStale(replica, entry, rw, foreignPart);

                // a part the index names and the store lost: retried with a fresh index, then reported
                store.delete(part0);
                final TableReplayer lost = new TableReplayer(key);
                for (int i = 0; i < 4; i++) {
                    Assert.assertEquals(0, lost.replay(replica, entry, rw, null, 100));
                    Assert.assertFalse(lost.isStale());
                }
                Assert.assertEquals(0, lost.replay(replica, entry, rw, null, 100));
                Assert.assertTrue(lost.isStale());
                TestUtils.assertContains(lost.getStaleReason(), "transaction log part is missing from the object store [key=" + part0 + ']');

                store.putRaw(part0, goodPart);
                final TableReplayer ok = new TableReplayer(key);
                Assert.assertEquals(3, ok.replay(replica, entry, rw, null, 100));
                Assert.assertNull(ok.getStaleReason());
            }
        });
    }

    @Test
    public void testSegmentDeltaBuilderRefusesCorruptSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dbRoot = newDir("segments") + File.separator + "db";
            final java.nio.file.Path wal = Paths.get(dbRoot, "t~1", "wal1");
            final SegmentDeltaBuilder builder = new SegmentDeltaBuilder(FilesFacadeImpl.INSTANCE);
            try (NativeBuffer out = new NativeBuffer()) {
                // a range that starts below zero, or ends before it starts, is refused before any file is read
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 0, -1, 0, out), "invalid segment txn range [lo=-1, hi=0]");
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 0, 2, 1, out), "invalid segment txn range [lo=2, hi=1]");

                // event records that disagree with their index: a length the index does not give, a record
                // shorter than its header, and the record of another segment transaction
                writeEvents(wal.resolve("0"), eventRecord(20, 0, WalTxnType.SQL, 13));
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 0, 0, 0, out),
                        "WAL event record does not match its index", "segmentTxn=0, length=20, indexLength=13, recordTxn=0]");
                writeEvents(wal.resolve("1"), eventRecord(12, 0, WalTxnType.SQL, 12));
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 1, 0, 0, out),
                        "WAL event record does not match its index", "segmentTxn=0, length=12, indexLength=12, recordTxn=0]");
                writeEvents(wal.resolve("2"), eventRecord(13, 5, WalTxnType.SQL, 13));
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 2, 0, 0, out),
                        "WAL event record does not match its index", "segmentTxn=0, length=13, indexLength=13, recordTxn=5]");

                // data records whose row range starts below zero, or ends before it starts
                writeEvents(wal.resolve("3"), dataRecord(-1, 1));
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 3, 0, 0, out), "invalid WAL row range", "startRowID=-1, endRowID=1]");
                writeEvents(wal.resolve("4"), dataRecord(5, 4));
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 4, 0, 0, out), "invalid WAL row range", "startRowID=5, endRowID=4]");

                // a segment without metadata ships its event records alone
                writeEvents(wal.resolve("5"), eventRecord(13, 0, WalTxnType.TRUNCATE, 13));
                builder.build(dbRoot, "t~1", 1, 5, 0, 0, out);
                final LongList stats = builder.getStats();
                Assert.assertEquals(SegmentDeltaBuilder.STATS_STRIDE, stats.size());
                Assert.assertEquals(0, stats.getQuick(0));
                Assert.assertEquals(WalTxnType.TRUNCATE, stats.getQuick(1));
                Assert.assertEquals(0, stats.getQuick(2));
                Assert.assertEquals(Long.MAX_VALUE, stats.getQuick(3));
                Assert.assertEquals(-1, stats.getQuick(4));
                final SegmentDelta delta = new SegmentDelta().of(out.ptr(), out.size());
                Assert.assertEquals(5, delta.getSegmentId());
                Assert.assertEquals(2, delta.getRangeCount());
                Assert.assertTrue(delta.nextRange());
                Assert.assertEquals(WalUtils.EVENT_INDEX_FILE_NAME, delta.getFileName());
                Assert.assertEquals(0, delta.getFileOffset());
                Assert.assertEquals(16, delta.getFileLength());
                Assert.assertTrue(delta.nextRange());
                Assert.assertEquals(WalUtils.EVENT_FILE_NAME, delta.getFileName());
                Assert.assertEquals(WalUtils.WALE_HEADER_SIZE, delta.getFileOffset());
                Assert.assertEquals(13, delta.getFileLength());
                Assert.assertFalse(delta.nextRange());

                // metadata whose column name has a negative length, or runs past the end of the metadata
                writeEvents(wal.resolve("6"), eventRecord(13, 0, WalTxnType.TRUNCATE, 13));
                writeMeta(wal.resolve("6"), ColumnType.LONG, -1, "");
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 6, 0, 0, out), "invalid WAL segment column name", "column=0]");
                writeEvents(wal.resolve("7"), eventRecord(13, 0, WalTxnType.TRUNCATE, 13));
                writeMeta(wal.resolve("7"), ColumnType.LONG, 100, "v");
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 7, 0, 0, out), "invalid WAL segment column name", "column=0]");

                // var-size columns whose data offsets go backwards, or below zero
                final java.nio.file.Path backwards = wal.resolve("8");
                writeEvents(backwards, dataRecord(1, 2));
                writeMeta(backwards, ColumnType.STRING, 1, "s");
                writeLongs(backwards.resolve("s.i"), 0, 100, 50);
                Files.write(backwards.resolve("s.d"), new byte[100]);
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 8, 0, 0, out),
                        "invalid WAL var-size column extent", "column=s, dataLo=100, dataHi=50]");
                final java.nio.file.Path negative = wal.resolve("9");
                writeEvents(negative, dataRecord(1, 2));
                writeMeta(negative, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), 1, "a");
                // array aux entries: a long data offset, an int data size and four unused bytes
                final ByteBuffer aux = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
                aux.putLong(0, 0).putInt(8, -5).putLong(16, 0).putInt(24, 8);
                Files.write(negative.resolve("a.i"), aux.array());
                Files.write(negative.resolve("a.d"), new byte[16]);
                assertCairo(() -> builder.build(dbRoot, "t~1", 1, 9, 0, 0, out),
                        "invalid WAL var-size column extent", "column=a, dataLo=-5, dataHi=8]");
            } finally {
                builder.close();
            }
            // a second close frees nothing twice: the leak check would see the native memory go negative
            builder.close();
            final SegmentDeltaWriter writer = new SegmentDeltaWriter(FilesFacadeImpl.INSTANCE, 509);
            writer.close();
            writer.close();
        });
    }

    @Test
    public void testSequencerFilesTakeTheCreateTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("sequencer-files");
            final long now = 1_700_000_000_123_456L;
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(dir + File.separator + "db", dir) {
                @Override
                public MicrosecondClock getMicrosecondClock() {
                    return () -> now;
                }
            };
            try (Path path = new Path()) {
                // a new local table: the sequencer is stamped with the clock
                Files.createDirectories(Paths.get(dir, "db", "t~1"));
                path.of(configuration.getDbRoot()).concat("t~1");
                final int tableDirLen = path.size();
                TableSequencerImpl.createSequencerFiles(configuration, DefaultWalDirectoryPolicy.INSTANCE, path,
                        new TableModel(configuration, "t", PartitionBy.DAY).col("v", ColumnType.LONG).timestamp().wal(),
                        new TableToken("t", "t~1", null, 1, true, false, false), 1);
                Assert.assertEquals(tableDirLen, path.size());
                // a replica's copy: with the primary's create timestamp
                Files.createDirectories(Paths.get(dir, "db", "u~2"));
                path.of(configuration.getDbRoot()).concat("u~2");
                TableSequencerImpl.createSequencerFiles(configuration, DefaultWalDirectoryPolicy.INSTANCE, path,
                        new TableModel(configuration, "u", PartitionBy.DAY).col("v", ColumnType.LONG).timestamp().wal(),
                        new TableToken("u", "u~2", null, 2, true, false, false), 2, 42);
            }
            Assert.assertEquals(now, readLong(Paths.get(dir, "db", "t~1", WalUtils.SEQ_DIR, WalUtils.TXNLOG_FILE_NAME),
                    TableTransactionLogFile.TABLE_CREATE_TIMESTAMP_OFFSET_64));
            Assert.assertEquals(42, readLong(Paths.get(dir, "db", "u~2", WalUtils.SEQ_DIR, WalUtils.TXNLOG_FILE_NAME),
                    TableTransactionLogFile.TABLE_CREATE_TIMESTAMP_OFFSET_64));
        });
    }

    @Test
    public void testTxnLogReaderRefusesCorruptRecords() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dbRoot = newDir("txnlog") + File.separator + "db";
            final java.nio.file.Path seq = Paths.get(dbRoot, "t~1", WalUtils.SEQ_DIR);
            Files.createDirectories(seq);
            // a V1 log: a 76-byte header, then 28-byte records of structure version, WAL id, segment id,
            // segment txn and commit timestamp
            final ByteBuffer log = ByteBuffer.allocate(76 + 4 * 28).order(ByteOrder.LITTLE_ENDIAN);
            log.putInt(0, WalUtils.WAL_SEQUENCER_FORMAT_VERSION_V1);
            log.putLong(4, 4);
            putV1Record(log, 1, 0, 1, -1, 0);
            putV1Record(log, 2, 0, 1, 0, -1);
            putV1Record(log, 3, 1, WalUtils.METADATA_WALID, -1, -1);
            putV1Record(log, 4, 2, WalUtils.METADATA_WALID, -1, -1);
            Files.write(seq.resolve(WalUtils.TXNLOG_FILE_NAME), log.array());
            // the changes of structure versions 1 and 2 claim lengths above and below what a change has
            writeLongs(seq.resolve(WalUtils.TXNLOG_FILE_NAME_META_INX), 0, 4);
            Files.write(seq.resolve(WalUtils.TXNLOG_FILE_NAME_META_VAR),
                    ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putInt(0, 16 * 1024 * 1024 + 1).putInt(4, 5).array());

            final TxnLogReader reader = new TxnLogReader(FilesFacadeImpl.INSTANCE);
            try {
                reader.of(dbRoot, "t~1");
                Assert.assertEquals(0, reader.getPartSize());
                Assert.assertEquals(4, reader.getMaxTxn());
                final TxnRecord out = new TxnRecord();
                assertCairo(() -> reader.readRecord(1, out), "invalid WAL coordinates in sequencer record", "txn=1, segmentId=-1, segmentTxn=0]");
                assertCairo(() -> reader.readRecord(2, out), "invalid WAL coordinates in sequencer record", "txn=2, segmentId=0, segmentTxn=-1]");
                assertCairo(() -> reader.readRecord(3, out), "invalid structural change length", "version=1, length=16777217]");
                assertCairo(() -> reader.readRecord(4, out), "invalid structural change length", "version=2, length=5]");
            } finally {
                reader.close();
            }
            // a second close frees nothing twice
            reader.close();
        });
    }

    @Test
    public void testUnappliedTailRepairKeepsLocalDataTheStoreNoLongerHolds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("repair-missing-a");
            final String b = newDir("repair-missing-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicationWorker pw = worker(store, primary, a)) {
                final IndexTable entry = uploadSample(primary, pw);
                // the replica replays all three transactions and stops before the apply job ran
                try (ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                     ReplicationWorker rw = worker(store, replica, b)) {
                    Assert.assertEquals(3, new TableReplayer(entry.getKey()).replay(replica, entry, rw, null, 100));
                }
                // meanwhile the cleaner removed the WAL deltas from the store
                final List<String> deltas = store.keys(k -> k.contains("/wal/"));
                Assert.assertFalse(deltas.isEmpty());
                for (String k : deltas) {
                    store.delete(k);
                }

                try (ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                     ReplicationWorker rw = worker(store, replica, b)) {
                    // the restart repairs the unapplied tail: what the store no longer holds stays as it is
                    // locally, and the repair, done once, is not retried until the table goes stale
                    final TableReplayer replayer = new TableReplayer(entry.getKey());
                    for (int i = 0; i < 6; i++) {
                        Assert.assertEquals(0, replayer.replay(replica, entry, rw, null, 100));
                        Assert.assertNull(replayer.getStaleReason());
                    }
                    TestUtils.drainWalQueue(replica);
                    Assert.assertFalse(replica.getTableSequencerAPI().isSuspended(replica.verifyTableName("t")));
                    Assert.assertEquals(SAMPLE_ROWS, rows(replica, "t", "ts"));
                }
            }
        });
    }

    @Test
    public void testUnappliedTailRepairStopsOnCorruptDelta() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("repair-corrupt-a");
            final String b = newDir("repair-corrupt-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicationWorker pw = worker(store, primary, a)) {
                final IndexTable entry = uploadSample(primary, pw);
                try (ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                     ReplicationWorker rw = worker(store, replica, b)) {
                    Assert.assertEquals(3, new TableReplayer(entry.getKey()).replay(replica, entry, rw, null, 100));
                }
                // the delta of the first unapplied transaction now names another segment
                final TxnRecord first = decodePart(store.getBytes(StoreKeys.seqPart(entry.getKey(), 0, EPOCH))).getRecord(1);
                final String deltaKey = StoreKeys.segmentDelta(entry.getKey(), first.getWalId(), first.getSegmentId(),
                        first.getDeltaFirstSegmentTxn(), first.getDeltaLastSegmentTxn(), first.getDeltaEpoch());
                store.putRaw(deltaKey, deltaHeader(first.getWalId(), first.getSegmentId() + 1, first.getDeltaFirstSegmentTxn(),
                        first.getDeltaLastSegmentTxn()));

                try (ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                     ReplicationWorker rw = worker(store, replica, b)) {
                    // the repair stops on it: the table stops replicating rather than apply what it cannot trust
                    final TableReplayer replayer = new TableReplayer(entry.getKey());
                    Assert.assertEquals(0, replayer.replay(replica, entry, rw, null, 100));
                    Assert.assertTrue(replayer.isStale());
                    TestUtils.assertContains(replayer.getStaleReason(), "WAL delta does not match its key [key=" + deltaKey + ']');
                    // the node still serves its local copy, which the repair left as it was
                    TestUtils.drainWalQueue(replica);
                    Assert.assertEquals(SAMPLE_ROWS, rows(replica, "t", "ts"));
                }
            }
        });
    }

    @Test
    public void testUploaderHoldsMarkersAndPinsUntilTheIndexCommits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("uploader-pins");
            final MemoryObjectStore store = new MemoryObjectStore();
            final NodeConfiguration configuration = new NodeConfiguration(a);
            // sequencer parts of two transactions
            configuration.seqPartTxnCount = 2;
            try (ReplicatedCairoEngine primary = newEngine(configuration, ReplicationRole.PRIMARY);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWalDirectoryPolicy policy = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, (tableDir, partId) -> false, null);
                 Path path = new Path()) {
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                final TableToken token = primary.verifyTableName("t");
                final CharSequence dbRoot = primary.getConfiguration().getDbRoot();
                final java.nio.file.Path tableDir = Paths.get(dbRoot.toString(), token.getDirName());
                final java.nio.file.Path seqMarker = Files.createFile(tableDir.resolve(WalUtils.SEQ_DIR).resolve(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER));
                final TableUploader u = new TableUploader(token, new IndexTable().of(token.getTableId(), 7, token.getDirName(), "t",
                        IndexTable.KIND_TABLE, 100), false);

                // nothing uploaded: nothing for an index to commit, and every sequencer part still needed
                Assert.assertFalse(u.hasPending());
                Assert.assertTrue(u.isSeqPartPinned(0));
                // the create manifest alone is for an index to commit; until it does, the sequencer keeps its marker
                Assert.assertEquals(0, u.upload(primary, pw, EPOCH, 0, 100));
                Assert.assertTrue(u.isCreateUploaded());
                Assert.assertTrue(u.hasPending());
                u.releaseMarkers(policy, path, dbRoot);
                Assert.assertTrue(Files.exists(seqMarker));
                u.onCommitted();
                Assert.assertFalse(u.hasPending());
                u.releaseMarkers(policy, path, dbRoot);
                Assert.assertFalse(Files.exists(seqMarker));

                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                // a zero budget uploads nothing and leaves the store alone
                final long puts = store.getPutCount();
                Assert.assertEquals(0, u.upload(primary, pw, EPOCH, 3, 0));
                Assert.assertEquals(0, u.getPendingTxn());
                Assert.assertFalse(u.hasPending());
                Assert.assertEquals(puts, store.getPutCount());

                // a close that names a transaction below one the segment holds does not release the
                // segment before the index commits what was uploaded of it
                Assert.assertEquals(3, u.upload(primary, pw, EPOCH, 3, 100));
                Assert.assertTrue(u.hasPending());
                final java.nio.file.Path segment = onlySegment(tableDir);
                final java.nio.file.Path marker = Files.createFile(segment.resolve(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER));
                u.onSegmentClosed(walIdOf(segment), Integer.parseInt(segment.getFileName().toString()), 0);
                u.releaseMarkers(policy, path, dbRoot);
                Assert.assertTrue(Files.exists(marker));
                // the commit releases the segment, and the sequencer parts before the last
                u.onCommitted();
                u.releaseMarkers(policy, path, dbRoot);
                Assert.assertFalse(Files.exists(marker));
                Assert.assertFalse(u.isSeqPartPinned(0));
                Assert.assertTrue(u.isSeqPartPinned(1));

                // a restart: the segment found at startup holds transactions up to 5, the store up to 3
                primary.execute("INSERT INTO t VALUES ('2024-01-04', 4)");
                primary.execute("INSERT INTO t VALUES ('2024-01-05', 5)");
                primary.releaseAllWalWriters();
                final TableUploader restarted = new TableUploader(token, copy(u.getEntry()), true);
                restarted.loadPart(pw);
                restarted.setStartupLastTxn(5, path, dbRoot, FilesFacadeImpl.INSTANCE);
                final java.nio.file.Path startupMarker = Files.createFile(onlySegment(tableDir).resolve(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER));
                Assert.assertEquals(1, restarted.upload(primary, pw, EPOCH, 5, 1));
                restarted.onCommitted();
                restarted.releaseMarkers(policy, path, dbRoot);
                Assert.assertTrue("txn 5 of the startup segment is not in the store yet", Files.exists(startupMarker));
                Assert.assertEquals(1, restarted.upload(primary, pw, EPOCH, 5, 1));
                restarted.onCommitted();
                restarted.releaseMarkers(policy, path, dbRoot);
                Assert.assertFalse(Files.exists(startupMarker));
            }
        });
    }

    @Test
    public void testUploaderRefusesWhatItCannotContinue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("uploader-refusals");
            final MemoryObjectStore store = new MemoryObjectStore();
            final FaultyFilesFacade ff = new FaultyFilesFacade();
            final NodeConfiguration configuration = new NodeConfiguration(a);
            configuration.ff = ff;
            try (ReplicatedCairoEngine primary = newEngine(configuration, ReplicationRole.PRIMARY);
                 ReplicationWorker pw = worker(store, primary, a)) {
                final IndexTable entry = uploadSample(primary, pw);
                final TableToken token = primary.verifyTableName("t");

                // an index entry that names no epoch for the part the next upload continues
                final IndexTable noEpoch = copy(entry);
                noEpoch.getEpochStarts().clear();
                assertReplication(() -> new TableUploader(token, noEpoch, true).loadPart(pw), ReplicationException.Kind.GAP,
                        "transaction log part is missing from the object store [key=" + StoreKeys.seqPart(entry.getKey(), 1, -1) + ']');

                // after the drop, nothing more goes up
                primary.execute("INSERT INTO t VALUES ('2024-01-04', 4, 'd', 4)");
                final IndexTable dropped = copy(entry);
                dropped.setDropped(1);
                final TableUploader afterDrop = new TableUploader(token, dropped, true);
                final int objects = store.size();
                Assert.assertEquals(0, afterDrop.upload(primary, pw, EPOCH, 4, 100));
                Assert.assertEquals(3, afterDrop.getPendingTxn());
                Assert.assertFalse(afterDrop.hasPending());
                Assert.assertEquals(objects, store.size());

                // a sequencer _meta.0 longer than 64 MiB is not read into memory
                final IndexTable other = new IndexTable().of(token.getTableId(), 8, token.getDirName(), "t", IndexTable.KIND_TABLE, 2);
                ff.trackOpenRO = WalUtils.INITIAL_META_FILE_NAME.toString();
                ff.lengthOverride = 64L * 1024 * 1024 + 1;
                assertReplication(() -> new TableUploader(token, other, false).upload(primary, pw, EPOCH, 4, 100),
                        ReplicationException.Kind.CORRUPT, "invalid table file length", ", length=67108865]");
                ff.lengthOverride = -1;
                ff.trackOpenRO = null;
                Assert.assertNull(store.getBytes(StoreKeys.create(other.getKey())));
            }
        });
    }

    @Test
    public void testWalCleanerPlansNothingForDeltasItCannotName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("cleaner");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicationWorker pw = worker(store, primary, a)) {
                final IndexTable entry = uploadSample(primary, pw);
                final WalCleaner cleaner = new WalCleaner(new ReplicationConfigurationImpl().setCleanerBackupWindowCount(1), store);
                final String part0 = StoreKeys.seqPart(entry.getKey(), 0, EPOCH);
                final TxnPart decoded = decodePart(store.getBytes(part0));

                // a data record that names no delta adds no object to delete
                store.putRaw(part0, envelope(withDelta(entry, decoded, -1, -1, -1)::encode));
                final WalCleaner.TablePlan unnamed = cleaner.planTable(entry, 2, pw);
                Assert.assertNotNull(unnamed);
                Assert.assertEquals(3, unnamed.getNewFirstTxn());
                Assert.assertEquals(List.of(part0), unnamed.getKeys());

                // nor does a part the store lost: the first transaction still moves past it
                store.delete(part0);
                final WalCleaner.TablePlan lost = cleaner.planTable(entry, 2, pw);
                Assert.assertNotNull(lost);
                Assert.assertEquals(3, lost.getNewFirstTxn());
                Assert.assertEquals(List.of(), lost.getKeys());
            }
        });
    }

    @Test
    public void testWalListenerForwardsReplicatedTablesOnly() {
        final List<String> events = new ArrayList<>();
        final ReplicationWalListener listener = new ReplicationWalListener(new ReplicationWalListener.Sink() {
            @Override
            public void onCommit(TableToken token, long txn, boolean isNonData) {
                events.add("commit " + token.getTableName() + ' ' + txn + ' ' + isNonData);
            }

            @Override
            public void onSegmentClosed(TableToken token, int walId, int segmentId, long lastTxn) {
                events.add("segment " + token.getTableName() + ' ' + walId + ' ' + segmentId + ' ' + lastTxn);
            }

            @Override
            public void onTableCreated(TableToken token) {
                events.add("created " + token.getTableName());
            }

            @Override
            public void onWalClosed(TableToken token, int walId, long lastTxn) {
                events.add("wal " + token.getTableName() + ' ' + walId + ' ' + lastTxn);
            }
        });
        final TableToken[] tokens = {
                token("nw", "nw~1", 1, TableToken.Type.TABLE, false, false),
                token("v", "v~2", 2, TableToken.Type.VIEW, true, false),
                token("mv", "mv~3", 3, TableToken.Type.MAT_VIEW, true, false),
                token("t", "t~4", 4, TableToken.Type.TABLE, true, false)
        };
        for (TableToken token : tokens) {
            listener.tableCreated(token, 0);
            listener.dataTxnCommitted(token, 1, 0, 1, 0, 0);
            listener.segmentClosed(token, 1, 1, 0);
            listener.nonDataTxnCommitted(token, 2, 0);
            listener.tableRenamed(token, 3, 0, token);
            listener.walClosed(token, 3, 1);
            listener.tableDropped(token, 4, 0);
        }
        Assert.assertEquals(List.of(
                "created t",
                "commit t 1 false",
                "segment t 1 0 1",
                "commit t 2 true",
                "commit t 3 true",
                "wal t 1 3",
                "commit t 4 true"
        ), events);
    }

    private static void assertCairo(ThrowingRunnable r, String... fragments) {
        try {
            r.run();
            Assert.fail("expected: " + String.join(", ", fragments));
        } catch (CairoException e) {
            for (String fragment : fragments) {
                TestUtils.assertContains(e.getMessage(), fragment);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void assertReadOnly(ThrowingRunnable r) {
        try {
            r.run();
            Assert.fail("expected a read-only refusal");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void assertReplication(ThrowingRunnable r, ReplicationException.Kind kind, String... fragments) {
        try {
            r.run();
            Assert.fail("expected: " + String.join(", ", fragments));
        } catch (ReplicationException e) {
            Assert.assertEquals(kind, e.getKind());
            for (String fragment : fragments) {
                TestUtils.assertContains(e.getMessage(), fragment);
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    // a fresh replayer goes stale for the reason given, on its first round, and applies nothing
    private static void assertStale(ReplicatedCairoEngine replica, IndexTable entry, ReplicationWorker w, String reason) {
        final TableReplayer r = new TableReplayer(entry.getKey());
        Assert.assertEquals(0, r.replay(replica, entry, w, null, 100));
        Assert.assertTrue("expected a stale table: " + reason, r.isStale());
        TestUtils.assertContains(r.getStaleReason(), reason);
        final TableToken token = replica.getTableTokenByDirName(entry.getDirName());
        if (token != null) {
            Assert.assertEquals(0, replica.getTableSequencerAPI().lastTxn(token));
        }
    }

    private static String columnNames(ReplicatedCairoEngine engine, String table) {
        final StringBuilder names = new StringBuilder();
        try (io.questdb.cairo.sql.TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(table))) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                names.append(i > 0 ? "," : "").append(metadata.getColumnName(i));
            }
        }
        return names.toString();
    }

    private static IndexTable copy(IndexTable t) {
        final IndexTable c = new IndexTable();
        c.copyFrom(t);
        return c;
    }

    // the record of a data transaction in the event file: its header and the row range and timestamps
    private static byte[] dataRecord(long startRow, long endRow) {
        final ByteBuffer b = ByteBuffer.allocate(45).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(0, 45).putLong(4, 0).put(12, WalTxnType.DATA);
        b.putLong(13, startRow).putLong(21, endRow).putLong(29, 0).putLong(37, 0);
        return b.array();
    }

    private static TableCreateManifest decodeManifest(byte[] stored) {
        try (NativeBuffer raw = new NativeBuffer(); ObjectEnvelope envelope = new ObjectEnvelope()) {
            envelope.decode(stored, stored.length, raw);
            return TableCreateManifest.decode(raw.ptr(), raw.size());
        }
    }

    private static TxnPart decodePart(byte[] stored) {
        try (NativeBuffer raw = new NativeBuffer(); ObjectEnvelope envelope = new ObjectEnvelope()) {
            envelope.decode(stored, stored.length, raw);
            return TxnPart.decode(raw.ptr(), raw.size());
        }
    }

    // a stored segment delta with the given header and no file ranges
    private static byte[] deltaHeader(int walId, int segmentId, int firstSegmentTxn, int lastSegmentTxn) {
        return envelope(raw -> SegmentDelta.begin(raw, walId, segmentId, firstSegmentTxn, lastSegmentTxn, 0));
    }

    private static byte[] envelope(Consumer<NativeBuffer> body) {
        try (NativeBuffer raw = new NativeBuffer(); ObjectEnvelope envelope = new ObjectEnvelope()) {
            body.accept(raw);
            final ByteSink sink = new ByteSink();
            envelope.encode(raw.ptr(), raw.size(), 1, true, sink);
            return sink.toByteArray();
        }
    }

    // an event record of the given declared length and record length, with a 13-byte header
    private static byte[] eventRecord(int length, long segmentTxn, byte type, int recordLength) {
        final ByteBuffer b = ByteBuffer.allocate(recordLength).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(0, length).putLong(4, segmentTxn);
        if (recordLength > 12) {
            b.put(12, type);
        }
        return b.array();
    }

    private static String newDir(String name) {
        final String dir = root + File.separator + name;
        TestUtils.removeTestPath(dir);
        Assert.assertTrue(new File(dir, "db").mkdirs());
        return dir;
    }

    private static ReplicatedCairoEngine newEngine(String dir, ReplicationRole role) {
        return newEngine(new NodeConfiguration(dir), role);
    }

    private static ReplicatedCairoEngine newEngine(NodeConfiguration configuration, ReplicationRole role) {
        return start(new ReplicatedCairoEngine(configuration, new QdbrWalLocker(), failure -> {
        }, replicationConfiguration(configuration.dir, role)), role);
    }

    // the only wal<N>/<segment> directory of a table
    private static java.nio.file.Path onlySegment(java.nio.file.Path tableDir) throws Exception {
        final List<java.nio.file.Path> segments = new ArrayList<>();
        try (Stream<java.nio.file.Path> walDirs = Files.list(tableDir)) {
            for (java.nio.file.Path wal : (Iterable<java.nio.file.Path>) walDirs::iterator) {
                if (!wal.getFileName().toString().matches("wal\\d+") || !Files.isDirectory(wal)) {
                    continue;
                }
                try (Stream<java.nio.file.Path> segmentDirs = Files.list(wal)) {
                    for (java.nio.file.Path segment : (Iterable<java.nio.file.Path>) segmentDirs::iterator) {
                        if (segment.getFileName().toString().matches("\\d+") && Files.isDirectory(segment)) {
                            segments.add(segment);
                        }
                    }
                }
            }
        }
        Assert.assertEquals(String.valueOf(segments), 1, segments.size());
        return segments.get(0);
    }

    private static void putV1Record(ByteBuffer log, long txn, long structureVersion, int walId, int segmentId, int segmentTxn) {
        final int offset = 76 + (int) (txn - 1) * 28;
        log.putLong(offset, structureVersion).putInt(offset + 8, walId).putInt(offset + 12, segmentId).putInt(offset + 16, segmentTxn)
                .putLong(offset + 20, 1_000_000L * txn);
    }

    private static long readLong(java.nio.file.Path file, long offset) throws Exception {
        return ByteBuffer.wrap(Files.readAllBytes(file)).order(ByteOrder.LITTLE_ENDIAN).getLong((int) offset);
    }

    private static ReplicationConfigurationImpl replicationConfiguration(String dir, ReplicationRole role) {
        return new ReplicationConfigurationImpl().setRole(role).setObjectStore("fs::root=" + dir + "/store;");
    }

    private static String rows(ReplicatedCairoEngine engine, String table, String orderBy) throws Exception {
        final StringSink sink = new StringSink();
        try (SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine)) {
            TestUtils.printSql(engine, context, "SELECT * FROM '" + table + "' ORDER BY " + orderBy, sink);
        }
        return sink.toString();
    }

    private static <T extends ReplicatedCairoEngine> T start(T engine, ReplicationRole role) {
        try {
            engine.completeInit();
            engine.load();
            engine.buildViewGraphs();
            if (role == ReplicationRole.PRIMARY) {
                engine.setRole(EngineRole.PRIMARY, null);
                // no replication service runs here to renew the lease
                engine.renewLease(System.nanoTime() + TimeUnit.HOURS.toNanos(1));
            }
            return engine;
        } catch (Throwable th) {
            engine.close();
            throw th;
        }
    }

    private static TableToken token(String name, String dirName, int tableId, TableToken.Type type, boolean isWal, boolean isSystem) {
        return new TableToken(name, dirName, null, tableId, type, isWal, isSystem, false, false);
    }

    // uploads every transaction of a primary table in one batch and returns its committed index entry
    private static IndexTable upload(ReplicatedCairoEngine primary, ReplicationWorker w, String table, String createdAs, int partTxnCount) {
        final TableToken token = primary.verifyTableName(table);
        final TableUploader u = new TableUploader(token, new IndexTable().of(token.getTableId(), 7, token.getDirName(), createdAs,
                IndexTable.KIND_TABLE, partTxnCount), false);
        final long last = primary.getTableSequencerAPI().lastTxn(token);
        Assert.assertEquals(last, u.upload(primary, w, EPOCH, last, 1_000));
        u.onCommitted();
        return copy(u.getEntry());
    }

    // uploads the create manifest of an existing primary table and returns it
    private static TableCreateManifest uploadCreate(ReplicatedCairoEngine primary, ReplicationWorker w, MemoryObjectStore store, String table) {
        final TableToken token = primary.verifyTableName(table);
        final TableUploader u = new TableUploader(token, new IndexTable().of(token.getTableId(), 7, token.getDirName(), table,
                IndexTable.KIND_TABLE, 2), false);
        Assert.assertEquals(0, u.upload(primary, w, EPOCH, 0, 100));
        Assert.assertTrue(u.isCreateUploaded());
        return decodeManifest(store.getBytes(StoreKeys.create(u.getEntry().getKey())));
    }

    // creates and fills a table on the primary, uploads it in parts of two transactions, and returns its
    // committed index entry; the new column makes the WAL writer start a second segment
    private static IndexTable uploadSample(ReplicatedCairoEngine primary, ReplicationWorker w) throws Exception {
        primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
        primary.execute("INSERT INTO t VALUES ('2024-01-01', 1, 'a'), ('2024-01-02', 2, NULL)");
        primary.execute("ALTER TABLE t ADD COLUMN x INT");
        primary.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'c', 3)");
        final IndexTable entry = upload(primary, w, "t", "t", 2);
        Assert.assertEquals(3, entry.getLastTxn());
        return entry;
    }

    private static int walIdOf(java.nio.file.Path segment) {
        return Integer.parseInt(segment.getParent().getFileName().toString().substring(WalUtils.WAL_NAME_BASE.length()));
    }

    private static TableModel walModel(ReplicatedCairoEngine engine, String name) {
        return new TableModel(engine.getConfiguration(), name, PartitionBy.DAY).col("v", ColumnType.LONG).timestamp("ts").wal();
    }

    // a copy of a decoded part whose data transaction 1 names the given delta
    private static TxnPart withDelta(IndexTable entry, TxnPart decoded, long epoch, int firstSegmentTxn, int lastSegmentTxn) {
        final TxnPart part = new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp(), 0, entry.getPartTxnCount());
        for (int i = 0, n = decoded.getRecords().size(); i < n; i++) {
            final TxnRecord r = new TxnRecord();
            r.copyFrom(decoded.getRecords().getQuick(i));
            if (r.getTxn() == 1) {
                r.setDelta(epoch, firstSegmentTxn, lastSegmentTxn);
            }
            part.add(r);
        }
        return part;
    }

    private static ReplicationWorker worker(MemoryObjectStore store, ReplicatedCairoEngine engine, String dir) {
        return new ReplicationWorker(store, 1, true, FilesFacadeImpl.INSTANCE, engine.getConfiguration().getMkDirMode(), engine,
                dir + File.separator + ".replication");
    }

    // a WAL segment's _event and _event.i holding the given records, the event file padded past its end
    private static void writeEvents(java.nio.file.Path segment, byte[]... records) throws Exception {
        Files.createDirectories(segment);
        int size = WalUtils.WALE_HEADER_SIZE;
        for (byte[] r : records) {
            size += r.length;
        }
        final ByteBuffer event = ByteBuffer.allocate(size + 64).order(ByteOrder.LITTLE_ENDIAN);
        event.putInt(0, records.length - 1).putInt(WalUtils.WAL_FORMAT_OFFSET_32, 0);
        final long[] index = new long[records.length + 1];
        int pos = WalUtils.WALE_HEADER_SIZE;
        for (int i = 0; i < records.length; i++) {
            index[i] = pos;
            event.put(pos, records[i]);
            pos += records[i].length;
        }
        index[records.length] = pos;
        Files.write(segment.resolve(WalUtils.EVENT_FILE_NAME), event.array());
        writeLongs(segment.resolve(WalUtils.EVENT_INDEX_FILE_NAME), index);
    }

    private static void writeLongs(java.nio.file.Path file, long... values) throws Exception {
        final ByteBuffer b = ByteBuffer.allocate(8 * values.length).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < values.length; i++) {
            b.putLong(8 * i, values[i]);
        }
        Files.write(file, b.array());
    }

    // a WAL segment _meta of one column without a designated timestamp; the metadata ends after the
    // name's actual characters, whatever name length it declares
    private static void writeMeta(java.nio.file.Path segment, int columnType, int nameLength, String name) throws Exception {
        final int columns = (int) WalUtils.SEQ_META_OFFSET_COLUMNS;
        final int size = columns + 8 + 2 * name.length();
        final ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt((int) WalUtils.SEQ_META_OFFSET_WAL_LENGTH, size);
        b.putInt((int) WalUtils.SEQ_META_OFFSET_COLUMN_COUNT, 1);
        b.putInt((int) WalUtils.SEQ_META_OFFSET_TIMESTAMP_INDEX, -1);
        b.putInt(columns, columnType).putInt(columns + 4, nameLength);
        for (int i = 0; i < name.length(); i++) {
            b.putChar(columns + 8 + 2 * i, name.charAt(i));
        }
        Files.write(segment.resolve(TableUtils.META_FILE_NAME), b.array());
    }

    // counts the attempts to take the table create lock that found it held, for one table directory
    private static class CreateLockWatchingEngine extends ReplicatedCairoEngine {
        private final CountDownLatch refusals = new CountDownLatch(2);
        private volatile String watchedDir;

        CreateLockWatchingEngine(NodeConfiguration configuration, String dir) {
            super(configuration, new QdbrWalLocker(), failure -> {
            }, replicationConfiguration(dir, ReplicationRole.REPLICA));
        }

        @Override
        public boolean lockTableCreate(TableToken tableToken) {
            final boolean isLocked = super.lockTableCreate(tableToken);
            if (!isLocked && tableToken.getDirName().equals(watchedDir)) {
                refusals.countDown();
            }
            return isLocked;
        }
    }

    private static class FaultyFilesFacade extends TestFilesFacadeImpl {
        private final AtomicInteger fdatasyncFailures = new AtomicInteger();
        // fails the next fdatasync once
        private volatile boolean isFdatasyncFailing;
        private volatile long lengthOverride = -1;
        private volatile String trackOpenRO;
        private volatile long trackedFd = -1;

        @Override
        public void fdatasync(long fd) {
            if (isFdatasyncFailing) {
                isFdatasyncFailing = false;
                fdatasyncFailures.incrementAndGet();
                throw CairoException.critical(5).put("injected fdatasync failure");
            }
            super.fdatasync(fd);
        }

        @Override
        public long length(long fd) {
            if (lengthOverride > -1 && fd == trackedFd) {
                return lengthOverride;
            }
            return super.length(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            final String tracked = trackOpenRO;
            if (tracked != null && Utf8s.containsAscii(name, tracked)) {
                trackedFd = fd;
            }
            return fd;
        }
    }

    private static class NodeConfiguration extends DefaultTestCairoConfiguration {
        private final String dir;
        private int commitMode = CommitMode.NOSYNC;
        private FilesFacade ff;
        private boolean isReadOnly;
        private int seqPartTxnCount;
        private String systemTableNamePrefix = "sys.";

        NodeConfiguration(String dir) {
            super(dir + File.separator + "db", dir);
            this.dir = dir;
        }

        @Override
        public int getCommitMode() {
            return commitMode;
        }

        @Override
        public int getDefaultSeqPartTxnCount() {
            return seqPartTxnCount;
        }

        @Override
        public @NotNull FilesFacade getFilesFacade() {
            return ff != null ? ff : super.getFilesFacade();
        }

        @Override
        public @NotNull CharSequence getSystemTableNamePrefix() {
            return systemTableNamePrefix;
        }

        @Override
        public boolean isReadOnlyInstance() {
            return isReadOnly;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
