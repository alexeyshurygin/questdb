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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.QdbrWalLocker;
import io.questdb.replication.ReplicaTableApplier;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationException;
import io.questdb.replication.ReplicationRole;
import io.questdb.replication.ReplicationWalDirectoryPolicy;
import io.questdb.replication.ReplicationWorker;
import io.questdb.replication.StoreKeys;
import io.questdb.replication.TableReplayer;
import io.questdb.replication.TableUploader;
import io.questdb.replication.WalCleaner;
import io.questdb.replication.codec.CheckpointHistory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.ObjectEnvelope;
import io.questdb.replication.codec.TableCreateManifest;
import io.questdb.replication.codec.TxnPart;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.replication.store.ByteSink;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import io.questdb.replication.SegmentDeltaWriter;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Replica-side apply and replay against a store holding real uploads, then corrupted, missing and
 * mismatched objects.
 */
public class ReplicaApplyTest extends AbstractTest {
    private static final ObjList<NativeBuffer> DELTA_BUFFERS = new ObjList<>();
    private static final long EPOCH = 1;


    @Test
    public void testApplierMisuse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("misuse-a");
            final String b = newDir("misuse-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                final IndexTable entry = uploadSample(primary, pw);
                final TableCreateManifest manifest = readManifest(store, entry, pw);
                final ReplicaTableApplier applier = rw.getTableApplier();
                final TableToken token = applier.createTable(manifest);
                // creating it again returns the registered table
                Assert.assertEquals(token, applier.createTable(manifest));

                // unsupported kinds and non-WAL tables
                assertReplication(() -> applier.createTable(new TableCreateManifest().of(50, 1, IndexTable.KIND_VIEW, 2, "v~50", "v", manifest.getMeta0(), null)),
                        ReplicationException.Kind.UNSUPPORTED, "only tables are replicated");
                primary.execute("CREATE TABLE nw (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                final byte[] nonWalMeta = Files.readAllBytes(Paths.get(primary.getConfiguration().getDbRoot(), primary.verifyTableName("nw").getDirName(), "_meta"));
                assertReplication(() -> applier.createTable(new TableCreateManifest().of(51, 1, IndexTable.KIND_TABLE, 2, "nw~51", "nw51", nonWalMeta, null)),
                        ReplicationException.Kind.UNSUPPORTED, "non-WAL tables are not replicated");
                Assert.assertNull(replica.getTableTokenIfExists("nw51"));

                // a name taken locally, and a stray directory
                assertReplication(() -> applier.createTable(new TableCreateManifest().of(52, 1, IndexTable.KIND_TABLE, 2, "t~52", token.getTableName(), manifest.getMeta0(), null)),
                        ReplicationException.Kind.DIVERGENCE, "table name is taken on the replica");
                new File(replica.getConfiguration().getDbRoot(), "stray~53").mkdirs();
                Files.write(Paths.get(replica.getConfiguration().getDbRoot(), "stray~53", "_txn"), new byte[8]);
                assertReplication(() -> applier.createTable(new TableCreateManifest().of(53, 1, IndexTable.KIND_TABLE, 2, "stray~53", "stray", manifest.getMeta0(), null)),
                        ReplicationException.Kind.DIVERGENCE, "table directory exists but the table is not registered");

                // transactions out of place
                assertReplication(() -> applier.apply(token, new TxnRecord().ofData(5, 1, 0, 0, 0, 1, Long.MAX_VALUE, -1, 0)),
                        ReplicationException.Kind.DIVERGENCE, "replica transaction log does not end before the shipped transaction");
                assertReplication(() -> applier.apply(token, new TxnRecord().ofData(1, 1, 0, 0, 7, 1, Long.MAX_VALUE, -1, 0)),
                        ReplicationException.Kind.DIVERGENCE, "structure version differs from the primary's");
                assertReplication(() -> applier.apply(token, new TxnRecord().ofStructural(1, 1, 1, null)),
                        ReplicationException.Kind.CORRUPT, "structural transaction without its change");
                assertReplication(() -> applier.apply(token, new TxnRecord().ofDrop(5, 0, 1)),
                        ReplicationException.Kind.DIVERGENCE, "replica transaction log does not end before the drop");
                final TxnRecord odd = new TxnRecord().ofDrop(1, 0, 1);
                final TxnRecord unknown = new TxnRecord();
                unknown.copyFrom(odd);
                final TxnRecord weird = new TxnRecord().ofData(1, 0, 0, 0, 0, 1, 0, 0, 0);
                assertReplication(() -> applier.apply(token, weird), ReplicationException.Kind.CORRUPT, "unknown transaction kind");

                // rename payloads
                Assert.assertEquals(14, ReplicaTableApplier.readCommand(new byte[]{14, 0}));
                assertCairo(() -> ReplicaTableApplier.readRenameTarget(renamePayload(1, 0)), "rename change carries no target name");
                assertCairo(() -> ReplicaTableApplier.readRenameTarget(renamePayload(2, -1)), "invalid rename change payload");
                assertCairo(() -> ReplicaTableApplier.readRenameTarget(renamePayload(2, 1000)), "invalid rename change payload");
            }
        });
    }

    @Test
    public void testFileErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("files-a");
            final String b = newDir("files-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            final FaultyFilesFacade ff = new FaultyFilesFacade();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA, ff);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = new ReplicationWorker(store, 1, true, ff, replica.getConfiguration().getMkDirMode(), replica,
                         b + File.separator + ".replication")) {
                final IndexTable entry = uploadSample(primary, pw);
                final TableCreateManifest manifest = readManifest(store, entry, pw);
                final ReplicaTableApplier applier = rw.getTableApplier();

                // table creation: the work directory, the temporary _meta.0 and its write
                ff.failMkdirs = ".replication";
                assertCairo(() -> applier.createTable(manifest), "could not create replication work directory");
                ff.failMkdirs = null;
                ff.failOpenClean = "_meta.0.tmp";
                assertCairo(() -> applier.createTable(manifest), "could not create file");
                ff.failOpenClean = null;
                ff.failWriteLength = manifest.getMeta0().length;
                assertCairo(() -> applier.createTable(manifest), "could not write file");
                ff.failWriteLength = -1;
                // an unreadable _meta.0 releases the metadata it began to load
                final TableCreateManifest broken = new TableCreateManifest().of(manifest.getTableId(), manifest.getCreateTimestamp(),
                        IndexTable.KIND_TABLE, manifest.getPartTxnCount(), manifest.getDirName(), manifest.getTableName(), new byte[64], null);
                try {
                    applier.createTable(broken);
                    Assert.fail("expected a metadata error");
                } catch (CairoException expected) {
                    // the message comes from the metadata reader
                }
                // none of the failures left the name or the directory behind
                Assert.assertNull(replica.getTableTokenByDirName(manifest.getDirName()));
                final TableToken token = applier.createTable(manifest);
                Assert.assertEquals(manifest.getTableName(), token.getTableName());

                // WAL segment writes
                final SegmentDeltaWriter writer = rw.getDeltaWriter();
                final CharSequence dbRoot = replica.getConfiguration().getDbRoot();
                ff.failMkdirs = "wal78";
                assertCairo(() -> writer.apply(dbRoot, token.getDirName(), delta(78, false), false), "could not create WAL segment directory");
                ff.failMkdirs = null;
                ff.failOpenRW = "wal79";
                assertCairo(() -> writer.apply(dbRoot, token.getDirName(), delta(79, false), false), "could not open WAL file for writing");
                ff.failOpenRW = null;
                ff.failWriteLength = 8;
                assertCairo(() -> writer.apply(dbRoot, token.getDirName(), delta(80, false), false), "could not write WAL file");
                ff.failWriteLength = -1;
                assertCairo(() -> writer.apply(dbRoot, token.getDirName(), delta(81, false), false), "segment delta carries no event records");
                ff.failWriteLength = 4;
                assertCairo(() -> writer.apply(dbRoot, token.getDirName(), delta(82, true), false), "could not write WAL event file");
                ff.failWriteLength = -1;

                // a healthy write ends the event file with the -1 marker and stamps the format version
                writer.apply(dbRoot, token.getDirName(), delta(83, true), true);
                final byte[] event = Files.readAllBytes(Paths.get(dbRoot.toString(), token.getDirName(), "wal83", "0", "_event"));
                Assert.assertEquals(-1, io.questdb.replication.codec.LittleEndian.getInt(event, 64));
                Assert.assertEquals(7, io.questdb.replication.codec.LittleEndian.getInt(event, 4));
                Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                        Files.readAllBytes(Paths.get(dbRoot.toString(), token.getDirName(), "wal83", "0", "x.d")));
            } finally {
                freeDeltaBuffers();
            }
        });
    }

    @Test
    public void testReplayFailures() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("replay-a");
            final String b = newDir("replay-b");
            final MemoryObjectStore store = new MemoryObjectStore();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicatedCairoEngine replica = newEngine(b, ReplicationRole.REPLICA);
                 ReplicationWorker pw = worker(store, primary, a);
                 ReplicationWorker rw = worker(store, replica, b)) {
                final IndexTable entry = uploadSample(primary, pw);
                final String key = entry.getKey();

                // a missing create manifest is retried, then reported
                final byte[] manifest = store.getBytes(StoreKeys.create(key));
                store.delete(StoreKeys.create(key));
                final TableReplayer missing = new TableReplayer(key);
                for (int i = 0; i < 4; i++) {
                    Assert.assertEquals(0, missing.replay(replica, entry, rw, null, 100));
                    Assert.assertFalse(missing.isStale());
                }
                Assert.assertEquals(0, missing.replay(replica, entry, rw, null, 100));
                Assert.assertTrue(missing.isStale());
                TestUtils.assertContains(missing.getStaleReason(), "table create manifest is missing from the object store");
                Assert.assertEquals(0, missing.replay(replica, entry, rw, null, 100));

                store.putRaw(StoreKeys.create(key), new byte[]{1, 2, 3});
                assertStale(replica, entry, rw, key, "corrupt object");
                store.putRaw(StoreKeys.create(key), envelope(rw, raw -> new TableCreateManifest().of(999, 1, IndexTable.KIND_TABLE, 2, "x~999", "x", new byte[10], null).encode(raw)));
                assertStale(replica, entry, rw, key, "table create manifest does not match its table");
                store.putRaw(StoreKeys.create(key), envelope(rw, raw -> {
                    raw.putInt(TableCreateManifest.MAGIC);
                    raw.putByte((byte) 9);
                }));
                assertStale(replica, entry, rw, key, "corrupt table create manifest");

                // a table whose history starts later cannot be created from the store
                final IndexTable late = copy(entry);
                late.setFirstTxn(3);
                assertStale(replica, late, rw, key, "holds the table from transaction 3 only");
                // a table dropped before this node saw it is simply done
                final IndexTable droppedEarly = copy(entry);
                droppedEarly.setDropped(1);
                droppedEarly.setFirstTxn(1);
                droppedEarly.setLastTxn(0);
                final TableReplayer done = new TableReplayer(key);
                Assert.assertEquals(0, done.replay(replica, droppedEarly, rw, null, 100));
                // so is a table dropped with its whole history still in the store: it is never created
                final IndexTable droppedLater = copy(entry);
                droppedLater.setDropped(1);
                final TableReplayer skipped = new TableReplayer(key);
                Assert.assertEquals(0, skipped.replay(replica, droppedLater, rw, null, 100));
                Assert.assertTrue(skipped.isDone());
                Assert.assertNull(replica.getTableTokenByDirName(entry.getDirName()));
                Assert.assertTrue(done.isDone());
                Assert.assertEquals(0, done.replay(replica, droppedEarly, rw, null, 100));

                store.putRaw(StoreKeys.create(key), manifest);
                // no epoch in the index for the part
                final IndexTable noEpoch = copy(entry);
                noEpoch.getEpochStarts().clear();
                assertStale(replica, noEpoch, rw, key, "no epoch wrote the transaction log part");

                // now the table exists locally (the failed runs created it); corrupt parts
                final String part0 = StoreKeys.seqPart(key, 0, EPOCH);
                final byte[] goodPart = store.getBytes(part0);
                store.putRaw(part0, envelope(rw, raw -> raw.putInt(0)));
                assertStale(replica, entry, rw, key, "corrupt transaction log part");
                store.putRaw(part0, envelope(rw, raw -> new TxnPart().of(123, entry.getCreateTimestamp(), 0, entry.getPartTxnCount()).encode(raw)));
                assertStale(replica, entry, rw, key, "transaction log part does not match its table");
                store.putRaw(part0, goodPart);

                // a data record that names no delta, then a delta that does not match its key
                final TxnPart decoded = decodePart(rw, goodPart);
                final TxnPart noDelta = new TxnPart().of(entry.getTableId(), entry.getCreateTimestamp(), 0, entry.getPartTxnCount());
                for (int i = 0; i < decoded.getRecords().size(); i++) {
                    final TxnRecord r = new TxnRecord();
                    r.copyFrom(decoded.getRecords().getQuick(i));
                    if (r.isData()) {
                        r.setDelta(-1, -1, -1);
                    }
                    noDelta.add(r);
                }
                store.putRaw(part0, envelope(rw, noDelta::encode));
                assertStale(replica, entry, rw, key, "does not name its WAL delta");
                store.putRaw(part0, goodPart);
                final TxnRecord first = decoded.getRecord(1);
                final String deltaKey = StoreKeys.segmentDelta(key, first.getWalId(), first.getSegmentId(),
                        first.getDeltaFirstSegmentTxn(), first.getDeltaLastSegmentTxn(), first.getDeltaEpoch());
                final byte[] goodDelta = store.getBytes(deltaKey);
                store.putRaw(deltaKey, envelope(rw, raw -> io.questdb.replication.codec.SegmentDelta.begin(raw, 9, 9, 0, 0, 0)));
                assertStale(replica, entry, rw, key, "WAL delta does not match its key");
                store.delete(deltaKey);
                final TableReplayer lostDelta = new TableReplayer(key);
                for (int i = 0; i < 5; i++) {
                    lostDelta.replay(replica, entry, rw, null, 100);
                }
                Assert.assertTrue(lostDelta.isStale());
                TestUtils.assertContains(lostDelta.getStaleReason(), "WAL delta is missing from the object store");
                store.putRaw(deltaKey, goodDelta);

                // healthy replay
                final TableReplayer ok = new TableReplayer(key);
                Assert.assertEquals(entry.getLastTxn(), ok.replay(replica, entry, rw, null, 100));
                Assert.assertNull(ok.getStaleReason());
                Assert.assertEquals(0, ok.replay(replica, entry, rw, null, 100));

                // the index names a transaction the part does not hold
                final IndexTable ahead = copy(entry);
                ahead.setLastTxn(entry.getLastTxn() + 1);
                assertStale(replica, ahead, rw, key, "part ends before the index says");

                // the store dropped history this node still needs
                final IndexTable trimmed = copy(entry);
                trimmed.setFirstTxn(entry.getLastTxn() + 5);
                trimmed.setLastTxn(entry.getLastTxn() + 6);
                assertStale(replica, trimmed, rw, key, "no longer holds the transactions");
            }
        });
    }

    @Test
    public void testUploaderFailures() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("uploader-a");
            final MemoryObjectStore store = new MemoryObjectStore();
            final FaultyFilesFacade ff = new FaultyFilesFacade();
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY, ff);
                 ReplicationWorker pw = new ReplicationWorker(store, 1, true, ff, primary.getConfiguration().getMkDirMode(), primary,
                         a + File.separator + ".replication");
                 ReplicationWalDirectoryPolicy policy = new ReplicationWalDirectoryPolicy(ff, (tableDir, partId) -> false, null);
                 Path path = new Path()) {
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 2)");
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                final TableToken token = primary.verifyTableName("t");
                final IndexTable fresh = new IndexTable().of(token.getTableId(), 7, token.getDirName(), token.getTableName(), IndexTable.KIND_TABLE, 2);
                final long last = primary.getTableSequencerAPI().lastTxn(token);
                Assert.assertEquals(3, last);

                // the create manifest needs the sequencer's _meta.0
                ff.failOpenRO = "_meta.0";
                assertReplication(() -> new TableUploader(token, copy(fresh), false).upload(primary, pw, EPOCH, last, 100),
                        ReplicationException.Kind.CORRUPT, "could not open table file");
                ff.failOpenRO = null;
                ff.trackOpenRO = "_meta.0";
                ff.isLengthFailing = true;
                assertReplication(() -> new TableUploader(token, copy(fresh), false).upload(primary, pw, EPOCH, last, 100),
                        ReplicationException.Kind.CORRUPT, "invalid table file length");
                ff.isLengthFailing = false;
                ff.isReadFailing = true;
                assertReplication(() -> new TableUploader(token, copy(fresh), false).upload(primary, pw, EPOCH, last, 100),
                        ReplicationException.Kind.CORRUPT, "could not read table file");
                ff.isReadFailing = false;
                ff.trackOpenRO = null;
                Assert.assertNull("a failed upload leaves no manifest", store.getBytes(StoreKeys.create(fresh.getKey())));

                // a healthy upload of the first transaction, committed: part 0 (txns 1..2) is half full
                final TableUploader u = new TableUploader(token, copy(fresh), false);
                Assert.assertEquals(1, u.upload(primary, pw, EPOCH, 1, 100));
                u.onCommitted();
                Assert.assertEquals(1, u.getCommittedTxn());
                final IndexTable committed = copy(u.getEntry());
                final String part0 = StoreKeys.seqPart(committed.getKey(), 0, EPOCH);
                final byte[] goodPart = store.getBytes(part0);

                // a restarted uploader continues the store's last part: it must be there and complete
                store.delete(part0);
                assertReplication(() -> new TableUploader(token, copy(committed), true).loadPart(pw),
                        ReplicationException.Kind.GAP, "transaction log part is missing from the object store");
                final TxnPart emptyPart = new TxnPart().of(committed.getTableId(), committed.getCreateTimestamp(), 0, committed.getPartTxnCount());
                store.putRaw(part0, envelope(pw, emptyPart::encode));
                assertReplication(() -> new TableUploader(token, copy(committed), true).loadPart(pw),
                        ReplicationException.Kind.CORRUPT, "transaction log part ends before the index says");
                store.putRaw(part0, goodPart);
                final TableUploader restarted = new TableUploader(token, copy(committed), true);
                restarted.loadPart(pw);
                Assert.assertEquals(2, restarted.upload(primary, pw, EPOCH, last, 100));
                restarted.onCommitted();
                // part 0 now holds both of its transactions, part 1 the third
                Assert.assertEquals(2, decodePart(pw, store.getBytes(part0)).getLastTxn());
                Assert.assertEquals(3, decodePart(pw, store.getBytes(StoreKeys.seqPart(committed.getKey(), 1, EPOCH))).getLastTxn());
                Assert.assertEquals(3, restarted.getCommittedTxn());

                // startup markers: once the store holds the startup transaction, every segment marker of
                // the table goes, whatever else lies in its directory
                final CharSequence dbRoot = primary.getConfiguration().getDbRoot();
                final java.nio.file.Path tableDir = Paths.get(dbRoot.toString(), token.getDirName());
                final java.nio.file.Path marker = marker(tableDir);
                Files.createDirectories(tableDir.resolve("walnotanumber"));
                Files.createDirectories(tableDir.resolve("wal900").resolve("notasegment"));
                Files.createFile(tableDir.resolve("wal900").resolve("5"));
                Files.createFile(tableDir.resolve("wal901"));
                Files.createDirectories(tableDir.resolve("wal902"));
                Files.createFile(marker);
                restarted.setStartupLastTxn(3);
                // a table directory that cannot be listed keeps its markers for the next round
                ff.failFindFirst = token.getDirName();
                restarted.releaseMarkers(policy, path, dbRoot, ff);
                Assert.assertTrue(Files.exists(marker));
                ff.failFindFirst = "wal902";
                restarted.setStartupLastTxn(3);
                restarted.releaseMarkers(policy, path, dbRoot, ff);
                ff.failFindFirst = null;
                Assert.assertFalse(Files.exists(marker));
            }
        });
    }

    @Test
    public void testWalCleanerPlans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String a = newDir("cleaner-a");
            // lists a history that is gone by the time the cleaner reads it
            final MemoryObjectStore store = new MemoryObjectStore() {
                @Override
                public void list(@NotNull String prefix, String startAfter, @NotNull ListConsumer consumer) {
                    if (prefix.equals(StoreKeys.CHECKPOINTS_PREFIX)) {
                        consumer.onKey(StoreKeys.checkpointHistory("deleted-meanwhile"), 10);
                    }
                    super.list(prefix, startAfter, consumer);
                }
            };
            try (ReplicatedCairoEngine primary = newEngine(a, ReplicationRole.PRIMARY);
                 ReplicationWorker pw = worker(store, primary, a)) {
                final IndexTable entry = uploadSample(primary, pw);
                final String dir = entry.getDirName();
                final WalCleaner cleaner = new WalCleaner(new ReplicationConfigurationImpl().setCleanerBackupWindowCount(2), store);

                // no instance kept two checkpoints yet: nothing may be cleaned
                Assert.assertNull(cleaner.computeCutoffs(pw));
                putHistory(store, pw, "one-checkpoint", checkpoint(dir, 3));
                store.putRaw(StoreKeys.checkpointHistory("unreadable"), envelope(pw, raw -> raw.putStr("not a checkpoint history")));
                Assert.assertNull(cleaner.computeCutoffs(pw));

                // two instances with two checkpoints each: a table is needed from the oldest kept
                // checkpoint on, and one that a kept checkpoint lacks is needed whole
                putHistory(store, pw, "a", checkpoint(dir, 1), checkpoint(dir, 2), checkpoint(dir, 3));
                final CheckpointHistory.Checkpoint withOther = checkpoint(dir, 3);
                withOther.addTable("other~9", 5);
                putHistory(store, pw, "b", withOther, checkpoint(dir, 3));
                final java.util.Map<String, Long> cutoffs = cleaner.computeCutoffs(pw);
                Assert.assertNotNull(cutoffs);
                Assert.assertEquals(2, cutoffs.size());
                Assert.assertEquals(2L, (long) cutoffs.get(dir));
                Assert.assertEquals(0L, (long) cutoffs.get("other~9"));

                // the table holds txns 1..3 in parts of two: part 0 goes once a checkpoint covers txn 2
                Assert.assertNull(cleaner.planTable(entry, 1, pw));
                final String seq = "t/" + entry.getKey() + "/seq/";
                store.putRaw(seq + "stray", new byte[]{1});
                store.putRaw(seq + "x.1", new byte[]{1});
                final WalCleaner.TablePlan plan = cleaner.planTable(entry, 2, pw);
                Assert.assertNotNull(plan);
                Assert.assertEquals(3, plan.getNewFirstTxn());
                Assert.assertTrue(plan.getKeys().contains(StoreKeys.seqPart(entry.getKey(), 0, EPOCH)));
                Assert.assertFalse(plan.getKeys().contains(StoreKeys.seqPart(entry.getKey(), 1, EPOCH)));
                Assert.assertFalse(plan.getKeys().contains(seq + "stray"));
                Assert.assertFalse(plan.getKeys().contains(seq + "x.1"));
                // the delta of txns 1..2 goes with part 0; what txn 3 needs stays
                final TxnPart part1 = decodePart(pw, store.getBytes(StoreKeys.seqPart(entry.getKey(), 1, EPOCH)));
                final TxnRecord third = part1.getRecord(3);
                final String keptDelta = StoreKeys.segmentDelta(entry.getKey(), third.getWalId(), third.getSegmentId(),
                        third.getDeltaFirstSegmentTxn(), third.getDeltaLastSegmentTxn(), third.getDeltaEpoch());
                Assert.assertFalse(plan.getKeys().contains(keptDelta));
                long deltas = 0;
                for (String k : plan.getKeys()) {
                    if (k.contains("/wal/")) {
                        deltas++;
                    }
                }
                Assert.assertTrue(deltas > 0);
                // an already cleaned table, and one whose parts no epoch names, plan nothing more
                final IndexTable cleaned = copy(entry);
                cleaned.setFirstTxn(3);
                Assert.assertNull(cleaner.planTable(cleaned, 2, pw));
                final IndexTable noEpoch = copy(entry);
                noEpoch.getEpochStarts().clear();
                final WalCleaner.TablePlan blind = cleaner.planTable(noEpoch, 2, pw);
                Assert.assertNotNull(blind);
                for (String k : blind.getKeys()) {
                    Assert.assertFalse(k, k.contains("/wal/"));
                }

                // a dropped table loses everything under its prefix
                final java.util.List<String> all = cleaner.planDroppedTable(entry);
                Assert.assertTrue(all.contains(StoreKeys.create(entry.getKey())));
                Assert.assertTrue(all.contains(seq + "stray"));
                Assert.assertTrue(all.contains(keptDelta));
            }
        });
    }

    private static void assertCairo(ThrowingRunnable r, String message) {
        try {
            r.run();
            Assert.fail("expected: " + message);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), message);
        }
    }

    private static void assertReplication(ThrowingRunnable r, ReplicationException.Kind kind, String message) {
        try {
            r.run();
            Assert.fail("expected: " + message);
        } catch (ReplicationException e) {
            Assert.assertEquals(kind, e.getKind());
            TestUtils.assertContains(e.getMessage(), message);
        }
    }

    private static void assertStale(ReplicatedCairoEngine replica, IndexTable entry, ReplicationWorker w, String key, String message) {
        final TableReplayer r = new TableReplayer(key);
        Assert.assertEquals(0, r.replay(replica, entry, w, null, 100));
        Assert.assertTrue("expected a stale table: " + message, r.isStale());
        TestUtils.assertContains(r.getStaleReason(), message);
    }

    private static CheckpointHistory.Checkpoint checkpoint(String tableDir, long seqTxn) {
        final CheckpointHistory.Checkpoint c = new CheckpointHistory.Checkpoint(seqTxn * 1_000_000L);
        c.addTable(tableDir, seqTxn);
        return c;
    }

    private static IndexTable copy(IndexTable t) {
        final IndexTable c = new IndexTable();
        c.copyFrom(t);
        return c;
    }

    private static TxnPart decodePart(ReplicationWorker w, byte[] stored) {
        final NativeBuffer raw = w.getBuffer();
        raw.clear();
        try (ObjectEnvelope envelope = new ObjectEnvelope()) {
            envelope.decode(stored, stored.length, raw);
        }
        return TxnPart.decode(raw.ptr(), raw.size());
    }

    private static byte[] envelope(ReplicationWorker w, java.util.function.Consumer<NativeBuffer> body) {
        final NativeBuffer raw = new NativeBuffer();
        try (ObjectEnvelope envelope = new ObjectEnvelope()) {
            body.accept(raw);
            final ByteSink sink = new ByteSink();
            envelope.encode(raw.ptr(), raw.size(), 1, true, sink);
            return sink.toByteArray();
        } finally {
            raw.close();
        }
    }

    // a delta for segment 0 of the given WAL with an 8-byte column range and, optionally, a 64-byte event range
    private static SegmentDelta delta(int walId, boolean hasEvents) {
        final NativeBuffer raw = new NativeBuffer();
        SegmentDelta.begin(raw, walId, 0, 0, 0, 7);
        final long p = SegmentDelta.appendRange(raw, "x.d", 0, 8);
        for (int i = 0; i < 8; i++) {
            Unsafe.getUnsafe().putByte(p + i, (byte) (i + 1));
        }
        if (hasEvents) {
            final long e = SegmentDelta.appendRange(raw, "_event", 0, 64);
            Unsafe.getUnsafe().setMemory(e, 64, (byte) 0);
        }
        SegmentDelta.finish(raw);
        DELTA_BUFFERS.add(raw);
        return new SegmentDelta().of(raw.ptr(), raw.size());
    }

    // the upload.pending marker of the table's first WAL segment
    private static java.nio.file.Path marker(java.nio.file.Path tableDir) throws java.io.IOException {
        try (java.util.stream.Stream<java.nio.file.Path> walDirs = Files.list(tableDir)) {
            final java.nio.file.Path wal = walDirs
                    .filter(d -> d.getFileName().toString().matches("wal\\d+") && Files.isDirectory(d.resolve("0")))
                    .sorted().findFirst().orElseThrow();
            return wal.resolve("0").resolve(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER);
        }
    }

    private static ReplicatedCairoEngine newEngine(String dir, ReplicationRole role) {
        return newEngine(dir, role, null);
    }

    private static ReplicatedCairoEngine newEngine(String dir, ReplicationRole role, FilesFacade ff) {
        final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(dir + File.separator + "db", dir) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff != null ? ff : super.getFilesFacade();
            }
        };
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
            if (role == ReplicationRole.PRIMARY) {
                engine.setRole(ReplicatedCairoEngine.EngineRole.PRIMARY, null);
            }
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

    private static void freeDeltaBuffers() {
        Misc.freeObjList(DELTA_BUFFERS);
        DELTA_BUFFERS.clear();
    }

    private static void putHistory(MemoryObjectStore store, ReplicationWorker w, String instanceId, CheckpointHistory.Checkpoint... checkpoints) {
        final CheckpointHistory history = new CheckpointHistory();
        for (CheckpointHistory.Checkpoint c : checkpoints) {
            history.add(c);
        }
        final byte[] text = history.encode();
        store.putRaw(StoreKeys.checkpointHistory(instanceId), envelope(w, raw -> raw.putBytes(text, 0, text.length)));
    }

    private static TableCreateManifest readManifest(MemoryObjectStore store, IndexTable entry, ReplicationWorker w) {
        final byte[] stored = store.getBytes(StoreKeys.create(entry.getKey()));
        final NativeBuffer raw = w.getBuffer();
        raw.clear();
        try (ObjectEnvelope envelope = new ObjectEnvelope()) {
            envelope.decode(stored, stored.length, raw);
        }
        return TableCreateManifest.decode(raw.ptr(), raw.size());
    }

    private static byte[] renamePayload(int stringCount, int charCount) {
        final byte[] b = new byte[64];
        int pos = 2 + 4;
        io.questdb.replication.codec.LittleEndian.putInt(b, pos, 0);
        pos += 4;
        io.questdb.replication.codec.LittleEndian.putInt(b, pos, stringCount);
        pos += 4;
        io.questdb.replication.codec.LittleEndian.putInt(b, pos, charCount);
        return b;
    }

    // creates and fills a table on the primary, uploads it, and returns its committed index entry
    private static IndexTable uploadSample(ReplicatedCairoEngine primary, ReplicationWorker w) throws Exception {
        primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s STRING) TIMESTAMP(ts) PARTITION BY DAY WAL");
        primary.execute("INSERT INTO t VALUES ('2024-01-01', 1, 'a'), ('2024-01-02', 2, NULL)");
        primary.execute("ALTER TABLE t ADD COLUMN x INT");
        primary.execute("INSERT INTO t VALUES ('2024-01-03', 3, 'c', 3)");
        final TableToken token = primary.verifyTableName("t");
        final IndexTable entry = new IndexTable().of(token.getTableId(), 7, token.getDirName(), token.getTableName(), IndexTable.KIND_TABLE, 2);
        final TableUploader u = new TableUploader(token, entry, false);
        Assert.assertEquals(3, u.upload(primary, w, EPOCH, primary.getTableSequencerAPI().lastTxn(token), 100));
        Assert.assertTrue(u.hasPending());
        u.onCommitted();
        Assert.assertFalse(u.hasPending());
        Assert.assertEquals(3, u.getPendingTxn());
        Assert.assertEquals(0, u.upload(primary, w, EPOCH, 3, 100));
        return copy(u.getEntry());
    }

    private static ReplicationWorker worker(MemoryObjectStore store, ReplicatedCairoEngine engine, String dir) {
        return new ReplicationWorker(store, 1, true, FilesFacadeImpl.INSTANCE, engine.getConfiguration().getMkDirMode(), engine,
                dir + File.separator + ".replication");
    }

    private static class FaultyFilesFacade extends TestFilesFacadeImpl {
        volatile String failMkdirs;
        volatile String failOpenClean;
        volatile String failFindFirst;
        volatile String failOpenRO;
        volatile String failOpenRW;
        volatile long failWriteLength = -1;
        volatile boolean isLengthFailing;
        volatile boolean isReadFailing;
        volatile String trackOpenRO;
        private volatile long trackedFd = -1;

        @Override
        public long findFirst(LPSZ path) {
            if (failFindFirst != null && Utf8s.endsWithAscii(path, failFindFirst)) {
                return 0;
            }
            return super.findFirst(path);
        }

        @Override
        public long length(long fd) {
            if (isLengthFailing && fd == trackedFd) {
                return -1;
            }
            return super.length(fd);
        }

        @Override
        public int mkdirs(Path path, int mode) {
            if (failMkdirs != null && Utf8s.containsAscii(path, failMkdirs)) {
                return -1;
            }
            return super.mkdirs(path, mode);
        }

        @Override
        public long openCleanRW(LPSZ name, long size) {
            if (failOpenClean != null && Utf8s.containsAscii(name, failOpenClean)) {
                return -1;
            }
            return super.openCleanRW(name, size);
        }

        @Override
        public long openRO(LPSZ name) {
            if (failOpenRO != null && Utf8s.containsAscii(name, failOpenRO)) {
                return -1;
            }
            final long fd = super.openRO(name);
            if (trackOpenRO != null && Utf8s.containsAscii(name, trackOpenRO)) {
                trackedFd = fd;
            }
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            if (failOpenRW != null && Utf8s.containsAscii(name, failOpenRW)) {
                return -1;
            }
            return super.openRW(name, opts);
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (isReadFailing && fd == trackedFd) {
                return -1;
            }
            return super.read(fd, buf, len, offset);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (len == failWriteLength) {
                return -1;
            }
            return super.write(fd, address, len, offset);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

}
