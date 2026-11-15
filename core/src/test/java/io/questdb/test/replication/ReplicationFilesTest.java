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
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationRole;
import io.questdb.replication.SegmentDeltaBuilder;
import io.questdb.replication.TxnLogReader;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

/**
 * The primary's readers of sequencer logs and WAL segments, on real files and corrupted copies.
 */
public class ReplicationFilesTest extends AbstractTest {

    @Test
    public void testSegmentDeltaBuilderOnRealAndCorruptSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = newDir("seg");
            try (ReplicatedCairoEngine engine = newPrimary(dir, 0)) {
                engine.execute("CREATE TABLE t (ts TIMESTAMP, s STRING, v VARCHAR, a DOUBLE[], b BINARY, i INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
                engine.execute("INSERT INTO t VALUES ('2024-01-01', 'abc', 'a long varchar value', ARRAY[1.0, 2.0], NULL, 1), ('2024-01-02', NULL, NULL, NULL, NULL, 2)");
                engine.execute("INSERT INTO t VALUES ('2024-01-03', 'def', 'x', ARRAY[3.0], NULL, 3), ('2024-01-04', 'ghi', 'y', NULL, NULL, 4)");
                engine.execute("UPDATE t SET i = 5");
                final TableToken t = engine.verifyTableName("t");
                final String dbRoot = engine.getConfiguration().getDbRoot();
                final Path seg = Paths.get(dbRoot, t.getDirName(), "wal1", "0");
                try (SegmentDeltaBuilder builder = new SegmentDeltaBuilder(FilesFacadeImpl.INSTANCE); NativeBuffer out = new NativeBuffer()) {
                    builder.build(dbRoot, t.getDirName(), 1, 0, 0, 0, out);
                    final SegmentDelta d = new SegmentDelta().of(out.ptr(), out.size());
                    Assert.assertEquals(0, d.getFirstSegmentTxn());
                    int ranges = 0;
                    while (d.nextRange()) {
                        ranges++;
                    }
                    // _event.i, _event, _meta, ts.d, s.i, s.d, v.i, v.d, a.i, a.d, b.i, b.d, i.d
                    Assert.assertEquals(13, ranges);
                    Assert.assertEquals(2, builder.getStats().getQuick(2));

                    // the UPDATE is a SQL transaction: var-size columns ship as empty ranges
                    final int updateSegTxn = segmentTxns(seg) - 1;
                    builder.build(dbRoot, t.getDirName(), 1, 0, updateSegTxn, updateSegTxn, out);
                    final SegmentDelta u = new SegmentDelta().of(out.ptr(), out.size());
                    boolean hasEmptyAux = false;
                    while (u.nextRange()) {
                        if (u.getFileName().equals("s.i")) {
                            hasEmptyAux = u.getFileLength() == 0;
                        }
                    }
                    Assert.assertTrue(hasEmptyAux);

                    assertFails(() -> builder.build(dbRoot, t.getDirName(), 1, 0, 1, 0, out), "invalid segment txn range [lo=1, hi=0]");
                    assertFails(() -> builder.build(dbRoot, t.getDirName(), 1, 9, 0, 0, out), "could not open WAL file");
                    // the event index is pre-sized: past the end reads zeros, which validation rejects
                    assertFails(() -> builder.build(dbRoot, t.getDirName(), 1, 0, 0, 100, out), "WAL event record does not match its index");

                    // corrupted copies of the segment
                    final Path copyRoot = Paths.get(dir, "copy");
                    copySegment(seg, copyRoot.resolve("c~1").resolve("wal1").resolve("0"));
                    final String copyDb = copyRoot.toString();
                    final long rec0 = readLong(copyRoot.resolve("c~1/wal1/0/_event.i"), 0);
                    patchInt(copyRoot.resolve("c~1/wal1/0/_event"), rec0, 5);
                    assertFails(() -> builder.build(copyDb, "c~1", 1, 0, 0, 0, out), "WAL event record does not match its index");

                    copySegment(seg, copyRoot.resolve("d~1").resolve("wal1").resolve("0"));
                    patchLong(copyRoot.resolve("d~1/wal1/0/_event"), rec0 + 13, 10);
                    assertFails(() -> builder.build(copyDb, "d~1", 1, 0, 0, 0, out), "invalid WAL row range");

                    copySegment(seg, copyRoot.resolve("e~1").resolve("wal1").resolve("0"));
                    patchInt(copyRoot.resolve("e~1/wal1/0/_meta"), 0, 3);
                    assertFails(() -> builder.build(copyDb, "e~1", 1, 0, 0, 0, out), "invalid WAL segment metadata size");

                    copySegment(seg, copyRoot.resolve("f~1").resolve("wal1").resolve("0"));
                    patchInt(copyRoot.resolve("f~1/wal1/0/_meta"), 16, 1000);
                    assertFails(() -> builder.build(copyDb, "f~1", 1, 0, 0, 0, out), "truncated WAL segment metadata");

                    copySegment(seg, copyRoot.resolve("g~1").resolve("wal1").resolve("0"));
                    patchInt(copyRoot.resolve("g~1/wal1/0/_meta"), 33, -5);
                    assertFails(() -> builder.build(copyDb, "g~1", 1, 0, 0, 0, out), "invalid WAL segment column name");

                    // short files
                    copySegment(seg, copyRoot.resolve("i~1").resolve("wal1").resolve("0"));
                    truncate(copyRoot.resolve("i~1/wal1/0/_event"), 10);
                    assertFails(() -> builder.build(copyDb, "i~1", 1, 0, 0, 0, out), "could not read WAL file");
                    copySegment(seg, copyRoot.resolve("j~1").resolve("wal1").resolve("0"));
                    truncate(copyRoot.resolve("j~1/wal1/0/i.d"), 2);
                    assertFails(() -> builder.build(copyDb, "j~1", 1, 0, 0, 0, out), "could not read WAL file");

                    // the second insert's STRING data would end before it starts: its extent runs backwards
                    copySegment(seg, copyRoot.resolve("h~1").resolve("wal1").resolve("0"));
                    patchLong(copyRoot.resolve("h~1/wal1/0/s.i"), 32, 1);
                    assertFails(() -> builder.build(copyDb, "h~1", 1, 0, 1, 1, out), "invalid WAL var-size column extent");
                }
            }
        });
    }

    @Test
    public void testTxnLogReaderV1AndV2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (int partSize : new int[]{0, 2}) {
                final String dir = newDir("txnlog" + partSize);
                try (ReplicatedCairoEngine engine = newPrimary(dir, partSize); TxnLogReader reader = new TxnLogReader(FilesFacadeImpl.INSTANCE)) {
                    engine.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    engine.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                    engine.execute("ALTER TABLE t ADD COLUMN x INT");
                    engine.execute("INSERT INTO t VALUES ('2024-01-03', 3, 3)");
                    final TableToken t = engine.verifyTableName("t");
                    final String dbRoot = engine.getConfiguration().getDbRoot();
                    reader.of(dbRoot, t.getDirName());
                    Assert.assertEquals(3, reader.getMaxTxn());
                    Assert.assertEquals(partSize, reader.getPartSize());
                    final TxnRecord r = new TxnRecord();
                    reader.readRecord(1, r);
                    Assert.assertTrue(r.isData());
                    Assert.assertEquals(1, r.getWalId());
                    if (partSize > 0) {
                        Assert.assertEquals(2, r.getRowCount());
                    } else {
                        // V1 keeps no statistics
                        Assert.assertEquals(0, r.getRowCount());
                        Assert.assertEquals(Long.MAX_VALUE, r.getTxnMinTimestamp());
                    }
                    reader.readRecord(2, r);
                    Assert.assertTrue(r.isStructural());
                    Assert.assertEquals(1, r.getStructureVersion());
                    Assert.assertNotNull(r.getPayload());
                    reader.readRecord(3, r);
                    Assert.assertTrue(r.isData());
                    assertFails(() -> reader.readRecord(0, new TxnRecord()), "invalid transaction [txn=0]");
                    // V1 logs are pre-sized: past the end reads zeros, which validation rejects; V2 lacks the part
                    assertFails(() -> reader.readRecord(100, new TxnRecord()), partSize > 0 ? "could not open sequencer part" : "invalid sequencer record");
                    reader.closeFiles();

                    final Path seq = Paths.get(dbRoot, t.getDirName(), "txn_seq");
                    final Path copyRoot = Paths.get(dir, "copy");
                    copyTree(seq, copyRoot.resolve("a~1").resolve("txn_seq"));
                    patchInt(copyRoot.resolve("a~1/txn_seq/_txnlog"), 0, 7);
                    assertFails(() -> reader.of(copyRoot.toString(), "a~1"), "unsupported sequencer format");

                    // record 1 with walId 0, record 3 with a negative segment txn, record 2 at structure version 0
                    copyTree(seq, copyRoot.resolve("b~1").resolve("txn_seq"));
                    patchRecord(copyRoot.resolve("b~1/txn_seq"), partSize, 1, 8, 0);
                    patchRecord(copyRoot.resolve("b~1/txn_seq"), partSize, 3, 16, -1);
                    patchRecordLong(copyRoot.resolve("b~1/txn_seq"), partSize, 2, 0, 0);
                    reader.of(copyRoot.toString(), "b~1");
                    assertFails(() -> reader.readRecord(1, new TxnRecord()), "invalid sequencer record");
                    assertFails(() -> reader.readRecord(3, new TxnRecord()), "invalid WAL coordinates in sequencer record");
                    assertFails(() -> reader.readRecord(2, new TxnRecord()), "invalid structure version in structural record");
                    reader.closeFiles();

                    copyTree(seq, copyRoot.resolve("c~1").resolve("txn_seq"));
                    patchInt(copyRoot.resolve("c~1/txn_seq/_txnlog.meta.d"), 0, 5);
                    reader.of(copyRoot.toString(), "c~1");
                    assertFails(() -> reader.readRecord(2, new TxnRecord()), "invalid structural change length");
                    reader.closeFiles();

                    copyTree(seq, copyRoot.resolve("d~1").resolve("txn_seq"));
                    Files.write(copyRoot.resolve("d~1/txn_seq/_txnlog.meta.d"), new byte[]{100, 0, 0, 0});
                    reader.of(copyRoot.toString(), "d~1");
                    assertFails(() -> reader.readRecord(2, new TxnRecord()), "could not read structural change");
                    reader.closeFiles();

                    if (partSize > 0) {
                        copyTree(seq, copyRoot.resolve("e~1").resolve("txn_seq"));
                        patchInt(copyRoot.resolve("e~1/txn_seq/_txnlog"), 20, 0);
                        assertFails(() -> reader.of(copyRoot.toString(), "e~1"), "invalid sequencer part size");
                    }
                    assertFails(() -> reader.of(copyRoot.toString(), "missing~1"), "could not open sequencer file");
                    // a short file
                    copyTree(seq, copyRoot.resolve("f~1").resolve("txn_seq"));
                    truncate(copyRoot.resolve("f~1/txn_seq/_txnlog"), 2);
                    assertFails(() -> reader.of(copyRoot.toString(), "f~1"), "could not read sequencer file");
                }
            }
        });
    }

    private static void assertFails(ThrowingRunnable r, String message) {
        try {
            r.run();
            Assert.fail("expected: " + message);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getMessage(), message);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void copySegment(Path from, Path to) throws Exception {
        copyTree(from, to);
    }

    private static void copyTree(Path from, Path to) throws Exception {
        try (Stream<Path> paths = Files.walk(from)) {
            for (Path src : (Iterable<Path>) paths::iterator) {
                final Path dst = to.resolve(from.relativize(src).toString());
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dst);
                } else {
                    Files.copy(src, dst);
                }
            }
        }
    }

    private static ReplicatedCairoEngine newPrimary(String dir, int partSize) {
        final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(dir + File.separator + "db", dir) {
            @Override
            public int getDefaultSeqPartTxnCount() {
                return partSize;
            }
        };
        final ReplicatedCairoEngine engine = new ReplicatedCairoEngine(
                configuration,
                new QdbrWalLocker(),
                failure -> {
                },
                new ReplicationConfigurationImpl().setRole(ReplicationRole.PRIMARY).setObjectStore("fs::root=" + dir + "/store;")
        );
        try {
            engine.completeInit();
            engine.load();
            engine.buildViewGraphs();
            engine.setRole(ReplicatedCairoEngine.EngineRole.PRIMARY, null);
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

    private static void patch(Path file, long offset, ByteBuffer value) throws Exception {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.write(value, offset);
        }
    }

    private static void patchInt(Path file, long offset, int value) throws Exception {
        patch(file, offset, ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, value));
    }

    private static void patchLong(Path file, long offset, long value) throws Exception {
        patch(file, offset, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value));
    }

    // patches an int field of sequencer record txn (1-based) in a V1 _txnlog or a V2 part file
    private static void patchRecord(Path seq, int partSize, long txn, int fieldOffset, int value) throws Exception {
        if (partSize == 0) {
            patchInt(seq.resolve("_txnlog"), 76 + (txn - 1) * 28 + fieldOffset, value);
        } else {
            patchInt(seq.resolve("_txn_parts").resolve(Long.toString((txn - 1) / partSize)), ((txn - 1) % partSize) * 60 + fieldOffset, value);
        }
    }

    private static void patchRecordLong(Path seq, int partSize, long txn, int fieldOffset, long value) throws Exception {
        if (partSize == 0) {
            patchLong(seq.resolve("_txnlog"), 76 + (txn - 1) * 28 + fieldOffset, value);
        } else {
            patchLong(seq.resolve("_txn_parts").resolve(Long.toString((txn - 1) / partSize)), ((txn - 1) % partSize) * 60 + fieldOffset, value);
        }
    }

    private static void truncate(Path file, long size) throws Exception {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.truncate(size);
        }
    }

    private static long readLong(Path file, long offset) throws Exception {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            final ByteBuffer b = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(b, offset);
            return b.getLong(0);
        }
    }

    // number of event records in a segment: _event.i holds maxTxn + 2 entries once the writer closed it
    private static int segmentTxns(Path seg) throws Exception {
        try (FileChannel ch = FileChannel.open(seg.resolve("_event"), StandardOpenOption.READ)) {
            final ByteBuffer b = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            ch.read(b, 0);
            return b.getInt(0) + 1;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

}
