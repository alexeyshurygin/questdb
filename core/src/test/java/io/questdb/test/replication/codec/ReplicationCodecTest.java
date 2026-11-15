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


package io.questdb.test.replication.codec;

import io.questdb.replication.codec.CheckpointHistory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.NativeReader;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.replication.codec.TableCreateManifest;
import io.questdb.replication.codec.TxnPart;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ReplicationCodecTest extends AbstractTest {

    @Test
    public void testCheckpointHistoryRoundTripAndCorruption() {
        final CheckpointHistory h = new CheckpointHistory();
        final CheckpointHistory.Checkpoint c1 = new CheckpointHistory.Checkpoint(100);
        c1.addTable("t~1", 5);
        c1.addTable("ü n~2", 7);
        h.add(c1);
        h.add(new CheckpointHistory.Checkpoint(200));
        final CheckpointHistory.Checkpoint c3 = new CheckpointHistory.Checkpoint(300);
        c3.addTable("t~1", 9);
        h.add(c3);
        final byte[] bytes = h.encode();
        Assert.assertEquals(
                "qdb-checkpoint-history 1\ncheckpoint 100 2\nt t~1 5\nt %c3%bc%20n~2 7\ncheckpoint 200 0\ncheckpoint 300 1\nt t~1 9\nend\n",
                new String(bytes, StandardCharsets.UTF_8)
        );
        final CheckpointHistory d = CheckpointHistory.decode(bytes, 0, bytes.length);
        Assert.assertEquals(3, d.getCheckpoints().size());
        Assert.assertEquals(7, d.getCheckpoints().get(0).getSeqTxn("ü n~2"));
        Assert.assertEquals(-1, d.getCheckpoints().get(1).getSeqTxn("t~1"));
        Assert.assertEquals(300, d.getCheckpoints().get(2).getTimestamp());
        d.keepLatest(2);
        Assert.assertEquals(2, d.getCheckpoints().size());
        Assert.assertEquals(200, d.getCheckpoints().get(0).getTimestamp());
        Assert.assertEquals(1, d.getCheckpoints().get(1).getTables().size());

        assertHistoryCorrupt("", "not a checkpoint history");
        assertHistoryCorrupt("qdb-checkpoint-history 2\nend\n", "not a checkpoint history");
        assertHistoryCorrupt("qdb-checkpoint-history 1\n", "checkpoint history is incomplete");
        assertHistoryCorrupt("qdb-checkpoint-history 1\nend\nx\n", "corrupt checkpoint history [line=3, error=content after end]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\ncheckpoint 1 1\nend\n", "corrupt checkpoint history [line=3, error=checkpoint lacks tables]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\ncheckpoint 1 1\ncheckpoint 2 0\nend\n", "corrupt checkpoint history [line=3, error=checkpoint lacks tables]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\ncheckpoint 1\nend\n", "corrupt checkpoint history [line=2, error=expected 3 fields]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\ncheckpoint 1 -1\nend\n", "corrupt checkpoint history [line=2, error=negative table count]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\ncheckpoint x 0\nend\n", "corrupt checkpoint history [line=2, error=invalid number]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\nt a 1\nend\n", "corrupt checkpoint history [line=2, error=unexpected table record]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\ncheckpoint 1 0\nt a 1\nend\n", "corrupt checkpoint history [line=3, error=unexpected table record]");
        assertHistoryCorrupt("qdb-checkpoint-history 1\nwhat\nend\n", "corrupt checkpoint history [line=2, error=unknown record]");
    }

    @Test
    public void testNativeReaderBounds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (NativeBuffer b = new NativeBuffer()) {
                b.putByte((byte) 1);
                b.putShort((short) 2);
                b.putInt(3);
                b.putLong(4);
                b.putStr("héllo");
                b.putStr(null);
                b.putInt(0, 0x7f); // patch the first int-sized slot: byte, short and the first byte of the int
                final NativeReader r = new NativeReader().of(b.ptr(), b.size());
                Assert.assertEquals(b.size(), r.getByteCount());
                Assert.assertEquals(0x7f, r.readByte());
                Assert.assertEquals(0, r.readShort());
                Assert.assertEquals(3 & 0xffffff00, r.readInt() & 0xffffff00);
                Assert.assertEquals(4, r.readLong());
                Assert.assertEquals("héllo", r.readStr());
                Assert.assertNull(r.readStr());
                Assert.assertFalse(r.hasMore());
                Assert.assertEquals(b.size(), r.getPosition());
                assertTruncated(r::readByte, "object truncated");
                assertTruncated(r::readShort, "object truncated");
                assertTruncated(r::readInt, "object truncated");
                assertTruncated(r::readLong, "object truncated");
                assertTruncated(() -> r.readBytes(-1), "negative length [length=-1");
                // a string whose length runs past the end
                b.clear();
                b.putInt(100);
                final NativeReader r2 = new NativeReader().of(b.ptr(), b.size());
                assertTruncated(r2::readStr, "object truncated [position=4, need=100, size=4]");
            }
        });
    }

    @Test
    public void testSegmentDeltaRoundTripAndCorruption() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (NativeBuffer b = new NativeBuffer()) {
                SegmentDelta.begin(b, 3, 4, 5, 9, 1);
                final long p1 = SegmentDelta.appendRange(b, "_event", 100, 4);
                Unsafe.getUnsafe().putInt(p1, 0x01020304);
                SegmentDelta.appendRange(b, "x.d", 0, 0);
                SegmentDelta.finish(b);
                final SegmentDelta d = new SegmentDelta().of(b.ptr(), b.size());
                Assert.assertEquals(3, d.getWalId());
                Assert.assertEquals(4, d.getSegmentId());
                Assert.assertEquals(5, d.getFirstSegmentTxn());
                Assert.assertEquals(9, d.getLastSegmentTxn());
                Assert.assertEquals(1, d.getEventFormatVersion());
                Assert.assertEquals(2, d.getRangeCount());
                Assert.assertTrue(d.nextRange());
                Assert.assertEquals("_event", d.getFileName());
                Assert.assertEquals(100, d.getFileOffset());
                Assert.assertEquals(4, d.getFileLength());
                Assert.assertEquals(0x01020304, Unsafe.getUnsafe().getInt(d.getFilePtr()));
                Assert.assertTrue(d.nextRange());
                Assert.assertEquals("x.d", d.getFileName());
                Assert.assertEquals(0, d.getFileLength());
                Assert.assertFalse(d.nextRange());

                for (String bad : new String[]{"", "a/b", "a\\b", ".", "..", "a\0b"}) {
                    SegmentDelta.begin(b, 3, 4, 5, 9, 0);
                    SegmentDelta.appendRange(b, bad, 0, 0);
                    final SegmentDelta bd = new SegmentDelta().of(b.ptr(), b.size());
                    assertCorrupt(bd::nextRange, "invalid file name in segment delta");
                }
                SegmentDelta.begin(b, 3, 4, 5, 9, 0);
                SegmentDelta.appendRange(b, "f", -1, 0);
                assertCorrupt(new SegmentDelta().of(b.ptr(), b.size())::nextRange, "negative offset in segment delta [file=f]");

                // trailing bytes after the last range
                SegmentDelta.begin(b, 3, 4, 5, 9, 0);
                b.putByte((byte) 7);
                assertCorrupt(new SegmentDelta().of(b.ptr(), b.size())::nextRange, "trailing bytes in segment delta");

                SegmentDelta.begin(b, 0, 4, 5, 9, 0);
                assertCorrupt(() -> new SegmentDelta().of(b.ptr(), b.size()), "invalid segment delta header [walId=0");
                SegmentDelta.begin(b, 1, 4, 9, 5, 0);
                assertCorrupt(() -> new SegmentDelta().of(b.ptr(), b.size()), "lastSegmentTxn=5");
                b.clear();
                b.putInt(1);
                assertCorrupt(() -> new SegmentDelta().of(b.ptr(), b.size()), "not a segment delta");
                b.clear();
                b.putInt(SegmentDelta.MAGIC);
                b.putByte((byte) 9);
                assertCorrupt(() -> new SegmentDelta().of(b.ptr(), b.size()), "unsupported segment delta version [version=9]");
            }
        });
    }

    @Test
    public void testTableCreateManifestRoundTripAndCorruption() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (NativeBuffer b = new NativeBuffer()) {
                final TableCreateManifest m = new TableCreateManifest().of(7, 123, IndexTable.KIND_TABLE, 5000, "t~7", "t", new byte[]{1, 2, 3}, null);
                m.encode(b);
                final TableCreateManifest d = TableCreateManifest.decode(b.ptr(), b.size());
                Assert.assertEquals(7, d.getTableId());
                Assert.assertEquals(123, d.getCreateTimestamp());
                Assert.assertEquals(IndexTable.KIND_TABLE, d.getKind());
                Assert.assertEquals(5000, d.getPartTxnCount());
                Assert.assertEquals("t~7", d.getDirName());
                Assert.assertEquals("t", d.getTableName());
                Assert.assertArrayEquals(new byte[]{1, 2, 3}, d.getMeta0());
                Assert.assertNull(d.getDefinition());

                new TableCreateManifest().of(7, 123, IndexTable.KIND_MAT_VIEW, 1, "v~8", "v", new byte[0], new byte[]{9}).encode(b);
                Assert.assertArrayEquals(new byte[]{9}, TableCreateManifest.decode(b.ptr(), b.size()).getDefinition());

                new TableCreateManifest().of(7, 123, 5, 1, "v~8", "v", new byte[0], null).encode(b);
                assertCorrupt(() -> TableCreateManifest.decode(b.ptr(), b.size()), "invalid table create manifest [table=v]");
                new TableCreateManifest().of(7, 123, 0, 0, "v~8", "v", new byte[0], null).encode(b);
                assertCorrupt(() -> TableCreateManifest.decode(b.ptr(), b.size()), "invalid table create manifest");
                m.encode(b);
                b.putByte((byte) 0);
                assertCorrupt(() -> TableCreateManifest.decode(b.ptr(), b.size()), "trailing bytes in table create manifest");
                b.clear();
                b.putInt(0);
                assertCorrupt(() -> TableCreateManifest.decode(b.ptr(), b.size()), "not a table create manifest");
                b.clear();
                b.putInt(TableCreateManifest.MAGIC);
                b.putByte((byte) 2);
                assertCorrupt(() -> TableCreateManifest.decode(b.ptr(), b.size()), "unsupported table create manifest version [version=2]");
            }
        });
    }

    @Test
    public void testTxnPartRoundTripAndCorruption() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (NativeBuffer b = new NativeBuffer()) {
                final TxnPart p = new TxnPart().of(3, 99, 2, 4);
                Assert.assertEquals(9, p.getFirstTxn());
                Assert.assertEquals(8, p.getLastTxn());
                p.add(new TxnRecord().ofData(9, 1, 0, 0, 1, 1000, 10, 20, 5));
                p.add(new TxnRecord().ofStructural(10, 2, 1001, new byte[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}));
                p.add(new TxnRecord().ofDrop(11, 2, 1002));
                p.getRecord(9).setDelta(1, 0, 3);
                Assert.assertFalse(p.isFull());
                try {
                    p.add(new TxnRecord().ofDrop(13, 2, 1));
                    Assert.fail();
                } catch (IllegalArgumentException e) {
                    Assert.assertEquals("record does not continue the part [expected=12, actual=13, size=3]", e.getMessage());
                }
                p.add(new TxnRecord().ofDrop(12, 2, 1));
                Assert.assertTrue(p.isFull());
                try {
                    p.add(new TxnRecord().ofDrop(13, 2, 1));
                    Assert.fail();
                } catch (IllegalArgumentException e) {
                    TestUtils.assertContains(e.getMessage(), "record does not continue the part");
                }
                p.encode(b);
                final TxnPart d = TxnPart.decode(b.ptr(), b.size());
                Assert.assertEquals(3, d.getTableId());
                Assert.assertEquals(99, d.getCreateTimestamp());
                Assert.assertEquals(2, d.getPart());
                Assert.assertEquals(4, d.getPartTxnCount());
                Assert.assertEquals(12, d.getLastTxn());
                final TxnRecord r9 = d.getRecord(9);
                Assert.assertTrue(r9.isData());
                Assert.assertEquals(1, r9.getDeltaEpoch());
                Assert.assertEquals(0, r9.getDeltaFirstSegmentTxn());
                Assert.assertEquals(3, r9.getDeltaLastSegmentTxn());
                Assert.assertEquals(10, r9.getTxnMinTimestamp());
                Assert.assertEquals(20, r9.getTxnMaxTimestamp());
                Assert.assertEquals(5, r9.getRowCount());
                Assert.assertTrue(r9.isSameTxn(p.getRecord(9)));
                Assert.assertTrue(d.getRecord(10).isStructural());
                Assert.assertArrayEquals(p.getRecord(10).getPayload(), d.getRecord(10).getPayload());
                Assert.assertTrue(d.getRecord(11).isDrop());
                final TxnRecord copy = new TxnRecord();
                copy.copyFrom(d.getRecord(10));
                Assert.assertTrue(copy.isSameTxn(d.getRecord(10)));
                Assert.assertFalse(copy.isSameTxn(d.getRecord(11)));
                Assert.assertEquals("TxnRecord{txn=10, walId=-1, segmentId=-1, segmentTxn=-1, structureVersion=2, commitTimestamp=1001}", copy.toString());
                d.truncateAfter(10);
                Assert.assertEquals(10, d.getLastTxn());
                d.truncateAfter(100);
                Assert.assertEquals(10, d.getLastTxn());
                d.truncateAfter(0);
                Assert.assertEquals(8, d.getLastTxn());

                // a part that starts mid-way: a table that existed before replication
                final TxnPart late = new TxnPart().of(3, 99, 2, 4, 11);
                late.add(new TxnRecord().ofDrop(11, 0, 1));
                late.encode(b);
                final TxnPart lateDecoded = TxnPart.decode(b.ptr(), b.size());
                Assert.assertEquals(11, lateDecoded.getFirstTxn());
                Assert.assertEquals(11, lateDecoded.getLastTxn());
                try {
                    new TxnPart().of(3, 99, 2, 4, 8);
                    Assert.fail();
                } catch (IllegalArgumentException e) {
                    Assert.assertEquals("start transaction outside the part [part=2, startTxn=8]", e.getMessage());
                }

                // corruption
                b.clear();
                b.putInt(0);
                assertCorrupt(() -> TxnPart.decode(b.ptr(), b.size()), "not a transaction log part");
                b.clear();
                b.putInt(TxnPart.MAGIC);
                b.putByte((byte) 5);
                assertCorrupt(() -> TxnPart.decode(b.ptr(), b.size()), "unsupported transaction log part version [version=5]");
                writePartHeader(b, 2, 4, 9, 5);
                assertCorrupt(() -> TxnPart.decode(b.ptr(), b.size()), "invalid transaction log part header [part=2, partTxnCount=4, startTxn=9, count=5]");
                writePartHeader(b, 2, 4, 8, 0);
                assertCorrupt(() -> TxnPart.decode(b.ptr(), b.size()), "invalid transaction log part header");
                writePartHeader(b, 2, 4, 9, 1);
                new TxnRecord().ofDrop(10, 0, 0).write(b);
                assertCorrupt(() -> TxnPart.decode(b.ptr(), b.size()), "transaction log part out of order [expected=9, actual=10]");
                writePartHeader(b, 2, 4, 9, 0);
                b.putByte((byte) 1);
                assertCorrupt(() -> TxnPart.decode(b.ptr(), b.size()), "trailing bytes in transaction log part");
            }
        });
    }

    private static void assertCorrupt(ThrowingRunnable r, String message) {
        try {
            r.run();
            Assert.fail("expected: " + message);
        } catch (NativeReader.CorruptObjectException e) {
            TestUtils.assertContains(e.getMessage(), message);
        }
    }

    private static void assertHistoryCorrupt(String text, String message) {
        final byte[] b = text.getBytes(StandardCharsets.UTF_8);
        try {
            CheckpointHistory.decode(b, 0, b.length);
            Assert.fail("expected: " + message);
        } catch (ReplicationIndex.IndexFormatException e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }

    private static void assertTruncated(ThrowingRunnable r, String message) {
        assertCorrupt(r, message);
    }

    private static void writePartHeader(NativeBuffer b, long part, int partTxnCount, long startTxn, int count) {
        b.clear();
        b.putInt(TxnPart.MAGIC);
        b.putByte(TxnPart.VERSION);
        b.putInt(3);
        b.putLong(99);
        b.putLong(part);
        b.putInt(partTxnCount);
        b.putLong(startTxn);
        b.putInt(count);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }
}
