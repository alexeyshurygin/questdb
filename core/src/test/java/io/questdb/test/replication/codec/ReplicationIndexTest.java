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

import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.ReplicationIndex;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ReplicationIndexTest {

    @Test
    public void testAuthoritativeEpochPerPart() {
        final IndexTable t = new IndexTable().of(1, 2, "t~1", "t", IndexTable.KIND_TABLE, 100);
        Assert.assertEquals(-1, t.getAuthoritativeEpoch(0));
        t.addEpochStart(1, 1);
        // epoch 2 took over at txn 150: it re-uploaded part 1 (txns 101..200) in full
        t.addEpochStart(2, 150);
        t.addEpochStart(2, 160); // same epoch again is ignored
        // epoch 3 took over exactly at a part boundary
        t.addEpochStart(3, 301);
        Assert.assertEquals(1, t.getAuthoritativeEpoch(0));   // 1..100
        Assert.assertEquals(2, t.getAuthoritativeEpoch(1));   // 101..200
        Assert.assertEquals(2, t.getAuthoritativeEpoch(2));   // 201..300
        Assert.assertEquals(3, t.getAuthoritativeEpoch(3));   // 301..400
        Assert.assertEquals(3, t.getAuthoritativeEpoch(10));
        Assert.assertEquals(0, IndexTable.partOf(1, 100));
        Assert.assertEquals(0, IndexTable.partOf(100, 100));
        Assert.assertEquals(1, IndexTable.partOf(101, 100));
        try {
            t.addEpochStart(1, 500);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("epoch starts must ascend [epoch=1, firstTxn=500, lastEpoch=3, lastFirstTxn=301]", e.getMessage());
        }
        try {
            t.addEpochStart(4, 300);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("epoch starts must ascend [epoch=4, firstTxn=300, lastEpoch=3, lastFirstTxn=301]", e.getMessage());
        }
    }

    @Test
    public void testCorruptDocuments() {
        final String good = "qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 0\nend\n";
        Assert.assertEquals(0, decode(good).getTables().size());
        assertCorrupt("", "replication index is incomplete");
        assertCorrupt("qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 0\n", "replication index is incomplete");
        assertCorrupt("qdb-replication-index 2\nend\n", "unsupported replication index version [version=2]");
        assertCorrupt("hello 1\nend\n", "corrupt replication index [line=1, error=missing header]");
        assertCorrupt("qdb-replication-index x\nend\n", "corrupt replication index [line=1, error=invalid number]");
        assertCorrupt(good + "table\n", "corrupt replication index [line=5, error=content after end]");
        assertCorrupt("qdb-replication-index 1\nowner - 0 0 0 0\nend\n", "replication index lacks data_id");
        assertCorrupt("qdb-replication-index 1\ndata_id a b\nend\n", "replication index lacks owner");
        assertCorrupt("qdb-replication-index 1\ndata_id zz b\nowner - 0 0 0 0\nend\n", "corrupt replication index [line=2, error=invalid hex number]");
        assertCorrupt("qdb-replication-index 1\ndata_id a\nowner - 0 0 0 0\nend\n", "corrupt replication index [line=2, error=expected 3 fields, found 2]");
        assertCorrupt("qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 2\nend\n", "corrupt replication index [line=3, error=invalid flag]");
        assertCorrupt("qdb-replication-index 1\ndata_id a b\nowner - x 0 0 0\nend\n", "corrupt replication index [line=3, error=invalid number]");
        assertCorrupt("qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 0\nfoo\nend\n", "corrupt replication index [line=4, error=unknown record]");
        assertCorrupt("qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 0\nend x\n", "corrupt replication index [line=4, error=expected 1 fields, found 2]");
        final String head = "qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 0\n";
        assertCorrupt(head + "table 1 2 0 7 1 0 100 0 - d n\nend\n", "corrupt replication index [line=4, error=unknown table state]");
        assertCorrupt(head + "table 1 2 0 0 1 0 0 0 - d n\nend\n", "corrupt replication index [line=4, error=invalid part txn count]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 x d n\nend\n", "corrupt replication index [line=4, error=invalid epoch start]");
        assertCorrupt(head + "table 1 2 0 0 0 0 100 0 - d n\nend\n", "corrupt replication index [line=4, error=invalid transaction range]");
        assertCorrupt(head + "table 1 2 0 0 5 3 100 0 - d n\nend\n", "corrupt replication index [line=4, error=invalid transaction range]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 2@5,1@9 d n\nend\n", "corrupt replication index [line=4, error=epoch starts must ascend [epoch=1, firstTxn=9, lastEpoch=2, lastFirstTxn=5]]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 - d%4 n\nend\n", "corrupt replication index [line=4, error=truncated escape]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 - d%zz n\nend\n", "corrupt replication index [line=4, error=invalid escape]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 - dé n\nend\n", "corrupt replication index [line=4, error=unescaped non-ASCII character]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 - d\nend\n", "corrupt replication index [line=4, error=expected 12 fields, found 11]");
    }

    @Test
    public void testCorruptHeaderAndEscapes() {
        assertCorrupt("qdb-replication-index 1 x\nend\n", "corrupt replication index [line=1, error=missing header]");
        assertCorrupt("qdb-replication-index\nend\n", "corrupt replication index [line=1, error=missing header]");
        final String head = "qdb-replication-index 1\ndata_id a b\nowner - 0 0 0 0\n";
        // a valid first hex digit followed by an invalid second one
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 - d%4z n\nend\n", "corrupt replication index [line=4, error=invalid escape]");
        assertCorrupt(head + "table 1 2 0 0 1 0 100 0 - d n%4-\nend\n", "corrupt replication index [line=4, error=invalid escape]");
        Assert.assertEquals("dJ", decode(head + "table 1 2 0 0 1 0 100 0 - d%4a n\nend\n").getTables().getQuick(0).getDirName());
    }

    @Test
    public void testEscapeKeepsOnlyUnreservedCharacters() {
        // every ASCII character, then a 2-byte and a 3-byte UTF-8 character
        final StringBuilder b = new StringBuilder();
        for (char c = 0; c < 128; c++) {
            b.append(c);
        }
        final String name = b.append("é表").toString();
        final ReplicationIndex index = new ReplicationIndex();
        index.setOwner("Node_1.eu-west~2", 1, 2, 3, false);
        index.addTable(new IndexTable().of(1, 2, name, "T_1." + name, IndexTable.KIND_TABLE, 10));
        final byte[] bytes = index.encode();
        final String[] lines = new String(bytes, StandardCharsets.UTF_8).split("\n", -1);
        Assert.assertEquals(6, lines.length);
        // letters, digits and "-._~" stay as they are
        Assert.assertEquals("owner Node_1.eu-west~2 1 2 3 0", lines[2]);
        final String[] fields = lines[3].split(" ", -1);
        Assert.assertEquals(12, fields.length);
        Assert.assertEquals(percentEncode(name), fields[10]);
        Assert.assertEquals("T_1." + percentEncode(name), fields[11]);
        Assert.assertTrue(fields[10], fields[10].startsWith("%00%01%02"));
        Assert.assertTrue(fields[10], fields[10].endsWith("~%7f%c3%a9%e8%a1%a8"));

        final ReplicationIndex decoded = ReplicationIndex.decode(bytes, 0, bytes.length);
        Assert.assertEquals("Node_1.eu-west~2", decoded.getOwnerInstanceId());
        final IndexTable table = decoded.findTable(1, 2);
        Assert.assertNotNull(table);
        Assert.assertEquals(name, table.getDirName());
        Assert.assertEquals("T_1." + name, table.getTableName());
    }

    @Test
    public void testHeaderMustBeFirstLine() {
        final String body = "data_id a b\nowner - 0 0 0 0\nend\n";
        // a document without the header is not an index, even when it starts with empty lines
        assertCorrupt("\n" + body, "corrupt replication index [line=1, error=missing header]");
        assertCorrupt("\n\n" + body, "corrupt replication index [line=1, error=missing header]");
        // the header on a later line does not count either
        assertCorrupt("\nqdb-replication-index 1\n" + body, "corrupt replication index [line=1, error=missing header]");
        assertCorrupt("\n", "corrupt replication index [line=1, error=missing header]");
        // empty lines after the header stay allowed
        Assert.assertEquals(0, decode("qdb-replication-index 1\n\n" + body + "\n").getTables().size());
    }

    @Test
    public void testRoundTrip() {
        final ReplicationIndex index = new ReplicationIndex();
        index.setDataId(0xfedcba9876543210L, 0x0123456789abcdefL);
        index.setOwner("node-1 a", 7, 1_700_000_000_000_000L, 42, false);
        final IndexTable t1 = new IndexTable().of(3, 1_700_000_000_123_456L, "trades~3", "trades", IndexTable.KIND_TABLE, 5_000);
        t1.setLastTxn(12_345);
        t1.addEpochStart(1, 1);
        t1.addEpochStart(7, 12_000);
        index.addTable(t1);
        final IndexTable t2 = new IndexTable().of(4, 5, "ünï cødé~4", "ünï cødé 表", IndexTable.KIND_MAT_VIEW, 1);
        t2.setDropped(99);
        index.addTable(t2);
        final IndexTable t3 = new IndexTable().of(5, 6, "", "", IndexTable.KIND_VIEW, 2);
        index.addTable(t3);

        final byte[] bytes = index.encode();
        final String text = new String(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(
                "qdb-replication-index 1\n" +
                        "data_id fedcba9876543210 123456789abcdef\n" +
                        "owner node-1%20a 7 1700000000000000 42 0\n" +
                        "table 3 1700000000123456 0 0 1 12345 5000 0 1@1,7@12000 trades~3 trades\n" +
                        "table 4 5 2 1 1 0 1 99 - %c3%bcn%c3%af%20c%c3%b8d%c3%a9~4 %c3%bcn%c3%af%20c%c3%b8d%c3%a9%20%e8%a1%a8\n" +
                        "table 5 6 1 0 1 0 2 0 - % %\n" +
                        "end\n",
                text
        );

        // decode from an offset inside a larger buffer
        final byte[] padded = new byte[bytes.length + 10];
        System.arraycopy(bytes, 0, padded, 5, bytes.length);
        final ReplicationIndex decoded = ReplicationIndex.decode(padded, 5, bytes.length);
        Assert.assertEquals(0xfedcba9876543210L, decoded.getDataIdHi());
        Assert.assertEquals(0x0123456789abcdefL, decoded.getDataIdLo());
        Assert.assertEquals("node-1 a", decoded.getOwnerInstanceId());
        Assert.assertEquals(7, decoded.getEpoch());
        Assert.assertEquals(1_700_000_000_000_000L, decoded.getRenewedAt());
        Assert.assertEquals(42, decoded.getSequence());
        Assert.assertFalse(decoded.isReleased());
        Assert.assertEquals(3, decoded.getTables().size());
        final IndexTable d1 = decoded.findTable(3, 1_700_000_000_123_456L);
        Assert.assertNotNull(d1);
        Assert.assertEquals("trades~3", d1.getDirName());
        Assert.assertEquals("trades", d1.getTableName());
        Assert.assertEquals("3_1700000000123456", d1.getKey());
        Assert.assertEquals(12_345, d1.getLastTxn());
        Assert.assertEquals(1, d1.getFirstTxn());
        Assert.assertEquals(5_000, d1.getPartTxnCount());
        Assert.assertEquals(IndexTable.KIND_TABLE, d1.getKind());
        Assert.assertFalse(d1.isDropped());
        Assert.assertEquals(7, d1.getAuthoritativeEpoch(2));
        Assert.assertEquals(1, d1.getAuthoritativeEpoch(1));
        final IndexTable d2 = decoded.findTable(4, 5);
        Assert.assertNotNull(d2);
        Assert.assertEquals("ünï cødé 表", d2.getTableName());
        Assert.assertEquals("ünï cødé~4", d2.getDirName());
        Assert.assertTrue(d2.isDropped());
        Assert.assertEquals(99, d2.getDroppedAt());
        Assert.assertEquals(IndexTable.STATE_DROPPED, d2.getState());
        Assert.assertEquals(IndexTable.KIND_MAT_VIEW, d2.getKind());
        final IndexTable d3 = decoded.findTable(5, 6);
        Assert.assertNotNull(d3);
        Assert.assertEquals("", d3.getTableName());
        Assert.assertEquals("", d3.getDirName());
        Assert.assertNull(decoded.findTable(5, 7));
        Assert.assertArrayEquals(bytes, decoded.encode());

        // a table that existed before replication starts later in the store
        final IndexTable late = new IndexTable().of(6, 7, "late~6", "late", IndexTable.KIND_TABLE, 10);
        late.setFirstTxn(41);
        late.setLastTxn(40);
        decoded.addTable(late);
        final byte[] withLate = decoded.encode();
        final IndexTable lateDecoded = ReplicationIndex.decode(withLate, 0, withLate.length).findTable(6, 7);
        Assert.assertNotNull(lateDecoded);
        Assert.assertEquals(41, lateDecoded.getFirstTxn());
        Assert.assertEquals(40, lateDecoded.getLastTxn());
        decoded.removeTable(decoded.getTables().size() - 1);

        // copy, rename, remove
        final IndexTable copy = new IndexTable();
        copy.copyFrom(d1);
        copy.setTableName("renamed");
        Assert.assertEquals("trades", d1.getTableName());
        Assert.assertEquals(d1.getEpochStarts(), copy.getEpochStarts());
        Assert.assertEquals(d1.getLastTxn(), copy.getLastTxn());
        Assert.assertEquals(d1.getFirstTxn(), copy.getFirstTxn());
        decoded.removeTable(0);
        Assert.assertEquals(2, decoded.getTables().size());
        Assert.assertNull(decoded.findTable(3, 1_700_000_000_123_456L));

        // released lease without an owner
        decoded.setOwner(null, 8, 1, 43, true);
        final ReplicationIndex released = ReplicationIndex.decode(decoded.encode(), 0, decoded.encode().length);
        Assert.assertNull(released.getOwnerInstanceId());
        Assert.assertTrue(released.isReleased());
        Assert.assertEquals(8, released.getEpoch());
    }

    private static void assertCorrupt(String text, String message) {
        try {
            decode(text);
            Assert.fail("expected failure: " + message);
        } catch (ReplicationIndex.IndexFormatException e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }

    private static ReplicationIndex decode(String text) {
        final byte[] b = text.getBytes(StandardCharsets.UTF_8);
        return ReplicationIndex.decode(b, 0, b.length);
    }

    // RFC 3986 percent-encoding of the UTF-8 bytes: unreserved characters stay, the rest become %hh
    private static String percentEncode(String s) {
        final String unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
        final StringBuilder b = new StringBuilder();
        for (byte value : s.getBytes(StandardCharsets.UTF_8)) {
            final int c = value & 0xff;
            if (unreserved.indexOf(c) >= 0) {
                b.append((char) c);
            } else {
                b.append(String.format("%%%02x", c));
            }
        }
        return b.toString();
    }
}
