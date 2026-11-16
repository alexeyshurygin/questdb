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

import io.questdb.replication.codec.LittleEndian;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.ObjectEnvelope;
import io.questdb.replication.store.ByteSink;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.zip.CRC32C;

public class ObjectEnvelopeTest extends AbstractTest {

    @Test
    public void testChecksumDetectsCorruption() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                fillRepetitive(raw, 10_000);
                final ByteSink sink = new ByteSink();
                envelope.encode(raw.ptr(), raw.size(), 1, true, sink);
                final byte[] bytes = sink.toByteArray();
                bytes[ObjectEnvelope.HEADER_SIZE + 3] ^= 0x10;
                assertDecodeFails(envelope, bytes, bytes.length, out, "envelope checksum mismatch");
                Assert.assertEquals(0, out.size());
            }
        });
    }

    @Test
    public void testCompressedRoundTripWithChecksum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                fillRepetitive(raw, 100_000);
                final ByteSink sink = new ByteSink();
                envelope.encode(raw.ptr(), raw.size(), 3, true, sink);
                final byte[] b = sink.data();
                Assert.assertEquals(ObjectEnvelope.MAGIC, LittleEndian.getInt(b, 0));
                Assert.assertEquals(ObjectEnvelope.VERSION, b[4]);
                Assert.assertEquals(ObjectEnvelope.FLAG_ZSTD | ObjectEnvelope.FLAG_CHECKSUM, b[5]);
                Assert.assertEquals(100_000, LittleEndian.getLong(b, 8));
                final long stored = LittleEndian.getLong(b, 16);
                Assert.assertEquals(sink.size() - ObjectEnvelope.HEADER_SIZE, stored);
                Assert.assertTrue("repetitive data must compress, stored=" + stored, stored < 10_000);
                final CRC32C crc = new CRC32C();
                crc.update(b, ObjectEnvelope.HEADER_SIZE, (int) stored);
                Assert.assertEquals((int) crc.getValue(), LittleEndian.getInt(b, 24));

                // decode appends after existing content
                out.putBytes(new byte[]{9, 8, 7}, 0, 3);
                envelope.decode(sink.data(), sink.size(), out);
                Assert.assertEquals(100_003, out.size());
                Assert.assertEquals(9, Unsafe.getUnsafe().getByte(out.ptr()));
                assertSameBytes(raw.ptr(), out.ptr() + 3, raw.size());
            }
        });
    }

    @Test
    public void testDecodeRejectsMalformedHeaders() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                fillRepetitive(raw, 1_000);
                final ByteSink sink = new ByteSink();
                envelope.encode(raw.ptr(), raw.size(), 1, false, sink);
                final byte[] good = sink.toByteArray();

                assertDecodeFails(envelope, good, ObjectEnvelope.HEADER_SIZE - 1, out, "object too short for its header [length=31]");

                byte[] bad = good.clone();
                bad[0] = 'X';
                assertDecodeFails(envelope, bad, bad.length, out, "object is not a replication envelope");

                bad = good.clone();
                bad[4] = 2;
                assertDecodeFails(envelope, bad, bad.length, out, "unsupported envelope version [version=2]");

                bad = good.clone();
                bad[5] = (byte) (bad[5] | 4);
                assertDecodeFails(envelope, bad, bad.length, out, "unsupported envelope flags [flags=5]");

                assertDecodeFails(envelope, good, good.length - 1, out, "envelope length mismatch");

                bad = good.clone();
                LittleEndian.putLong(bad, 8, -1);
                assertDecodeFails(envelope, bad, bad.length, out, "invalid raw length [raw=-1");

                // a wrong raw length makes zstd produce a different size
                bad = good.clone();
                LittleEndian.putLong(bad, 8, 999);
                assertDecodeFails(envelope, bad, bad.length, out, "zstd decompression failed [expected=999");

                // garbage compressed payload
                bad = good.clone();
                for (int i = ObjectEnvelope.HEADER_SIZE; i < bad.length; i++) {
                    bad[i] = (byte) 0xAA;
                }
                assertDecodeFails(envelope, bad, bad.length, out, "zstd decompression failed");

                // stored (uncompressed) payload with a raw length that does not match
                envelope.encode(raw.ptr(), raw.size(), Integer.MIN_VALUE, false, sink);
                bad = sink.toByteArray();
                LittleEndian.putLong(bad, 8, 5);
                assertDecodeFails(envelope, bad, bad.length, out, "invalid raw length [raw=5, stored=1000]");
            }
        });
    }

    @Test
    public void testEmptyPayload() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer out = new NativeBuffer()) {
                final ByteSink sink = new ByteSink();
                envelope.encode(0, 0, 1, true, sink);
                Assert.assertEquals(ObjectEnvelope.HEADER_SIZE, sink.size());
                Assert.assertEquals(ObjectEnvelope.FLAG_CHECKSUM, sink.data()[5]);
                envelope.decode(sink.data(), sink.size(), out);
                Assert.assertEquals(0, out.size());
            }
        });
    }

    @Test
    public void testEncodeRejectsPayloadTooLarge() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope()) {
                final ByteSink sink = new ByteSink();
                sink.put(new byte[]{1, 2, 3}, 0, 3);
                // the envelope must fit a byte array with room to spare; the check comes before the
                // payload is read, so the pointer is never dereferenced
                final long limit = Integer.MAX_VALUE - ObjectEnvelope.HEADER_SIZE - 1024;
                for (long length : new long[]{limit + 1, Integer.MAX_VALUE, Long.MAX_VALUE}) {
                    try {
                        envelope.encode(0, length, 1, true, sink);
                        Assert.fail("expected a failure for length " + length);
                    } catch (ObjectEnvelope.EnvelopeException e) {
                        Assert.assertEquals("payload too large [length=" + length + ']', e.getMessage());
                    }
                    // the sink keeps what it held
                    Assert.assertEquals(3, sink.size());
                    Assert.assertEquals(2, sink.data()[1]);
                }
            }
        });
    }

    @Test
    public void testEncodeReportsCompressionFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                final ByteSink sink = new ByteSink();
                sink.put(new byte[]{1, 2, 3}, 0, 3);
                // Zstd.compress() answers a null source with a non-zero length with -1; the payload
                // is large enough to go to compression, so encode() never copies from the pointer
                try {
                    envelope.encode(0, 100, 1, true, sink);
                    Assert.fail("expected a compression failure");
                } catch (ObjectEnvelope.EnvelopeException e) {
                    Assert.assertEquals("zstd compression failed [result=-1]", e.getMessage());
                }
                // no partial envelope is left behind
                Assert.assertEquals(0, sink.size());

                // the envelope still works, with the same compression level
                fillRepetitive(raw, 10_000);
                envelope.encode(raw.ptr(), raw.size(), 1, true, sink);
                Assert.assertEquals(ObjectEnvelope.FLAG_ZSTD | ObjectEnvelope.FLAG_CHECKSUM, sink.data()[5]);
                envelope.decode(sink.data(), sink.size(), out);
                Assert.assertEquals(raw.size(), out.size());
                assertSameBytes(raw.ptr(), out.ptr(), raw.size());
            }
        });
    }

    @Test
    public void testIncompressibleAndSmallPayloadsStoredAsIs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final long p = raw.ensureCapacity(50_000);
                for (int i = 0; i < 50_000; i++) {
                    Unsafe.getUnsafe().putByte(p + i, rnd.nextByte());
                }
                raw.setSize(50_000);
                final ByteSink sink = new ByteSink();
                envelope.encode(raw.ptr(), raw.size(), 1, false, sink);
                Assert.assertEquals("random bytes do not compress", 0, sink.data()[5]);
                Assert.assertEquals(50_000 + ObjectEnvelope.HEADER_SIZE, sink.size());
                Assert.assertEquals(0, LittleEndian.getInt(sink.data(), 24));
                envelope.decode(sink.data(), sink.size(), out);
                assertSameBytes(raw.ptr(), out.ptr(), 50_000);

                // below the compression threshold
                raw.clear();
                fillRepetitive(raw, 63);
                envelope.encode(raw.ptr(), raw.size(), 1, true, sink);
                Assert.assertEquals(ObjectEnvelope.FLAG_CHECKSUM, sink.data()[5]);
                out.clear();
                envelope.decode(sink.data(), sink.size(), out);
                assertSameBytes(raw.ptr(), out.ptr(), 63);
            }
        });
    }

    @Test
    public void testLevelChangeAndNegativeLevels() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (ObjectEnvelope envelope = new ObjectEnvelope(); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                fillRepetitive(raw, 20_000);
                final ByteSink sink = new ByteSink();
                for (int level : new int[]{-7, 1, 22, 1}) {
                    envelope.encode(raw.ptr(), raw.size(), level, false, sink);
                    Assert.assertEquals(ObjectEnvelope.FLAG_ZSTD, sink.data()[5]);
                    out.clear();
                    envelope.decode(sink.data(), sink.size(), out);
                    assertSameBytes(raw.ptr(), out.ptr(), raw.size());
                }
            }
        });
    }

    @Test
    public void testLittleEndianHelpers() {
        final byte[] b = new byte[16];
        LittleEndian.putLong(b, 1, 0x0102030405060708L);
        Assert.assertEquals(0x08, b[1]);
        Assert.assertEquals(0x01, b[8]);
        Assert.assertEquals(0x0102030405060708L, LittleEndian.getLong(b, 1));
        LittleEndian.putInt(b, 9, -2);
        Assert.assertEquals(-2, LittleEndian.getInt(b, 9));
        Assert.assertEquals((byte) 0xfe, b[9]);
        LittleEndian.putShort(b, 13, (short) -3);
        Assert.assertEquals((short) -3, LittleEndian.getShort(b, 13));
        LittleEndian.putLong(b, 0, Long.MIN_VALUE);
        Assert.assertEquals(Long.MIN_VALUE, LittleEndian.getLong(b, 0));
    }

    @Test
    public void testNativeBufferGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (NativeBuffer buf = new NativeBuffer()) {
                Assert.assertEquals(0, buf.ptr());
                Assert.assertEquals(0, buf.getCapacity());
                final byte[] chunk = new byte[3_000];
                for (int i = 0; i < chunk.length; i++) {
                    chunk[i] = (byte) i;
                }
                for (int i = 0; i < 10; i++) {
                    buf.putBytes(chunk, 0, chunk.length);
                }
                Assert.assertEquals(30_000, buf.size());
                Assert.assertTrue(buf.getCapacity() >= 30_000);
                for (int i = 0; i < 30_000; i++) {
                    Assert.assertEquals((byte) (i % 3_000), Unsafe.getUnsafe().getByte(buf.ptr() + i));
                }
                buf.clear();
                Assert.assertEquals(0, buf.size());
                buf.putBytes(chunk, 10, 5);
                Assert.assertEquals(10, Unsafe.getUnsafe().getByte(buf.ptr()));
                buf.close();
                Assert.assertEquals(0, buf.ptr());
                Assert.assertEquals(0, buf.size());
                // reusable after close
                buf.putBytes(chunk, 0, 1);
                Assert.assertEquals(1, buf.size());
            }
        });
    }

    private static void assertDecodeFails(ObjectEnvelope envelope, byte[] bytes, int length, NativeBuffer out, String expected) {
        final long before = out.size();
        try {
            envelope.decode(bytes, length, out);
            Assert.fail("expected: " + expected);
        } catch (ObjectEnvelope.EnvelopeException e) {
            TestUtils.assertContains(e.getMessage(), expected);
        }
        out.setSize(before);
    }

    private static void assertSameBytes(long expected, long actual, long len) {
        for (long i = 0; i < len; i++) {
            final byte e = Unsafe.getUnsafe().getByte(expected + i);
            final byte a = Unsafe.getUnsafe().getByte(actual + i);
            if (e != a) {
                Assert.fail("mismatch at " + i + ": expected " + e + ", actual " + a);
            }
        }
    }

    private static void fillRepetitive(NativeBuffer buf, int len) {
        final long p = buf.ensureCapacity(len);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(p + i, (byte) ((i / 7) % 13));
        }
        buf.setSize(buf.size() + len);
    }
}
