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


package io.questdb.replication.codec;

import io.questdb.replication.store.ByteSink;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Zstd;
import org.jetbrains.annotations.NotNull;

import java.util.zip.CRC32C;

/**
 * Wraps every object replication uploads: a 32-byte header, then the payload, zstd-compressed
 * unless compression does not pay off, followed by nothing else. The header is little-endian:
 * <pre>
 * 0  int   magic 'QRP1'
 * 4  byte  format version (1)
 * 5  byte  flags: 1 = zstd payload, 2 = CRC32C present
 * 6  short reserved, 0
 * 8  long  raw (uncompressed) length
 * 16 long  stored payload length
 * 24 int   CRC32C of the stored payload, 0 without the flag
 * 28 int   reserved, 0
 * 32       payload
 * </pre>
 * The checksum covers the stored bytes, so a reader checks it before decompressing. Not thread-safe.
 */
public final class ObjectEnvelope implements QuietCloseable {
    public static final int FLAG_CHECKSUM = 2;
    public static final int FLAG_ZSTD = 1;
    public static final int HEADER_SIZE = 32;
    public static final int MAGIC = 0x31505251; // "QRP1" read as a little-endian int
    public static final byte VERSION = 1;
    // Payloads this small rarely compress enough to pay for the decompression call.
    private static final int MIN_COMPRESS_SIZE = 64;
    private final CRC32C crc = new CRC32C();
    private final NativeBuffer scratch = new NativeBuffer();
    private long cctx;
    private int cctxLevel = Integer.MIN_VALUE;
    private long dctx;

    @Override
    public void close() {
        if (cctx != 0) {
            Zstd.freeCCtx(cctx);
            cctx = 0;
        }
        if (dctx != 0) {
            Zstd.freeDCtx(dctx);
            dctx = 0;
        }
        scratch.close();
    }

    /**
     * Decodes an envelope held in {@code src[0..length)} and appends the raw payload to {@code out}.
     *
     * @throws EnvelopeException when the bytes are not a valid envelope or fail the checksum
     */
    public void decode(byte @NotNull [] src, int length, @NotNull NativeBuffer out) {
        if (length < HEADER_SIZE) {
            throw new EnvelopeException("object too short for its header [length=" + length + ']');
        }
        final int magic = LittleEndian.getInt(src, 0);
        if (magic != MAGIC) {
            throw new EnvelopeException("object is not a replication envelope [magic=" + Integer.toHexString(magic) + ']');
        }
        final byte version = src[4];
        if (version != VERSION) {
            throw new EnvelopeException("unsupported envelope version [version=" + version + ']');
        }
        final int flags = src[5];
        if ((flags & ~(FLAG_ZSTD | FLAG_CHECKSUM)) != 0) {
            throw new EnvelopeException("unsupported envelope flags [flags=" + flags + ']');
        }
        final long rawLength = LittleEndian.getLong(src, 8);
        final long storedLength = LittleEndian.getLong(src, 16);
        if (storedLength != length - HEADER_SIZE) {
            throw new EnvelopeException("envelope length mismatch [stored=" + storedLength + ", actual=" + (length - HEADER_SIZE) + ']');
        }
        if (rawLength < 0 || ((flags & FLAG_ZSTD) == 0 && rawLength != storedLength)) {
            throw new EnvelopeException("invalid raw length [raw=" + rawLength + ", stored=" + storedLength + ']');
        }
        if ((flags & FLAG_CHECKSUM) != 0) {
            crc.reset();
            crc.update(src, HEADER_SIZE, (int) storedLength);
            final int expected = LittleEndian.getInt(src, 24);
            final int actual = (int) crc.getValue();
            if (expected != actual) {
                throw new EnvelopeException("envelope checksum mismatch [expected=" + Integer.toHexString(expected)
                        + ", actual=" + Integer.toHexString(actual) + ']');
            }
        }
        if ((flags & FLAG_ZSTD) == 0) {
            out.putBytes(src, HEADER_SIZE, (int) storedLength);
            return;
        }
        if (dctx == 0) {
            dctx = Zstd.createDCtx();
            if (dctx == 0) {
                throw new EnvelopeException("could not create zstd decompression context");
            }
        }
        scratch.clear();
        scratch.putBytes(src, HEADER_SIZE, (int) storedLength);
        final long dst = out.ensureCapacity(rawLength);
        final long n = Zstd.decompress(dctx, scratch.ptr(), storedLength, dst, rawLength);
        if (n != rawLength) {
            throw new EnvelopeException("zstd decompression failed [expected=" + rawLength + ", result=" + n + ']');
        }
        out.setSize(out.size() + rawLength);
    }

    /**
     * Encodes {@code rawLength} bytes at {@code rawPtr} into {@code out}, replacing its content.
     *
     * @param compressionLevel zstd level, or {@link Integer#MIN_VALUE} to store the payload as is
     */
    public void encode(long rawPtr, long rawLength, int compressionLevel, boolean isChecksum, @NotNull ByteSink out) {
        if (rawLength > Integer.MAX_VALUE - HEADER_SIZE - 1024) {
            throw new EnvelopeException("payload too large [length=" + rawLength + ']');
        }
        out.clear();
        int flags = isChecksum ? FLAG_CHECKSUM : 0;
        long storedLength = rawLength;
        boolean isCompressed = false;
        if (compressionLevel != Integer.MIN_VALUE && rawLength >= MIN_COMPRESS_SIZE) {
            if (cctx == 0 || cctxLevel != compressionLevel) {
                if (cctx != 0) {
                    Zstd.freeCCtx(cctx);
                }
                cctx = Zstd.createCCtx(compressionLevel);
                if (cctx == 0) {
                    throw new EnvelopeException("could not create zstd compression context [level=" + compressionLevel + ']');
                }
                cctxLevel = compressionLevel;
            }
            // ZSTD_compressBound: input + input/256 + a small margin
            final long bound = rawLength + (rawLength >>> 8) + 128;
            scratch.clear();
            final long dst = scratch.ensureCapacity(bound);
            final long n = Zstd.compress(cctx, rawPtr, rawLength, dst, bound);
            if (n < 0) {
                throw new EnvelopeException("zstd compression failed [result=" + n + ']');
            }
            if (n < rawLength) {
                isCompressed = true;
                storedLength = n;
                flags |= FLAG_ZSTD;
            }
        }
        final byte[] buf = out.ensureCapacity((int) (HEADER_SIZE + storedLength));
        LittleEndian.putInt(buf, 0, MAGIC);
        buf[4] = VERSION;
        buf[5] = (byte) flags;
        LittleEndian.putShort(buf, 6, (short) 0);
        LittleEndian.putLong(buf, 8, rawLength);
        LittleEndian.putLong(buf, 16, storedLength);
        LittleEndian.putInt(buf, 28, 0);
        final long src = isCompressed ? scratch.ptr() : rawPtr;
        Unsafe.copyMemory(null, src, buf, Unsafe.BYTE_OFFSET + HEADER_SIZE, storedLength);
        int checksum = 0;
        if (isChecksum) {
            crc.reset();
            crc.update(buf, HEADER_SIZE, (int) storedLength);
            checksum = (int) crc.getValue();
        }
        LittleEndian.putInt(buf, 24, checksum);
        out.advance((int) (HEADER_SIZE + storedLength));
    }

    public static class EnvelopeException extends RuntimeException {
        public EnvelopeException(String message) {
            super(message, null, false, false);
        }
    }
}
