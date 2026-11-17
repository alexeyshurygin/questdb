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

/**
 * Little-endian reads and writes on heap byte arrays, for the replication object formats.
 */
public final class LittleEndian {
    private LittleEndian() {
    }

    public static int getInt(byte[] b, int offset) {
        return (b[offset] & 0xff)
                | (b[offset + 1] & 0xff) << 8
                | (b[offset + 2] & 0xff) << 16
                | (b[offset + 3] & 0xff) << 24;
    }

    public static long getLong(byte[] b, int offset) {
        return (getInt(b, offset) & 0xffffffffL) | ((long) getInt(b, offset + 4) << 32);
    }

    public static short getShort(byte[] b, int offset) {
        return (short) ((b[offset] & 0xff) | (b[offset + 1] & 0xff) << 8);
    }

    public static void putInt(byte[] b, int offset, int value) {
        b[offset] = (byte) value;
        b[offset + 1] = (byte) (value >>> 8);
        b[offset + 2] = (byte) (value >>> 16);
        b[offset + 3] = (byte) (value >>> 24);
    }

    public static void putLong(byte[] b, int offset, long value) {
        putInt(b, offset, (int) value);
        putInt(b, offset + 4, (int) (value >>> 32));
    }

    public static void putShort(byte[] b, int offset, short value) {
        b[offset] = (byte) value;
        b[offset + 1] = (byte) (value >>> 8);
    }
}
