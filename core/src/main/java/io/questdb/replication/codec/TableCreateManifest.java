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

import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The create manifest of a replicated table: what a replica needs to create the table with the
 * primary's identity before replaying its transactions. It carries the sequencer's initial metadata
 * ({@code txn_seq/_meta.0}) and, for views and materialized views, the create-time definition file
 * of the sequencer directory.
 * <pre>
 * int magic 'QRTC', byte version, int tableId, long createTimestamp, int kind, int partTxnCount,
 * str dirName, str tableName, int meta0Length, bytes, int definitionLength (-1 none), bytes
 * </pre>
 */
public final class TableCreateManifest {
    public static final int MAGIC = 0x43545251; // "QRTC" read as a little-endian int
    public static final byte VERSION = 1;
    private long createTimestamp;
    private byte @Nullable [] definition;
    private String dirName;
    private int kind;
    private byte[] meta0;
    private int partTxnCount;
    private int tableId;
    private String tableName;

    public static @NotNull TableCreateManifest decode(long ptr, long size) {
        final NativeReader r = new NativeReader().of(ptr, size);
        if (r.readInt() != MAGIC) {
            throw new NativeReader.CorruptObjectException("not a table create manifest");
        }
        final byte version = r.readByte();
        if (version != VERSION) {
            throw new NativeReader.CorruptObjectException("unsupported table create manifest version [version=" + version + ']');
        }
        final TableCreateManifest m = new TableCreateManifest();
        m.tableId = r.readInt();
        m.createTimestamp = r.readLong();
        m.kind = r.readInt();
        m.partTxnCount = r.readInt();
        m.dirName = r.readStr();
        m.tableName = r.readStr();
        m.meta0 = readBytes(r);
        m.definition = readBytes(r);
        if (m.dirName == null || m.tableName == null || m.meta0 == null || m.partTxnCount < 1
                || m.kind < IndexTable.KIND_TABLE || m.kind > IndexTable.KIND_MAT_VIEW) {
            throw new NativeReader.CorruptObjectException("invalid table create manifest [table=" + m.tableName + ']');
        }
        if (r.hasMore()) {
            throw new NativeReader.CorruptObjectException("trailing bytes in table create manifest");
        }
        return m;
    }

    public void encode(@NotNull NativeBuffer out) {
        out.clear();
        out.putInt(MAGIC);
        out.putByte(VERSION);
        out.putInt(tableId);
        out.putLong(createTimestamp);
        out.putInt(kind);
        out.putInt(partTxnCount);
        out.putStr(dirName);
        out.putStr(tableName);
        writeBytes(out, meta0);
        writeBytes(out, definition);
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public byte @Nullable [] getDefinition() {
        return definition;
    }

    public String getDirName() {
        return dirName;
    }

    public int getKind() {
        return kind;
    }

    public byte[] getMeta0() {
        return meta0;
    }

    public int getPartTxnCount() {
        return partTxnCount;
    }

    public int getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public TableCreateManifest of(
            int tableId,
            long createTimestamp,
            int kind,
            int partTxnCount,
            @NotNull String dirName,
            @NotNull String tableName,
            byte @NotNull [] meta0,
            byte @Nullable [] definition
    ) {
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.kind = kind;
        this.partTxnCount = partTxnCount;
        this.dirName = dirName;
        this.tableName = tableName;
        this.meta0 = meta0;
        this.definition = definition;
        return this;
    }

    private static byte[] readBytes(NativeReader r) {
        final int len = r.readInt();
        if (len == -1) {
            return null;
        }
        final long p = r.readBytes(len);
        final byte[] b = new byte[len];
        Unsafe.copyMemory(null, p, b, Unsafe.BYTE_OFFSET, len);
        return b;
    }

    private static void writeBytes(NativeBuffer out, byte[] b) {
        if (b == null) {
            out.putInt(-1);
        } else {
            out.putInt(b.length);
            out.putBytes(b, 0, b.length);
        }
    }
}
