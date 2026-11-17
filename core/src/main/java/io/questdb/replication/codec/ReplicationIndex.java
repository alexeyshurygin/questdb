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

import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * The replication index, the object store's single commit point. It names the database (DataID),
 * holds the ownership lease of the primary that writes the store, and lists every replicated table
 * with the last transaction the store holds for it. The primary replaces it with a conditional
 * write ({@code If-Match}) after each upload, so two primaries can never both commit.
 * <p>
 * Serialized as UTF-8 text, one record per line, so it can be inspected with any object store
 * browser:
 * <pre>
 * qdb-replication-index 1
 * data_id &lt;hex hi&gt; &lt;hex lo&gt;
 * owner &lt;instance id or -&gt; &lt;epoch&gt; &lt;renewed at, epoch micros&gt; &lt;sequence&gt; &lt;released 0|1&gt;
 * table &lt;id&gt; &lt;create ts&gt; &lt;kind&gt; &lt;state&gt; &lt;first txn&gt; &lt;last txn&gt; &lt;part txn count&gt; &lt;dropped at&gt; &lt;epoch@first txn,...|-&gt; &lt;dir name&gt; &lt;table name&gt;
 * end
 * </pre>
 * Names are percent-encoded. The {@code end} line marks a complete document.
 */
public final class ReplicationIndex {
    public static final int FORMAT_VERSION = 1;
    private static final String HEADER = "qdb-replication-index";
    private final ObjList<IndexTable> tables = new ObjList<>();
    private long dataIdHi;
    private long dataIdLo;
    private long epoch;
    private boolean isReleased;
    private String ownerInstanceId;
    private long renewedAt;
    private long sequence;

    public static @NotNull ReplicationIndex decode(byte @NotNull [] data, int offset, int length) {
        final ReplicationIndex index = new ReplicationIndex();
        final String text = new String(data, offset, length, StandardCharsets.UTF_8);
        if (text.isEmpty()) {
            throw new IndexFormatException("replication index is incomplete");
        }
        final String[] lines = text.split("\n", -1);
        int lineNo = 0;
        boolean isEnd = false;
        boolean hasDataId = false;
        boolean hasOwner = false;
        for (String line : lines) {
            lineNo++;
            if (lineNo == 1) {
                // the header must be the first line, so an empty first line is not skipped
                final String[] f = line.split(" ", -1);
                if (f.length != 2 || !HEADER.equals(f[0])) {
                    throw corrupt(lineNo, "missing header");
                }
                final int version = parseInt(f[1], lineNo);
                if (version != FORMAT_VERSION) {
                    throw new IndexFormatException("unsupported replication index version [version=" + version + ']');
                }
                continue;
            }
            if (line.isEmpty()) {
                continue;
            }
            if (isEnd) {
                throw corrupt(lineNo, "content after end");
            }
            final String[] f = line.split(" ", -1);
            switch (f[0]) {
                case "data_id" -> {
                    expectFields(f, 3, lineNo);
                    index.dataIdHi = parseHex(f[1], lineNo);
                    index.dataIdLo = parseHex(f[2], lineNo);
                    hasDataId = true;
                }
                case "owner" -> {
                    expectFields(f, 6, lineNo);
                    index.ownerInstanceId = "-".equals(f[1]) ? null : unescape(f[1], lineNo);
                    index.epoch = parseLong(f[2], lineNo);
                    index.renewedAt = parseLong(f[3], lineNo);
                    index.sequence = parseLong(f[4], lineNo);
                    index.isReleased = parseFlag(f[5], lineNo);
                    hasOwner = true;
                }
                case "table" -> {
                    expectFields(f, 12, lineNo);
                    final IndexTable t = new IndexTable().of(
                            parseInt(f[1], lineNo),
                            parseLong(f[2], lineNo),
                            unescape(f[10], lineNo),
                            unescape(f[11], lineNo),
                            parseInt(f[3], lineNo),
                            parseInt(f[7], lineNo)
                    );
                    final int state = parseInt(f[4], lineNo);
                    if (state == IndexTable.STATE_DROPPED) {
                        t.setDropped(parseLong(f[8], lineNo));
                    } else if (state != IndexTable.STATE_ACTIVE) {
                        throw corrupt(lineNo, "unknown table state");
                    }
                    if (t.getPartTxnCount() < 1) {
                        throw corrupt(lineNo, "invalid part txn count");
                    }
                    t.setFirstTxn(parseLong(f[5], lineNo));
                    t.setLastTxn(parseLong(f[6], lineNo));
                    if (t.getFirstTxn() < 1 || t.getLastTxn() < t.getFirstTxn() - 1) {
                        throw corrupt(lineNo, "invalid transaction range");
                    }
                    if (!"-".equals(f[9])) {
                        for (String pair : f[9].split(",", -1)) {
                            final int at = pair.indexOf('@');
                            if (at < 1) {
                                throw corrupt(lineNo, "invalid epoch start");
                            }
                            try {
                                t.addEpochStart(parseLong(pair.substring(0, at), lineNo), parseLong(pair.substring(at + 1), lineNo));
                            } catch (IllegalArgumentException e) {
                                throw corrupt(lineNo, e.getMessage());
                            }
                        }
                    }
                    index.tables.add(t);
                }
                case "end" -> {
                    expectFields(f, 1, lineNo);
                    isEnd = true;
                }
                default -> throw corrupt(lineNo, "unknown record");
            }
        }
        if (!isEnd) {
            throw new IndexFormatException("replication index is incomplete");
        }
        if (!hasDataId || !hasOwner) {
            throw new IndexFormatException("replication index lacks " + (hasDataId ? "owner" : "data_id"));
        }
        return index;
    }

    public void addTable(@NotNull IndexTable table) {
        tables.add(table);
    }

    public byte @NotNull [] encode() {
        final StringBuilder b = new StringBuilder(256 + tables.size() * 96);
        b.append(HEADER).append(' ').append(FORMAT_VERSION).append('\n');
        b.append("data_id ").append(Long.toHexString(dataIdHi)).append(' ').append(Long.toHexString(dataIdLo)).append('\n');
        b.append("owner ").append(ownerInstanceId == null ? "-" : escape(ownerInstanceId)).append(' ')
                .append(epoch).append(' ').append(renewedAt).append(' ').append(sequence).append(' ')
                .append(isReleased ? '1' : '0').append('\n');
        for (int i = 0, n = tables.size(); i < n; i++) {
            final IndexTable t = tables.getQuick(i);
            b.append("table ").append(t.getTableId()).append(' ').append(t.getCreateTimestamp()).append(' ')
                    .append(t.getKind()).append(' ').append(t.getState()).append(' ').append(t.getFirstTxn()).append(' ')
                    .append(t.getLastTxn()).append(' ')
                    .append(t.getPartTxnCount()).append(' ').append(t.getDroppedAt()).append(' ');
            final int m = t.getEpochStarts().size();
            if (m == 0) {
                b.append('-');
            } else {
                for (int j = 0; j < m; j += 2) {
                    if (j > 0) {
                        b.append(',');
                    }
                    b.append(t.getEpochStarts().getQuick(j)).append('@').append(t.getEpochStarts().getQuick(j + 1));
                }
            }
            b.append(' ').append(escape(t.getDirName())).append(' ').append(escape(t.getTableName())).append('\n');
        }
        b.append("end\n");
        return b.toString().getBytes(StandardCharsets.UTF_8);
    }

    public @Nullable IndexTable findTable(int tableId, long createTimestamp) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            final IndexTable t = tables.getQuick(i);
            if (t.getTableId() == tableId && t.getCreateTimestamp() == createTimestamp) {
                return t;
            }
        }
        return null;
    }

    public long getDataIdHi() {
        return dataIdHi;
    }

    public long getDataIdLo() {
        return dataIdLo;
    }

    public long getEpoch() {
        return epoch;
    }

    public @Nullable String getOwnerInstanceId() {
        return ownerInstanceId;
    }

    public long getRenewedAt() {
        return renewedAt;
    }

    public long getSequence() {
        return sequence;
    }

    public @NotNull ObjList<IndexTable> getTables() {
        return tables;
    }

    public boolean isReleased() {
        return isReleased;
    }

    public void removeTable(int index) {
        tables.remove(index);
    }

    public void setDataId(long hi, long lo) {
        this.dataIdHi = hi;
        this.dataIdLo = lo;
    }

    public void setOwner(@Nullable String ownerInstanceId, long epoch, long renewedAt, long sequence, boolean isReleased) {
        this.ownerInstanceId = ownerInstanceId;
        this.epoch = epoch;
        this.renewedAt = renewedAt;
        this.sequence = sequence;
        this.isReleased = isReleased;
    }

    private static IndexFormatException corrupt(int lineNo, String what) {
        return new IndexFormatException("corrupt replication index [line=" + lineNo + ", error=" + what + ']');
    }

    static String escape(String s) {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        final StringBuilder b = new StringBuilder(bytes.length + 8);
        for (byte value : bytes) {
            final int c = value & 0xff;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '~' || c == '-') {
                b.append((char) c);
            } else {
                b.append('%').append(Character.forDigit(c >>> 4, 16)).append(Character.forDigit(c & 0xf, 16));
            }
        }
        if (b.isEmpty()) {
            // an empty field would merge with its neighbour on split
            return "%";
        }
        return b.toString();
    }

    private static void expectFields(String[] f, int count, int lineNo) {
        if (f.length != count) {
            throw corrupt(lineNo, "expected " + count + " fields, found " + f.length);
        }
    }

    private static boolean parseFlag(String s, int lineNo) {
        return switch (s) {
            case "0" -> false;
            case "1" -> true;
            default -> throw corrupt(lineNo, "invalid flag");
        };
    }

    private static long parseHex(String s, int lineNo) {
        try {
            return Long.parseUnsignedLong(s, 16);
        } catch (NumberFormatException e) {
            throw corrupt(lineNo, "invalid hex number");
        }
    }

    private static int parseInt(String s, int lineNo) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw corrupt(lineNo, "invalid number");
        }
    }

    private static long parseLong(String s, int lineNo) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw corrupt(lineNo, "invalid number");
        }
    }

    static String unescape(String s, int lineNo) {
        if ("%".equals(s)) {
            return "";
        }
        final byte[] out = new byte[s.length()];
        int n = 0;
        for (int i = 0, len = s.length(); i < len; i++) {
            final char c = s.charAt(i);
            if (c == '%') {
                if (i + 2 >= len) {
                    throw corrupt(lineNo, "truncated escape");
                }
                final int hi = Character.digit(s.charAt(i + 1), 16);
                final int lo = Character.digit(s.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) {
                    throw corrupt(lineNo, "invalid escape");
                }
                out[n++] = (byte) (hi << 4 | lo);
                i += 2;
            } else if (c > 0x7f) {
                throw corrupt(lineNo, "unescaped non-ASCII character");
            } else {
                out[n++] = (byte) c;
            }
        }
        return new String(out, 0, n, StandardCharsets.UTF_8);
    }

    public static class IndexFormatException extends RuntimeException {
        public IndexFormatException(String message) {
            super(message, null, false, false);
        }
    }
}
