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

import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The checkpoint history of one instance: for each checkpoint it took, when, and the sequencer
 * transaction every replicated table had then. A node restored from such a checkpoint needs from the
 * object store only the transactions after those, which is what lets the WAL cleaner delete older
 * ones. Serialized as UTF-8 text:
 * <pre>
 * qdb-checkpoint-history 1
 * checkpoint &lt;timestamp micros&gt; &lt;table count&gt;
 * t &lt;table dir name, percent-encoded&gt; &lt;seq txn&gt;
 * ...
 * end
 * </pre>
 */
public final class CheckpointHistory {
    public static final int FORMAT_VERSION = 1;
    private static final String HEADER = "qdb-checkpoint-history";
    private final List<Checkpoint> checkpoints = new ArrayList<>();

    public static @NotNull CheckpointHistory decode(byte @NotNull [] data, int offset, int length) {
        final CheckpointHistory h = new CheckpointHistory();
        final String[] lines = new String(data, offset, length, StandardCharsets.UTF_8).split("\n", -1);
        if (lines.length < 2 || !(HEADER + ' ' + FORMAT_VERSION).equals(lines[0])) {
            throw new ReplicationIndex.IndexFormatException("not a checkpoint history");
        }
        Checkpoint current = null;
        int expected = 0;
        boolean isEnd = false;
        for (int i = 1; i < lines.length; i++) {
            final String line = lines[i];
            if (line.isEmpty()) {
                continue;
            }
            if (isEnd) {
                throw corrupt(i + 1, "content after end");
            }
            final String[] f = line.split(" ", -1);
            switch (f[0]) {
                case "checkpoint" -> {
                    if (current != null && expected != 0) {
                        throw corrupt(i + 1, "checkpoint lacks tables");
                    }
                    if (f.length != 3) {
                        throw corrupt(i + 1, "expected 3 fields");
                    }
                    current = new Checkpoint(parseLong(f[1], i + 1));
                    expected = (int) parseLong(f[2], i + 1);
                    if (expected < 0) {
                        throw corrupt(i + 1, "negative table count");
                    }
                    h.checkpoints.add(current);
                }
                case "t" -> {
                    if (current == null || expected == 0 || f.length != 3) {
                        throw corrupt(i + 1, "unexpected table record");
                    }
                    current.tables.put(ReplicationIndex.unescape(f[1], i + 1), parseLong(f[2], i + 1));
                    expected--;
                }
                case "end" -> {
                    if (expected != 0) {
                        throw corrupt(i + 1, "checkpoint lacks tables");
                    }
                    isEnd = true;
                }
                default -> throw corrupt(i + 1, "unknown record");
            }
        }
        if (!isEnd) {
            throw new ReplicationIndex.IndexFormatException("checkpoint history is incomplete");
        }
        return h;
    }

    public void add(@NotNull Checkpoint checkpoint) {
        checkpoints.add(checkpoint);
    }

    public byte @NotNull [] encode() {
        final StringBuilder b = new StringBuilder(64 + checkpoints.size() * 64);
        b.append(HEADER).append(' ').append(FORMAT_VERSION).append('\n');
        for (Checkpoint c : checkpoints) {
            b.append("checkpoint ").append(c.timestamp).append(' ').append(c.tables.size()).append('\n');
            for (Map.Entry<String, Long> e : c.tables.entrySet()) {
                b.append("t ").append(ReplicationIndex.escape(e.getKey())).append(' ').append(e.getValue()).append('\n');
            }
        }
        b.append("end\n");
        return b.toString().getBytes(StandardCharsets.UTF_8);
    }

    public @NotNull List<Checkpoint> getCheckpoints() {
        return checkpoints;
    }

    /**
     * Keeps the newest {@code count} checkpoints.
     */
    public void keepLatest(int count) {
        while (checkpoints.size() > count) {
            checkpoints.remove(0);
        }
    }

    private static ReplicationIndex.IndexFormatException corrupt(int lineNo, String what) {
        return new ReplicationIndex.IndexFormatException("corrupt checkpoint history [line=" + lineNo + ", error=" + what + ']');
    }

    private static long parseLong(String s, int lineNo) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw corrupt(lineNo, "invalid number");
        }
    }

    public static final class Checkpoint {
        private final LinkedHashMap<String, Long> tables = new LinkedHashMap<>();
        private final long timestamp;

        public Checkpoint(long timestamp) {
            this.timestamp = timestamp;
        }

        public void addTable(@NotNull String tableDir, long seqTxn) {
            tables.put(tableDir, seqTxn);
        }

        /**
         * The table's sequencer transaction at the checkpoint, -1 when the table was not part of it.
         */
        public long getSeqTxn(@NotNull String tableDir) {
            final Long v = tables.get(tableDir);
            return v == null ? -1 : v;
        }

        public @NotNull Map<String, Long> getTables() {
            return tables;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
