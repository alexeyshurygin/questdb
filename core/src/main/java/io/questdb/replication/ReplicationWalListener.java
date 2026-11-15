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


package io.questdb.replication;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalListener;
import org.jetbrains.annotations.NotNull;

/**
 * Feeds WAL events to the primary's uploader. Every callback runs on the committing thread, some
 * under the table's sequencer lock, so each one only records the event and wakes the uploader.
 */
public class ReplicationWalListener implements WalListener {
    private final Sink sink;

    public ReplicationWalListener(@NotNull Sink sink) {
        this.sink = sink;
    }

    @Override
    public void dataTxnCommitted(TableToken tableToken, long txn, long timestamp, int walId, int segmentId, int segmentTxn) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onCommit(tableToken, txn, false);
        }
    }

    @Override
    public void nonDataTxnCommitted(TableToken tableToken, long txn, long timestamp) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onCommit(tableToken, txn, true);
        }
    }

    @Override
    public void segmentClosed(TableToken tableToken, long txn, int walId, int segmentId) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onSegmentClosed(tableToken, walId, segmentId);
        }
    }

    @Override
    public void tableCreated(TableToken tableToken, long timestamp) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onTableCreated(tableToken);
        }
    }

    @Override
    public void tableDropped(TableToken tableToken, long txn, long timestamp) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onCommit(tableToken, txn, true);
        }
    }

    @Override
    public void tableRenamed(TableToken tableToken, long txn, long timestamp, TableToken oldTableToken) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onCommit(tableToken, txn, true);
        }
    }

    @Override
    public void walClosed(TableToken tableToken, long txn, int walId) {
        if (ReplicationWalDirectoryPolicy.isReplicated(tableToken)) {
            sink.onWalClosed(tableToken, walId);
        }
    }

    /**
     * Receives the events; implementations must not block or call into the sequencer.
     */
    public interface Sink {
        /**
         * @param isNonData a structural change, rename or drop, uploaded without waiting for the
         *                  throttle window unless {@code replication.primary.throttle.non.data} is set
         */
        void onCommit(TableToken token, long txn, boolean isNonData);

        void onSegmentClosed(TableToken token, int walId, int segmentId);

        void onTableCreated(TableToken token);

        void onWalClosed(TableToken token, int walId);
    }
}
