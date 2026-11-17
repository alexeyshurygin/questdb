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

import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.replication.store.ObjectStore;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * Per-thread working set of a replication worker: object I/O, native buffers and the file
 * readers/writers. Replication tasks borrow the context of the thread they run on.
 */
public class ReplicationWorker implements QuietCloseable {
    private final NativeBuffer buffer = new NativeBuffer();
    private final SegmentDelta delta = new SegmentDelta();
    private final SegmentDeltaBuilder deltaBuilder;
    private final SegmentDeltaWriter deltaWriter;
    private final ObjectIO io;
    private final NativeBuffer partBuffer = new NativeBuffer();
    private final ReplicaTableApplier tableApplier;
    private final TxnLogReader txnLogReader;

    public ReplicationWorker(
            @NotNull ObjectStore store,
            int compressionLevel,
            boolean isChecksum,
            @NotNull FilesFacade ff,
            int mkDirMode,
            @NotNull ReplicatedCairoEngine engine,
            @NotNull CharSequence workDir
    ) {
        this.io = new ObjectIO(store, compressionLevel, isChecksum);
        this.deltaBuilder = new SegmentDeltaBuilder(ff);
        this.deltaWriter = new SegmentDeltaWriter(ff, mkDirMode);
        this.txnLogReader = new TxnLogReader(ff);
        this.tableApplier = new ReplicaTableApplier(engine, workDir);
    }

    @Override
    public void close() {
        Misc.free(io);
        Misc.free(buffer);
        Misc.free(partBuffer);
        Misc.free(deltaBuilder);
        Misc.free(deltaWriter);
        Misc.free(txnLogReader);
        Misc.free(tableApplier);
    }

    public NativeBuffer getBuffer() {
        return buffer;
    }

    public SegmentDelta getDelta() {
        return delta;
    }

    public SegmentDeltaBuilder getDeltaBuilder() {
        return deltaBuilder;
    }

    public SegmentDeltaWriter getDeltaWriter() {
        return deltaWriter;
    }

    public ObjectIO getIo() {
        return io;
    }

    public NativeBuffer getPartBuffer() {
        return partBuffer;
    }

    public ReplicaTableApplier getTableApplier() {
        return tableApplier;
    }

    public TxnLogReader getTxnLogReader() {
        return txnLogReader;
    }
}
