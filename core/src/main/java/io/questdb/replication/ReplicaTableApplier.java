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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.vm.MemoryFCRImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableSequencer;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableSequencerImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.LittleEndian;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.TableCreateManifest;
import io.questdb.replication.codec.TxnRecord;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Applies replicated changes to a replica's tables through the engine's own machinery: it creates a
 * table with the primary's name, directory and id, and appends each shipped transaction to the
 * table's sequencer with the primary's coordinates and commit timestamp, so the transaction numbers,
 * structure versions and commit times match the primary's. The WAL apply job then applies the
 * transactions as it would local ones. Thread-safe per table; the caller serializes work on one table.
 */
public class ReplicaTableApplier implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(ReplicaTableApplier.class);
    private static final String META0_TEMP_FILE = "_meta.0.tmp";
    private static final AtomicLong META0_TEMP_FILE_COUNTER = new AtomicLong();
    private final NativeBuffer alterBuffer = new NativeBuffer();
    private final MemoryFCRImpl alterMem = new MemoryFCRImpl();
    private final CairoConfiguration configuration;
    private final String meta0TempFile;
    private final ReplicatedCairoEngine engine;
    private final FilesFacade ff;
    private final Path path = new Path();
    private final CharSequence workDir;

    /**
     * @param workDir node-local scratch directory, outside the database directory
     */
    public ReplicaTableApplier(@NotNull ReplicatedCairoEngine engine, @NotNull CharSequence workDir) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.ff = configuration.getFilesFacade();
        this.workDir = workDir;
        // appliers of different workers create tables at the same time in the same work directory
        this.meta0TempFile = META0_TEMP_FILE + '.' + META0_TEMP_FILE_COUNTER.incrementAndGet();
    }

    /**
     * Reads the new table name from a serialized RENAME_TABLE change: {@code short command,
     * int tableNamePosition, int longCount, long[longCount], int stringCount, then strings as
     * int charCount + UTF-16LE chars}; the strings are {@code [oldName, newName]}.
     */
    public static @NotNull String readRenameTarget(byte @NotNull [] payload) {
        int pos = 2 + 4;
        final int longCount = LittleEndian.getInt(payload, pos);
        pos += 4 + 8 * longCount;
        final int stringCount = LittleEndian.getInt(payload, pos);
        pos += 4;
        if (stringCount < 2) {
            throw CairoException.critical(0).put("rename change carries no target name");
        }
        String name = null;
        for (int i = 0; i < 2; i++) {
            final int charCount = LittleEndian.getInt(payload, pos);
            pos += 4;
            if (charCount < 0 || pos + 2L * charCount > payload.length) {
                throw CairoException.critical(0).put("invalid rename change payload");
            }
            name = new String(payload, pos, 2 * charCount, StandardCharsets.UTF_16LE);
            pos += 2 * charCount;
        }
        return name;
    }

    /**
     * The command of a serialized structural change.
     */
    public static short readCommand(byte @NotNull [] payload) {
        return LittleEndian.getShort(payload, 0);
    }

    /**
     * Appends one shipped transaction to the table's sequencer as transaction {@code record.getTxn()}.
     * For a data transaction, the caller has already written its WAL segment bytes.
     *
     * @return the table token after the transaction (a rename changes it)
     * @throws ReplicationException when the replica's log does not end right before the transaction,
     *                              or its structure version differs from the primary's
     */
    public @NotNull TableToken apply(@NotNull TableToken token, @NotNull TxnRecord record) {
        final TableSequencerAPI api = engine.getTableSequencerAPI();
        final long expectedLastTxn = record.getTxn() - 1;
        if (record.isData()) {
            // the replica never allocates WAL ids, but a promoted replica must not reuse the primary's
            api.advanceWalIdTo(token, record.getWalId());
            final long txn = api.nextReplicatedTxn(
                    token,
                    expectedLastTxn,
                    record.getWalId(),
                    record.getStructureVersion(),
                    record.getSegmentId(),
                    record.getSegmentTxn(),
                    record.getCommitTimestamp(),
                    record.getTxnMinTimestamp(),
                    record.getTxnMaxTimestamp(),
                    record.getRowCount()
            );
            checkTxn(token, record, txn);
            return token;
        }
        if (record.isStructural()) {
            final byte[] payload = record.getPayload();
            if (payload == null) {
                throw ReplicationException.corrupt("structural transaction without its change [table=" + token.getTableName()
                        + ", txn=" + record.getTxn() + ']');
            }
            final AlterOperation alter = deserialize(payload);
            final long txn = api.nextReplicatedStructureTxn(
                    token,
                    expectedLastTxn,
                    record.getStructureVersion() - 1,
                    alter,
                    record.getCommitTimestamp()
            );
            checkTxn(token, record, txn);
            if (alter.getCommand() == AlterOperation.RENAME_TABLE) {
                final TableToken renamed = token.renamed(readRenameTarget(payload));
                engine.applyTableRename(token, renamed);
                LOG.info().$("replicated table rename [from=").$(token).$(", to=").$(renamed).I$();
                return renamed;
            }
            return token;
        }
        if (record.isDrop()) {
            if (api.lastTxn(token) != expectedLastTxn) {
                throw ReplicationException.divergence("replica transaction log does not end before the drop [table="
                        + token.getTableName() + ", txn=" + record.getTxn() + ", local=" + api.lastTxn(token) + ']');
            }
            path.of(configuration.getDbRoot());
            engine.dropReplicatedTable(path, token);
            LOG.info().$("replicated table drop [table=").$(token).$(", txn=").$(record.getTxn()).I$();
            return token;
        }
        throw ReplicationException.corrupt("unknown transaction kind [table=" + token.getTableName() + ", txn=" + record.getTxn()
                + ", walId=" + record.getWalId() + ']');
    }

    @Override
    public void close() {
        alterBuffer.close();
        path.close();
    }

    /**
     * Creates the replica's copy of a table: same name, directory name, id and initial metadata as
     * the primary's, with an empty transaction log ready for the shipped transactions.
     */
    public @NotNull TableToken createTable(@NotNull TableCreateManifest manifest) {
        if (manifest.getKind() != IndexTable.KIND_TABLE) {
            throw ReplicationException.unsupported("only tables are replicated in this version [table=" + manifest.getTableName()
                    + ", kind=" + manifest.getKind() + ']');
        }
        final String dirName = manifest.getDirName();
        final TableToken existing = engine.getTableTokenByDirName(dirName);
        if (existing != null) {
            return existing;
        }
        final CharSequence root = configuration.getDbRoot();
        if (TableUtils.exists(ff, path, root, dirName) != TableUtils.TABLE_DOES_NOT_EXIST) {
            throw ReplicationException.divergence("table directory exists but the table is not registered [dir=" + dirName + ']');
        }
        // keep local ids above the primary's, so that a promoted replica never reuses one
        engine.advanceTableIdTo(manifest.getTableId());
        final TableToken token = engine.lockTableName(manifest.getTableName(), dirName, manifest.getTableId(), false, false, false, true);
        if (token == null) {
            throw ReplicationException.divergence("table name is taken on the replica [table=" + manifest.getTableName() + ']');
        }
        boolean isRegistered = false;
        try {
            while (!engine.lockTableCreate(token)) {
                Thread.onSpinWait();
            }
            try {
                try (TableWriterMetadata metadata = loadInitialMetadata(token, manifest.getMeta0());
                     MemoryMARW mem = Vm.getCMARWInstance()) {
                    if (!metadata.isWalEnabled()) {
                        throw ReplicationException.unsupported("non-WAL tables are not replicated [table=" + manifest.getTableName() + ']');
                    }
                    // resolved by the engine: a replica does not flush, the object store is its durable copy
                    final int commitMode = engine.getResolvedCommitMode();
                    TableUtils.createTableOrViewOrMatView(
                            ff,
                            root,
                            configuration.getMkDirMode(),
                            null,
                            mem,
                            null,
                            path,
                            dirName,
                            metadata,
                            ColumnType.VERSION,
                            manifest.getTableId(),
                            commitMode
                    );
                    path.of(root).concat(dirName);
                    TableSequencerImpl.createSequencerFiles(
                            configuration,
                            engine.getWalDirectoryPolicy(),
                            path,
                            metadata,
                            token,
                            manifest.getTableId(),
                            manifest.getCreateTimestamp()
                    );
                    if (CommitMode.isPowerLossDurable(commitMode)) {
                        TableUtils.fsyncDirDurable(ff, path.of(root).$());
                    }
                }
                engine.registerTableToken(token);
                isRegistered = true;
            } finally {
                engine.unlockTableCreate(token);
            }
        } finally {
            if (!isRegistered) {
                engine.unlockTableName(token);
            }
        }
        engine.getTableSequencerAPI().initTxnTracker(token, 0, 0);
        LOG.info().$("replicated table created [table=").$(token).$(", tableId=").$(manifest.getTableId()).I$();
        return token;
    }

    private void checkTxn(TableToken token, TxnRecord record, long txn) {
        if (txn == record.getTxn()) {
            return;
        }
        if (txn == TableSequencer.FENCE_REJECTED) {
            throw ReplicationException.divergence("replica transaction log does not end before the shipped transaction [table="
                    + token.getTableName() + ", txn=" + record.getTxn() + ", local=" + engine.getTableSequencerAPI().lastTxn(token) + ']');
        }
        if (txn == TableSequencer.NO_TXN) {
            throw ReplicationException.divergence("structure version differs from the primary's [table=" + token.getTableName()
                    + ", txn=" + record.getTxn() + ", structureVersion=" + record.getStructureVersion() + ']');
        }
        throw ReplicationException.divergence("transaction number differs from the primary's [table=" + token.getTableName()
                + ", expected=" + record.getTxn() + ", actual=" + txn + ']');
    }

    private AlterOperation deserialize(byte[] payload) {
        alterBuffer.clear();
        alterBuffer.putBytes(payload, 0, payload.length);
        alterMem.of(alterBuffer.ptr(), alterBuffer.size());
        final AlterOperation op = new AlterOperation();
        op.deserializeBody(alterMem, 0, alterBuffer.size());
        // detaches the strings from the buffer: re-serializing a freshly deserialized change drops them
        return op.deepClone();
    }

    private TableWriterMetadata loadInitialMetadata(TableToken token, byte[] meta0) {
        path.of(workDir);
        if (ff.mkdirs(path.slash(), configuration.getMkDirMode()) != 0) {
            throw CairoException.critical(ff.errno()).put("could not create replication work directory [path=").put(path).put(']');
        }
        path.of(workDir).concat(meta0TempFile);
        final long fd = ff.openCleanRW(path.$(), meta0.length);
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not create file [path=").put(path).put(']');
        }
        try {
            final long buf = alterBuffer.ensureCapacity(meta0.length);
            io.questdb.std.Unsafe.copyMemory(meta0, io.questdb.std.Unsafe.BYTE_OFFSET, null, buf, meta0.length);
            if (ff.write(fd, buf, meta0.length, 0) != meta0.length) {
                throw CairoException.critical(ff.errno()).put("could not write file [path=").put(path).put(']');
            }
        } finally {
            ff.close(fd);
        }
        final TableWriterMetadata metadata = new TableWriterMetadata(token);
        try (MemoryCMR mem = Vm.getCMRInstance(ff, path.$(), meta0.length, MemoryTag.MMAP_DEFAULT)) {
            metadata.reload(path, mem);
        } catch (Throwable th) {
            metadata.close();
            throw th;
        } finally {
            ff.remove(path.$());
        }
        return metadata;
    }
}
