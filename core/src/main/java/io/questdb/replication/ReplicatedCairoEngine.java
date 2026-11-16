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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CheckpointListener;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.mv.NoOpMatViewStateStore;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.ViewWalWriter;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.codec.QwpServerInfoProvider;
import io.questdb.griffin.engine.ops.CreateMatViewOperation;
import io.questdb.griffin.engine.ops.CreateViewOperation;
import io.questdb.griffin.engine.ops.OperationDispatcher;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.LongSupplier;

/**
 * The storage engine of a replicated node. It carries the node's replication role and refuses client
 * writes whenever the node is not a writable primary: a replica, a primary still catching up with
 * the object store, and a primary that lost its ownership lease all answer {@link #isReadOnlyMode()}
 * with true, which every SQL, PGWire, HTTP, ILP and QWP write path consults. Replication itself writes
 * through the table sequencer and the WAL apply job, which these client paths do not gate.
 * <p>
 * Constructed with partial initialization: {@code ServerMain}'s engine envelope runs
 * {@link #completeInit()} after the replication hooks are installed.
 */
public class ReplicatedCairoEngine extends CairoEngine {
    private static final Log LOG = LogFactory.getLog(ReplicatedCairoEngine.class);
    private final QwpServerInfoProvider qwpServerInfoProvider = new RoleAwareQwpServerInfoProvider();
    private final ReplicationConfiguration replicationConfiguration;
    // bumped on every role change, reported to QWP clients so they notice a failover
    private final AtomicLong roleEpoch = new AtomicLong();
    private volatile @Nullable CheckpointListener checkpointListener;
    private volatile @Nullable String fenceReason;
    private volatile boolean isDataIdMintedAtBoot;
    // a primary acknowledges writes until this moment; the replication service moves it with every
    // index write that the object store accepted
    private volatile long leaseValidUntilNanos = System.nanoTime();
    // the clock of the lease and of every replication timing; System.nanoTime() outside tests
    private volatile LongSupplier nanoClock = System::nanoTime;
    // the threads of the replication service; close() frees nothing while one of them may still run
    private volatile @Nullable ReplicationThreads replicationThreads;
    private volatile EngineRole role;

    public ReplicatedCairoEngine(
            CairoConfiguration configuration,
            @NotNull WalLocker walLocker,
            @NotNull DurabilityFailureHandler durabilityFailureHandler,
            @NotNull ReplicationConfiguration replicationConfiguration
    ) {
        super(configuration, walLocker, false, durabilityFailureHandler);
        this.replicationConfiguration = replicationConfiguration;
        switch (replicationConfiguration.getRole()) {
            case PRIMARY -> role = EngineRole.PRIMARY_CATCHUP;
            case REPLICA -> {
                role = EngineRole.REPLICA;
                // A replica keeps no save points: nothing downloads WAL again after a crash, the replica
                // resumes from its local transaction log, so a save point would rewind onto purged WAL.
                setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            }
            default -> throw new IllegalArgumentException("replication is disabled");
        }
        LOG.info().$("replication enabled [role=").$(replicationConfiguration.getRole().getConfigName())
                .$(", engineRole=").$(role.name()).I$();
    }

    /**
     * REBASE WAL starts a table's history over from local seed transactions, which the object store
     * cannot express; a replicated node refuses both variants.
     */
    @Override
    protected void assertRebaseRole(boolean replicaVariant) {
        throw CairoException.nonCritical().put("REBASE WAL is not supported when replication is enabled");
    }

    /**
     * Frees the engine once no replication thread can use it any more. The replication service's own
     * stop gives up on a thread stuck in a request that ignores interrupts; this waits for it a while
     * longer and, if it still runs, leaves the engine open rather than free memory under it. A later
     * call tries again.
     */
    @Override
    public void close() {
        final ReplicationThreads threads = replicationThreads;
        if (threads != null && !threads.awaitEnd()) {
            LOG.critical().$("a replication thread is still running; the engine stays open and its memory is not freed").$();
            return;
        }
        super.close();
    }

    /**
     * Refuses materialized views while read-only: they do not replicate, and a read-only node creates
     * nothing a client asks for.
     */
    @Override
    public @NotNull MatViewDefinition createMatView(
            SecurityContext securityContext,
            MemoryMARW mem,
            BlockFileWriter blockFileWriter,
            Path path,
            boolean ifNotExists,
            CreateMatViewOperation operation,
            boolean keepLock,
            boolean inVolume
    ) {
        if (isReadOnlyMode()) {
            throw CairoException.readOnlyAccess();
        }
        return super.createMatView(securityContext, mem, blockFileWriter, path, ifNotExists, operation, keepLock, inVolume);
    }

    /**
     * Refuses client-created replicated tables while read-only. A read-only node still creates its own
     * system tables and the temporary tables that query exports materialize.
     */
    @Override
    public @NotNull TableToken createTable(
            SecurityContext securityContext,
            MemoryMARW mem,
            Path path,
            boolean ifNotExists,
            TableStructure struct,
            boolean keepLock,
            boolean inVolume,
            int tableKind
    ) {
        if (isReadOnlyMode()
                && tableKind != TableUtils.TABLE_KIND_TEMP_PARQUET_EXPORT
                && struct.isWalEnabled()
                && !isSystemTableName(struct.getTableName())) {
            throw CairoException.readOnlyAccess();
        }
        return super.createTable(securityContext, mem, path, ifNotExists, struct, keepLock, inVolume, tableKind);
    }

    /**
     * Refuses client drops of replicated tables while read-only. Replication applies a primary's drop
     * through {@link #dropReplicatedTable(Path, TableToken)}.
     */
    /**
     * Refuses views while read-only, as every other DDL.
     */
    @Override
    public @NotNull ViewDefinition createView(
            SecurityContext securityContext,
            MemoryMARW mem,
            BlockFileWriter blockFileWriter,
            Path path,
            boolean ifNotExists,
            CreateViewOperation operation,
            @Nullable RecordMetadata metadata
    ) {
        if (isReadOnlyMode()) {
            throw CairoException.readOnlyAccess();
        }
        return super.createView(securityContext, mem, blockFileWriter, path, ifNotExists, operation, metadata);
    }

    @Override
    public void dropTableOrViewOrMatView(Path path, TableToken tableToken) {
        checkReplicatedWrite(tableToken);
        super.dropTableOrViewOrMatView(path, tableToken);
    }

    /**
     * Drops a table on behalf of replication, bypassing the read-only fence.
     */
    public void dropReplicatedTable(Path path, TableToken tableToken) {
        super.dropTableOrViewOrMatView(path, tableToken);
    }

    @Override
    public @NotNull CheckpointListener getCheckpointListener() {
        final CheckpointListener listener = checkpointListener;
        return listener != null ? listener : super.getCheckpointListener();
    }

    /**
     * Why the node fenced itself, null unless {@link #getRole()} is {@link EngineRole#FENCED}.
     */
    public @Nullable String getFenceReason() {
        return fenceReason;
    }

    /**
     * Test seam: the {@link System#nanoTime()} until which the primary's lease is current.
     */
    @org.jetbrains.annotations.TestOnly
    public long getLeaseValidUntilNanos() {
        return leaseValidUntilNanos;
    }

    /**
     * The time on the scale of {@link System#nanoTime()} by which the lease and the replication
     * service's timings are measured.
     */
    public long getNanoTime() {
        return nanoClock.getAsLong();
    }

    @Override
    public @NotNull QwpServerInfoProvider getQwpServerInfoProvider() {
        return qwpServerInfoProvider;
    }

    public @NotNull ReplicationConfiguration getReplicationConfiguration() {
        return replicationConfiguration;
    }

    /**
     * The role in effect. A primary whose lease lapsed reports {@link EngineRole#PRIMARY_CATCHUP}: it
     * refuses writes until it has renewed the lease, and clients treat that role as transient.
     */
    public @NotNull EngineRole getRole() {
        final EngineRole r = role;
        return r == EngineRole.PRIMARY && isLeaseLapsed() ? EngineRole.PRIMARY_CATCHUP : r;
    }

    @Override
    public TableWriterAPI getTableWriterAPI(TableToken tableToken, @NotNull String lockReason) {
        checkReplicatedWrite(tableToken);
        return super.getTableWriterAPI(tableToken, lockReason);
    }

    @Override
    public TableWriterAPI getTableWriterAPI(CharSequence tableName, @NotNull String lockReason) {
        if (isReadOnlyMode()) {
            final TableToken token = getTableTokenIfExists(tableName);
            if (token != null) {
                checkReplicatedWrite(token);
            }
        }
        return super.getTableWriterAPI(tableName, lockReason);
    }

    @Override
    public @NotNull ViewWalWriter getViewWalWriter(TableToken viewToken) {
        checkReplicatedWrite(viewToken);
        return super.getViewWalWriter(viewToken);
    }

    @Override
    public @NotNull WalWriter getWalWriter(TableToken tableToken) {
        checkReplicatedWrite(tableToken);
        return super.getWalWriter(tableToken);
    }

    /**
     * Refuses client TableWriters of replicated tables while read-only, except for the break-glass
     * alters {@link #isForceAlterAllowed()} permits.
     */
    @Override
    public TableWriter getWriter(TableToken tableToken, @NotNull String lockReason) {
        if (!(OperationDispatcher.FORCE_OPERATION_APPLY_REASON.equals(lockReason) && isForceAlterAllowed())) {
            checkReplicatedWrite(tableToken);
        }
        return super.getWriter(tableToken, lockReason);
    }

    /**
     * A read-only replica permits the break-glass alters that bypass the WAL (they mint no
     * replicated transaction), as QuestDB Enterprise does; the {@code readonly} lockdown still refuses
     * them.
     */
    @Override
    public boolean isForceAlterAllowed() {
        return role == EngineRole.REPLICA && !getConfiguration().isReadOnlyInstance();
    }

    /**
     * True for a primary that has not renewed its ownership lease in time. Another node may take the
     * object store over once the lease is three keepalive intervals old, so a primary stops
     * acknowledging writes before that: what it accepted after a takeover could never be replicated.
     * The check reads the clock on every call, so it holds while the replication coordinator is stuck
     * in a request to an unreachable store.
     */
    public boolean isLeaseLapsed() {
        return role == EngineRole.PRIMARY && getNanoTime() - leaseValidUntilNanos >= 0;
    }

    /**
     * True when the node holds the primary role, whether or not its lease is current.
     */
    public boolean isPrimaryRole() {
        return role == EngineRole.PRIMARY;
    }

    @Override
    public boolean isReadOnlyMode() {
        return role != EngineRole.PRIMARY || isLeaseLapsed() || super.isReadOnlyMode();
    }

    /**
     * Records a lease renewal: the node may act as primary until {@code validUntilNanos}
     * ({@link #getNanoTime()} scale). Only moves the bound forward.
     */
    public void renewLease(long validUntilNanos) {
        if (validUntilNanos - leaseValidUntilNanos > 0) {
            leaseValidUntilNanos = validUntilNanos;
        }
    }

    /**
     * Test seam: replaces the clock of {@link #getNanoTime()}. Set it before the replication service
     * starts; a clock that jumps ahead stands in for a request that takes that long.
     */
    @org.jetbrains.annotations.TestOnly
    public void setNanoClockForTesting(@NotNull LongSupplier nanoClock) {
        this.nanoClock = nanoClock;
    }

    @Override
    public TableToken rename(
            SecurityContext securityContext,
            Path fromPath,
            MemoryMARW memory,
            CharSequence fromTableName,
            Path toPath,
            CharSequence toTableName
    ) {
        // RENAME TABLE runs while the statement compiles, before any per-protocol gate
        if (isReadOnlyMode()) {
            throw CairoException.readOnlyAccess();
        }
        return super.rename(securityContext, fromPath, memory, fromTableName, toPath, toTableName);
    }

    @Override
    public void replaceViewDefinition(
            TableToken viewToken,
            @NotNull String viewSql,
            @NotNull LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies,
            BlockFileWriter blockFileWriter,
            Path path
    ) {
        // ALTER VIEW runs while the statement compiles, before any per-protocol gate
        if (isReadOnlyMode()) {
            throw CairoException.readOnlyAccess();
        }
        super.replaceViewDefinition(viewToken, viewSql, dependencies, blockFileWriter, path);
    }

    /**
     * Installs the listener that records checkpoints into the replication checkpoint history; null
     * restores the configured one.
     */
    public void setCheckpointListener(@Nullable CheckpointListener checkpointListener) {
        this.checkpointListener = checkpointListener;
    }

    /**
     * Installs the threads that {@link #close()} waits for; the replication service installs its own.
     */
    public void setReplicationThreads(@Nullable ReplicationThreads replicationThreads) {
        this.replicationThreads = replicationThreads;
    }

    /**
     * Changes the role under the role-switch write lock, so that no write that checked the old role
     * is still in flight when the new one is published.
     *
     * @param reason why the node fenced itself, for {@link EngineRole#FENCED}; ignored otherwise
     */
    public void setRole(@NotNull EngineRole newRole, @Nullable String reason) {
        final Lock lock = getRoleSwitchWriteLock();
        lock.lock();
        try {
            final EngineRole old = role;
            role = newRole;
            roleEpoch.incrementAndGet();
            fenceReason = newRole == EngineRole.FENCED ? reason : null;
            if (old != newRole) {
                LOG.advisory().$("replication role changed [from=").$(old.name()).$(", to=").$(newRole.name())
                        .$(", reason=").$(reason).I$();
            }
        } finally {
            lock.unlock();
        }
    }

    // Live views are node-local derived data: every node refreshes its own, so they stay writable.
    // Non-WAL tables are not replicated; a replica creates some of its own (query export temp tables),
    // and system tables are node-local too.
    private void checkReplicatedWrite(TableToken token) {
        if (isReadOnlyMode() && token.isWal() && !token.isLiveView() && !token.isSystem()) {
            throw CairoException.readOnlyAccess();
        }
    }

    private boolean isSystemTableName(CharSequence tableName) {
        final CharSequence prefix = getConfiguration().getSystemTableNamePrefix();
        return tableName != null && prefix.length() > 0 && io.questdb.std.Chars.startsWith(tableName, prefix);
    }

    /**
     * A replica refreshes no materialized view. Materialized views do not replicate: one that the node
     * created while it was a primary stays as it was until the node is a primary again.
     */
    @Override
    protected MatViewStateStore createMatViewStateStore() {
        return role == EngineRole.REPLICA ? NoOpMatViewStateStore.INSTANCE : super.createMatViewStateStore();
    }

    /**
     * A replica adopts the DataID of the database it follows, from the object store; minting a
     * random one here would make the store refuse it.
     */
    @Override
    protected void initDataID() {
        if (role != EngineRole.REPLICA) {
            final boolean wasInitialized = getDataID().isInitialized();
            super.initDataID();
            isDataIdMintedAtBoot = !wasInitialized && getDataID().isInitialized();
        }
    }

    /**
     * True when this boot minted the database id: the node started without one, so it may still take
     * the id of the object store it joins.
     */
    public boolean isDataIdMintedAtBoot() {
        return isDataIdMintedAtBoot;
    }

    private class RoleAwareQwpServerInfoProvider implements QwpServerInfoProvider {
        @Override
        public int getCapabilities() {
            return ReplicatedCairoEngine.super.getQwpServerInfoProvider().getCapabilities();
        }

        @Override
        public CharSequence getClusterId() {
            return ReplicatedCairoEngine.super.getQwpServerInfoProvider().getClusterId();
        }

        @Override
        public long getEpoch() {
            return roleEpoch.get();
        }

        @Override
        public CharSequence getNodeId() {
            return ReplicatedCairoEngine.super.getQwpServerInfoProvider().getNodeId();
        }

        @Override
        public CharSequence getZoneId() {
            return ReplicatedCairoEngine.super.getQwpServerInfoProvider().getZoneId();
        }

        @Override
        public byte role() {
            return switch (getRole()) {
                case PRIMARY -> QwpEgressMsgKind.ROLE_PRIMARY;
                case PRIMARY_CATCHUP -> QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP;
                // a fenced primary refuses writes like a replica
                case REPLICA, FENCED -> QwpEgressMsgKind.ROLE_REPLICA;
            };
        }
    }

    public enum EngineRole {
        /**
         * Writable primary that holds the object store's ownership lease.
         */
        PRIMARY,
        /**
         * Primary role configured, still downloading what the object store holds or waiting for the
         * lease; writes are refused.
         */
        PRIMARY_CATCHUP,
        /**
         * Read-only node following the object store.
         */
        REPLICA,
        /**
         * Primary that lost its lease, met a diverging history or a different database in the store:
         * read-only until restarted with a fixed configuration.
         */
        FENCED
    }

    /**
     * The threads of the replication service, which use the engine until they end.
     */
    @FunctionalInterface
    public interface ReplicationThreads {
        /**
         * Waits, for a bounded time, until no replication thread can use the engine any more.
         *
         * @return true once none can; false when one still may after the wait
         */
        boolean awaitEnd();
    }
}
