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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DataID;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs replication on a node: on a primary it catches up with the object store, takes the ownership
 * lease and then uploads committed WAL transactions in batches, one index commit per batch; on a
 * replica it polls the index and applies what the store holds past the local log. One coordinator
 * thread drives the rounds; a pool of worker threads uploads or downloads the tables of a round in
 * parallel.
 * <p>
 * Lifecycle: construct and {@link #installHooks()} before the engine's {@code completeInit()}, so no
 * sequencer opens without the replication WAL policy; {@link #start()} once the engine is loaded;
 * {@link #close()} at shutdown, which drains a primary's pending uploads and releases its lease.
 */
public class ReplicationService implements QuietCloseable, ReplicationWalListener.Sink, ReplicationWalDirectoryPolicy.Pins,
        io.questdb.cairo.CheckpointListener {
    private static final long CATCHUP_BATCH = 10_000;
    private static final String INSTANCE_FILE = "instance";
    private static final Log LOG = LogFactory.getLog(ReplicationService.class);
    /**
     * An empty file of this name in the install root lets a node that does not own the object store, or
     * is behind it, become its primary. The node removes it once it is the primary.
     */
    public static final String MIGRATE_PRIMARY_FILE = "_migrate_primary";
    private static final long UPLOAD_BATCH = 10_000;
    private static final int WHOLE_WAL = -1;
    private final ConcurrentLinkedQueue<ReplicationWorker> allWorkers = new ConcurrentLinkedQueue<>();
    private final WalCleaner cleaner;
    private final io.questdb.cairo.CheckpointListener configuredCheckpointListener;
    private final ConcurrentLinkedQueue<io.questdb.replication.codec.CheckpointHistory.Checkpoint> pendingCheckpoints = new ConcurrentLinkedQueue<>();
    private final ReplicationConfiguration config;
    private final ConcurrentHashMap<String, TableToken> createdTables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> dirtyTables = new ConcurrentHashMap<>();
    private final ReplicatedCairoEngine engine;
    private final ExecutorService executor;
    private final IndexClient indexClient;
    private final boolean isChecksum;
    private final boolean isPrimary;
    private final ReentrantLock lock = new ReentrantLock();
    private final Path path = new Path();
    private final ReplicaSegmentPins pins;
    private final ReplicationWalDirectoryPolicy policy;
    private final HashMap<String, TableReplayer> replayers = new HashMap<>();
    // dropped tables the store still lists, carried over into every index until the cleaner removes them
    private final ConcurrentHashMap<String, IndexTable> retainedEntries = new ConcurrentHashMap<>();
    private final ObjectStore store;
    private final ConcurrentHashMap<String, TableUploader> uploaders = new ConcurrentHashMap<>();
    private final Condition wakeUp = lock.newCondition();
    // closes of segments and WALs (segment WHOLE_WAL) of tables without an uploader yet: {walId, segmentId, lastTxn}
    private final Map<String, List<long[]>> earlyCloses = new HashMap<>();
    // objects the WAL cleaner could not delete, or did not get to; coordinator thread only
    private final List<String> pendingDeletes = new ArrayList<>();
    private final ThreadLocal<ReplicationWorker> workerSlot = new ThreadLocal<>();
    private final String workDir;
    private volatile long epoch;
    private volatile String instanceId;
    // the WAL cleaner changed index entries it could not commit yet; coordinator thread only
    private boolean isCleanerCommitPending;
    private volatile boolean isClosed;
    // an operator allowed this node to take over a store it does not own or is behind; read once at start
    private volatile boolean isMigrationAllowed;
    // when close() began waiting for the coordinator; 0 until then
    private volatile long closeStartedNanos;
    private volatile boolean isCrashed;
    private volatile boolean isNonDataPending;
    private volatile boolean isRestoredAtBoot;
    private volatile boolean isRunning;
    private volatile boolean isStarted;
    private io.questdb.replication.codec.CheckpointHistory checkpointHistory;
    private ReplicationIndex lastIndex;
    private long lastIndexWriteNanos;
    private Thread mainThread;
    private long nextCheckpointUploadNanos;
    private long nextCleanerRunNanos;
    private volatile long sequence;
    private volatile String status = "starting";

    public ReplicationService(@NotNull ReplicatedCairoEngine engine, @NotNull ObjectStore store) {
        this.engine = engine;
        this.config = engine.getReplicationConfiguration();
        this.store = store;
        this.isPrimary = config.getRole() == ReplicationRole.PRIMARY;
        final CairoConfiguration cairoConfig = engine.getConfiguration();
        this.workDir = Paths.get(cairoConfig.getInstallRoot(), ".replication").toString();
        this.isChecksum = ObjectIO.isChecksum(config.getChecksumMode(), store);
        this.indexClient = new IndexClient(store, isChecksum);
        this.cleaner = new WalCleaner(config, store);
        this.configuredCheckpointListener = cairoConfig.getCheckpointListener();
        this.pins = isPrimary ? null : new ReplicaSegmentPins(10 * 60_000L);
        this.policy = new ReplicationWalDirectoryPolicy(cairoConfig.getFilesFacade(), isPrimary ? this : null, pins);
        final int threads = Math.max(1, Math.min(config.getMaxConcurrentRequests(), config.getMaxBatchSizeFast()));
        // No queue: a task goes to an idle thread, else to a new one up to the limit, else the submitting
        // thread runs it. Threads end after a minute of idleness, and each frees its context as it ends,
        // so the contexts alive match the concurrency in use.
        this.executor = new ThreadPoolExecutor(
                0,
                threads,
                60,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                r -> {
                    final Thread t = new Thread(() -> {
                        try {
                            r.run();
                        } finally {
                            releaseWorkerOfThisThread();
                            Path.clearThreadLocals();
                        }
                    }, "qdb-replication-worker");
                    t.setDaemon(true);
                    return t;
                },
                (task, pool) -> {
                    if (pool.isShutdown()) {
                        throw new RejectedExecutionException("replication is stopping");
                    }
                    task.run();
                }
        );
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        closeStartedNanos = System.nanoTime();
        isRunning = false;
        wake();
        final Thread t = mainThread;
        if (t != null) {
            try {
                t.join(shutdownTimeoutMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                LOG.critical().$("replication coordinator did not stop in time").$();
                t.interrupt();
            }
        }
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ReplicationWorker w;
        while ((w = allWorkers.poll()) != null) {
            Misc.free(w);
        }
        indexClient.close();
        Misc.free(policy);
        Misc.free(pins);
        Misc.free(store);
        path.close();
    }

    /**
     * Test seam: stops the coordinator as a crash would, without draining uploads or releasing the
     * lease, so a successor must wait for the lease to expire.
     */
    @org.jetbrains.annotations.TestOnly
    public void crashForTesting() {
        isCrashed = true;
        isRunning = false;
        wake();
        final Thread t = mainThread;
        if (t != null) {
            try {
                t.join(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * The number of per-thread contexts alive: one for the coordinator and one for each pool thread
     * that has not ended.
     */
    @org.jetbrains.annotations.TestOnly
    public int getWorkerContextCount() {
        return allWorkers.size();
    }

    /**
     * Test seam: how long an idle pool thread lives.
     */
    @org.jetbrains.annotations.TestOnly
    public void setWorkerKeepAliveForTesting(long millis) {
        ((ThreadPoolExecutor) executor).setKeepAliveTime(millis, TimeUnit.MILLISECONDS);
    }

    public long getEpoch() {
        return epoch;
    }

    public @Nullable String getInstanceId() {
        return instanceId;
    }

    /**
     * Human-readable state of the coordinator, for logs and tests.
     */
    public String getStatus() {
        return status;
    }

    /**
     * Installs the WAL directory policy and, on a primary, the WAL listener. Must run before the
     * engine's {@code completeInit()}.
     */
    public void installHooks() {
        engine.setWalDirectoryPolicy(policy);
        // also tells this service when a checkpoint restore ran: a restored copy is a new instance
        engine.setCheckpointListener(this);
        if (isPrimary) {
            engine.setWalListener(new ReplicationWalListener(this));
        }
    }

    /**
     * The last transaction of the table that the object store holds from this primary, or -1 when it
     * uploaded none.
     */
    public long getUploadedTxn(@NotNull TableToken token) {
        final TableUploader u = uploaders.get(token.getDirName());
        if (u == null) {
            return -1;
        }
        synchronized (u) {
            return u.getCommittedTxn();
        }
    }

    @Override
    public boolean isSeqPartPinned(@NotNull String tableDir, long partId) {
        final TableUploader u = uploaders.get(tableDir);
        if (u != null) {
            return u.isSeqPartPinned(partId);
        }
        // not tracked yet: keep it, unless the table is node-local or gone
        final TableToken token = engine.getTableTokenByDirName(tableDir);
        return token != null && ReplicationWalDirectoryPolicy.isReplicated(token);
    }

    @Override
    public void onCheckpointReleased(long timestampMicros, io.questdb.std.CharSequenceLongHashMap tableDirNamesToSeqTxn) {
        try {
            configuredCheckpointListener.onCheckpointReleased(timestampMicros, tableDirNamesToSeqTxn);
        } finally {
            if (config.isCheckpointHistoryEnabled()) {
                recordCheckpoint(timestampMicros, tableDirNamesToSeqTxn);
            }
        }
    }

    @Override
    public void onCheckpointRestoreComplete() {
        isRestoredAtBoot = true;
        configuredCheckpointListener.onCheckpointRestoreComplete();
    }

    @Override
    public void onCommit(TableToken token, long txn, boolean isNonData) {
        dirtyTables.put(token.getDirName(), Boolean.TRUE);
        if (isNonData && !config.isThrottleNonData()) {
            isNonDataPending = true;
            wake();
        }
    }

    @Override
    public void onSegmentClosed(TableToken token, int walId, int segmentId, long lastTxn) {
        final TableUploader u = uploaderOrKeepClose(token, walId, segmentId, lastTxn);
        if (u != null) {
            synchronized (u) {
                u.onSegmentClosed(walId, segmentId, lastTxn);
            }
        }
        if (!config.isThrottleNonData()) {
            dirtyTables.put(token.getDirName(), Boolean.TRUE);
        }
    }

    @Override
    public void onTableCreated(TableToken token) {
        createdTables.put(token.getDirName(), token);
        if (!config.isThrottleNonData()) {
            isNonDataPending = true;
            wake();
        }
    }

    @Override
    public void onWalClosed(TableToken token, int walId, long lastTxn) {
        final TableUploader u = uploaderOrKeepClose(token, walId, WHOLE_WAL, lastTxn);
        if (u != null) {
            synchronized (u) {
                u.onWalClosed(walId, lastTxn);
            }
        }
    }

    /**
     * Starts the coordinator thread. A primary stays read-only until it caught up and holds the lease.
     */
    public synchronized void start() {
        if (isStarted) {
            return;
        }
        isStarted = true;
        isRunning = true;
        instanceId = loadInstanceId();
        isMigrationAllowed = isPrimary && Files.exists(Paths.get(engine.getConfiguration().getInstallRoot().toString(), MIGRATE_PRIMARY_FILE));
        mainThread = new Thread(this::run, "qdb-replication");
        mainThread.setDaemon(true);
        mainThread.start();
    }

    /**
     * Runs one replica round synchronously, for tests: polls the index and applies everything it names.
     *
     * @return the number of transactions applied
     */
    public long syncReplicaOnce() {
        return replicaRound(true);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // waits for the lease: free, released, ours, or not renewed for 3 keepalive intervals
    private boolean acquireLease() {
        long observedSeq = Long.MIN_VALUE;
        String observedEtag = null;
        long observedSince = System.nanoTime();
        while (isRunning) {
            final ReplicationIndex idx = indexClient.read();
            if (idx == null || idx.isReleased() || idx.getOwnerInstanceId() == null || instanceId.equals(idx.getOwnerInstanceId())) {
                return true;
            }
            final String etag = indexClient.getEtag();
            if (observedEtag == null || !observedEtag.equals(etag) || idx.getSequence() != observedSeq) {
                observedEtag = etag;
                observedSeq = idx.getSequence();
                observedSince = System.nanoTime();
            }
            final long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - observedSince);
            final long leaseMs = 3 * config.getKeepaliveInterval();
            if (waitedMs >= leaseMs) {
                LOG.advisory().$("taking over the replication lease of an unresponsive primary [previousOwner=")
                        .$(idx.getOwnerInstanceId()).$(", epoch=").$(idx.getEpoch()).I$();
                return true;
            }
            status = "waiting for lease, owner=" + idx.getOwnerInstanceId();
            LOG.info().$("waiting for the replication lease [owner=").$(idx.getOwnerInstanceId())
                    .$(", remainingMs=").$(leaseMs - waitedMs).I$();
            awaitWake(Math.min(config.getKeepaliveInterval(), Math.max(100, leaseMs - waitedMs)));
        }
        return false;
    }

    private void adoptDataId(ReplicationIndex idx) {
        final DataID dataID = engine.getDataID();
        if (!dataID.isInitialized()) {
            dataID.initialize(idx.getDataIdLo(), idx.getDataIdHi());
            LOG.info().$("adopted the database id of the object store").$();
            return;
        }
        if (dataID.getHi() == idx.getDataIdHi() && dataID.getLo() == idx.getDataIdLo()) {
            return;
        }
        // an empty node that minted its id on this boot joins the store's database instead
        if (engine.isDataIdMintedAtBoot() && !hasReplicatedTables()) {
            dataID.change(idx.getDataIdLo(), idx.getDataIdHi());
            LOG.info().$("an empty node adopted the database id of the object store").$();
            return;
        }
        throw ReplicationException.divergence("the object store belongs to another database (DataID differs)");
    }

    private boolean hasReplicatedTables() {
        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        engine.getTableTokens(tokens, false);
        for (int i = 0, n = tokens.size(); i < n; i++) {
            if (ReplicationWalDirectoryPolicy.isReplicated(tokens.get(i))) {
                return true;
            }
        }
        return false;
    }

    private void awaitWake(long millis) {
        lock.lock();
        try {
            if (isRunning && !isNonDataPending) {
                wakeUp.await(millis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private ReplicationIndex buildIndex(boolean isReleased) {
        final ReplicationIndex idx = new ReplicationIndex();
        final DataID dataID = engine.getDataID();
        idx.setDataId(dataID.getHi(), dataID.getLo());
        // a released index still names who released it: that node may come back as primary unasked
        idx.setOwner(instanceId, epoch, engine.getConfiguration().getMicrosecondClock().getTicks(), sequence + 1, isReleased);
        for (TableUploader u : uploaders.values()) {
            // a replica asks for the create manifest of every table the index lists and gives the table
            // up when it stays missing, so a table created under replication waits for its manifest
            if (!u.isCreateUploaded() && u.getEntry().getFirstTxn() <= 1) {
                continue;
            }
            final IndexTable t = new IndexTable();
            t.copyFrom(u.getEntry());
            idx.addTable(t);
        }
        for (IndexTable retained : retainedEntries.values()) {
            final IndexTable t = new IndexTable();
            t.copyFrom(retained);
            idx.addTable(t);
        }
        return idx;
    }

    private boolean catchUp(ReplicationIndex idx) {
        if (idx == null) {
            return true;
        }
        final ObjList<IndexTable> tables = idx.getTables();
        boolean isClean = true;
        for (int i = 0, n = tables.size(); i < n && isRunning; i++) {
            final IndexTable t = tables.getQuick(i);
            final TableToken token = engine.getTableTokenByDirName(t.getDirName());
            final TableReplayer r = replayers.computeIfAbsent(t.getKey(), TableReplayer::new);
            if (token != null) {
                final long local = engine.getTableSequencerAPI().lastTxn(token);
                if (local > t.getLastTxn()) {
                    // the local tail goes up next, provided local history agrees with the store's
                    try {
                        r.checkCommonPrefix(engine, t, worker(), local);
                    } catch (ReplicationException e) {
                        LOG.critical().$("table diverged from the object store [table=").$(token).$(", error=").$safe(e.getMessage()).I$();
                        isClean = false;
                    }
                    continue;
                }
            }
            long applied;
            do {
                applied = r.replay(engine, t, worker(), null, CATCHUP_BATCH);
            } while (applied > 0 && isRunning);
            if (r.isStale()) {
                isClean = false;
            }
        }
        return isClean;
    }

    /**
     * Replaces the index: the commit point of everything uploaded since the last one, and the renewal
     * of the lease.
     *
     * @return false when nothing was committed: a transient failure the caller retries, or a lost
     * lease, in which case a writable primary is fenced and a node still taking the lease keeps trying
     */
    private boolean commitIndex(boolean isReleased) {
        final ReplicationIndex idx = buildIndex(isReleased);
        // the lease counts from before the request: the store accepted the write no earlier than this
        final long writeStartNanos = System.nanoTime();
        try {
            indexClient.write(idx, config.getCompressionLevel());
        } catch (ObjectStoreException e) {
            // S3 answers a conditional replace of an object that is gone with "not found"
            final boolean isConflict = e.getKind() == ObjectStoreException.Kind.PRECONDITION_FAILED
                    || e.getKind() == ObjectStoreException.Kind.NOT_FOUND;
            if (!isConflict && !e.isRetryable()) {
                throw e;
            }
            // Ask the index itself what happened. The write may have landed despite the error; a store
            // client that retries a request whose answer was lost even reports a conflict with its own
            // first attempt.
            final ReplicationIndex current;
            try {
                current = indexClient.read();
            } catch (ObjectStoreException readError) {
                if (!readError.isRetryable()) {
                    throw readError;
                }
                LOG.error().$("could not commit the replication index, will retry [error=").$safe(e.getMessage()).I$();
                return false;
            } catch (RuntimeException readError) {
                // somebody replaced the index with something this node cannot read
                LOG.critical().$("the replication index is unreadable [error=").$safe(String.valueOf(readError.getMessage())).I$();
                onLeaseLost("the replication index in the object store is unreadable: this node lost its lease");
                return false;
            }
            final boolean isOurs = current != null && instanceId.equals(current.getOwnerInstanceId());
            final boolean isOurTerm = current != null && current.getSequence() == idx.getSequence() && current.getEpoch() == idx.getEpoch()
                    && (isOurs || current.isReleased());
            if (isOurTerm && current.getRenewedAt() == idx.getRenewedAt() && current.isReleased() == isReleased) {
                LOG.info().$("replication index commit landed despite an error [sequence=").$(idx.getSequence()).I$();
            } else if (isOurTerm) {
                // an earlier write of this node landed without it noticing, and the store holds that one,
                // not this one: build the next index on top of it
                sequence = current.getSequence();
                LOG.info().$("an earlier replication index commit had landed, committing again [sequence=").$(sequence).I$();
                return false;
            } else if (current == null ? sequence == 0 : current.getSequence() == sequence) {
                // the index this write was based on is still there: the write did not land and nobody
                // else wrote; the read above refreshed the tag the next attempt compares against
                LOG.error().$("could not commit the replication index, will retry [error=").$safe(e.getMessage()).I$();
                return false;
            } else {
                onLeaseLost(current == null
                        ? "the replication index is gone from the object store: this node lost its lease"
                        : "another primary replaced the replication index: this node lost its lease");
                return false;
            }
        }
        sequence = idx.getSequence();
        lastIndexWriteNanos = System.nanoTime();
        if (!isReleased) {
            engine.renewLease(writeStartNanos + TimeUnit.MILLISECONDS.toNanos(leaseMillis()));
        }
        for (TableUploader u : uploaders.values()) {
            synchronized (u) {
                u.onCommitted();
            }
        }
        return true;
    }

    private void fence(String reason) {
        LOG.critical().$("replication fenced this node [reason=").$(reason).I$();
        status = "fenced: " + reason;
        engine.setRole(ReplicatedCairoEngine.EngineRole.FENCED, reason);
        isRunning = false;
    }

    // another node committed an index after the one this node last saw
    private void onLeaseLost(String reason) {
        if (engine.isPrimaryRole()) {
            fence(reason);
        } else {
            // not writable yet, so nothing of this node's is at stake: catch up and wait for the lease again
            LOG.info().$("another node took the replication lease first, waiting for it again").$();
        }
    }

    // makes the uploader visible and hands it the closes that arrived before it existed
    private void registerUploader(String tableDir, TableUploader u) {
        final List<long[]> closes;
        synchronized (earlyCloses) {
            uploaders.put(tableDir, u);
            closes = earlyCloses.remove(tableDir);
        }
        if (closes != null) {
            synchronized (u) {
                for (long[] c : closes) {
                    if (c[1] == WHOLE_WAL) {
                        u.onWalClosed((int) c[0], c[2]);
                    } else {
                        u.onSegmentClosed((int) c[0], (int) c[1], c[2]);
                    }
                }
            }
        }
    }

    // A table created a moment ago has no uploader until the next round picks it up. A segment its
    // writer closes meanwhile must not be forgotten: the uploader releases a segment's marker on that
    // close only, and without it the segment would stay on disk until a restart.
    private TableUploader uploaderOrKeepClose(TableToken token, int walId, int segmentId, long lastTxn) {
        synchronized (earlyCloses) {
            final TableUploader u = uploaders.get(token.getDirName());
            // views and materialized views never get an uploader
            if (u == null && isPrimary && !token.isView() && !token.isMatView()) {
                earlyCloses.computeIfAbsent(token.getDirName(), k -> new ArrayList<>()).add(new long[]{walId, segmentId, lastTxn});
            }
            return u;
        }
    }

    // gives up the calling thread's context, if it has one
    private void releaseWorkerOfThisThread() {
        final ReplicationWorker w = workerSlot.get();
        if (w != null) {
            workerSlot.remove();
            // close() frees whatever is still registered: only one of the two gets each context
            if (allWorkers.remove(w)) {
                Misc.free(w);
            }
        }
    }

    // the calling thread's buffers, codecs and readers, created on first use
    private ReplicationWorker worker() {
        ReplicationWorker w = workerSlot.get();
        if (w == null) {
            final CairoConfiguration cairoConfig = engine.getConfiguration();
            w = new ReplicationWorker(
                    store,
                    config.getCompressionLevel(),
                    isChecksum,
                    cairoConfig.getFilesFacade(),
                    cairoConfig.getMkDirMode(),
                    engine,
                    workDir
            );
            allWorkers.add(w);
            workerSlot.set(w);
        }
        return w;
    }

    private String loadInstanceId() {
        try {
            final java.nio.file.Path dir = Paths.get(workDir);
            Files.createDirectories(dir);
            final java.nio.file.Path file = dir.resolve(INSTANCE_FILE);
            // a node restored from a checkpoint copy may carry its source's id: it is another instance
            if (Files.exists(file) && !isRestoredAtBoot) {
                final String id = Files.readString(file, StandardCharsets.UTF_8).trim();
                if (!id.isEmpty()) {
                    return id;
                }
            }
            final String id = UUID.randomUUID().toString();
            final java.nio.file.Path tmp = dir.resolve(INSTANCE_FILE + ".tmp");
            Files.writeString(tmp, id, StandardCharsets.UTF_8);
            Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            LOG.info().$("new replication instance id [id=").$(id).$(", restoredFromCheckpoint=").$(isRestoredAtBoot).I$();
            return id;
        } catch (java.io.IOException e) {
            throw CairoException.critical(0).put("could not load the replication instance id [dir=").put(workDir)
                    .put(", error=").put(e.getMessage()).put(']');
        }
    }

    // under SYNC and ADAPTIVE, only what is durable locally: a crash could cut the rest and reissue its numbers
    private long durableTarget(TableUploader u, long last) {
        final int commitMode = engine.getConfiguration().getCommitMode();
        if (commitMode == CommitMode.ADAPTIVE || commitMode == CommitMode.SYNC) {
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTrackerIfExists(u.getToken());
            if (tracker != null) {
                final long durable = tracker.getLocalDurableSeqTxn();
                if (durable >= 0 && durable < last && durable >= u.getPendingTxn()) {
                    return durable;
                }
            }
        }
        return last;
    }

    private long localLastTxn(TableUploader u) {
        final TableToken token = u.getToken();
        try {
            return engine.getTableSequencerAPI().lastTxn(token);
        } catch (CairoException e) {
            // a dropped table: read the log itself, which the drop leaves in place until it is uploaded
            final TxnLogReader r = worker().getTxnLogReader();
            try {
                return r.of(engine.getConfiguration().getDbRoot(), token.getDirName()).getMaxTxn();
            } finally {
                r.closeFiles();
            }
        }
    }

    private void prepareUploaders(@Nullable ReplicationIndex idx) {
        final HashMap<String, IndexTable> stored = new HashMap<>();
        if (idx != null) {
            for (int i = 0, n = idx.getTables().size(); i < n; i++) {
                final IndexTable t = idx.getTables().getQuick(i);
                stored.put(t.getDirName(), t);
            }
        }
        final ObjHashSet<TableToken> tokens = new ObjHashSet<>();
        engine.getTableTokens(tokens, false);
        for (int i = 0, n = tokens.size(); i < n; i++) {
            final TableToken token = tokens.get(i);
            if (token.isView() || token.isMatView()) {
                LOG.advisory().$("views and materialized views are not replicated in this version [name=").$(token).I$();
                continue;
            }
            if (!ReplicationWalDirectoryPolicy.isReplicated(token)) {
                continue;
            }
            final IndexTable t = stored.remove(token.getDirName());
            final long local = engine.getTableSequencerAPI().lastTxn(token);
            final TableUploader u;
            if (t != null && !t.isDropped()) {
                u = new TableUploader(token, t, true);
            } else {
                u = newUploader(token, local);
            }
            u.setStartupLastTxn(local, path, engine.getConfiguration().getDbRoot(), engine.getConfiguration().getFilesFacade());
            registerUploader(token.getDirName(), u);
            dirtyTables.put(token.getDirName(), Boolean.TRUE);
        }
        // tables dropped or gone meanwhile stay listed until the cleaner removes them
        retainedEntries.clear();
        for (IndexTable t : stored.values()) {
            retainedEntries.put(t.getDirName(), t);
        }
    }

    private TableUploader newUploader(TableToken token, long localLastTxn) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(WalUtils.SEQ_DIR).slash()
                .put(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER);
        // a sequencer created under replication carries the marker, and all of its WAL is still here
        final boolean isCreatedUnderReplication = ff.exists(path.$());
        final IndexTable t = new IndexTable().of(
                token.getTableId(),
                readCreateTimestamp(token),
                token.getDirName(),
                token.getTableName(),
                IndexTable.KIND_TABLE,
                config.getSequencerPartTxnCount()
        );
        t.setFirstTxn(isCreatedUnderReplication ? 1 : localLastTxn + 1);
        t.setLastTxn(t.getFirstTxn() - 1);
        if (!isCreatedUnderReplication) {
            LOG.advisory().$("table existed before replication; replicas need a checkpoint copy of it [table=").$(token)
                    .$(", firstReplicatedTxn=").$(t.getFirstTxn()).I$();
            // keep its sequencer until its drop, if any, is uploaded
            policy.initDirectory(path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(WalUtils.SEQ_DIR).slash(), token);
        }
        return new TableUploader(token, t, false);
    }

    /**
     * One primary round: uploads the dirty tables when {@code isUpload}, then commits the index if
     * there is anything to commit, the lease needs renewing, or this is the final round.
     */
    /**
     * Uploads what the dirty tables hold, one batch per table, and commits it.
     *
     * @return the number of transactions uploaded
     */
    private long primaryRound(boolean isUpload) {
        if (!isUpload) {
            commitIndex(false);
            return 0;
        }
        // pick up tables created since the last round
        for (TableToken token : createdTables.values()) {
            createdTables.remove(token.getDirName());
            if (!uploaders.containsKey(token.getDirName()) && !token.isView() && !token.isMatView()) {
                registerUploader(token.getDirName(), newUploader(token, 0));
                dirtyTables.put(token.getDirName(), Boolean.TRUE);
            }
        }
        final List<Future<Long>> futures = new ArrayList<>();
        for (String dir : dirtyTables.keySet()) {
            dirtyTables.remove(dir);
            final TableUploader u = uploaders.get(dir);
            if (u == null || u.isDropCommitted()) {
                continue;
            }
            futures.add(executor.submit(() -> {
                synchronized (u) {
                    try {
                        final TableToken current = engine.getTableTokenByDirName(dir);
                        if (current != null) {
                            u.setToken(current);
                        }
                        final long last = localLastTxn(u);
                        final long n = u.upload(engine, worker(), epoch, durableTarget(u, last), UPLOAD_BATCH);
                        if (u.getPendingTxn() < last) {
                            // more to upload: a batch limit or a durable frontier still behind the log
                            dirtyTables.put(dir, Boolean.TRUE);
                        }
                        return n;
                    } catch (Throwable th) {
                        // retried on the next round
                        dirtyTables.put(dir, Boolean.TRUE);
                        LOG.error().$("table upload failed, will retry [table=").$(u.getToken()).$(", error=").$(th).I$();
                        return 0L;
                    }
                }
            }));
        }
        long uploaded = 0;
        boolean hasPending = false;
        for (Future<Long> f : futures) {
            try {
                uploaded += f.get();
            } catch (Exception e) {
                final Throwable cause = e.getCause() != null ? e.getCause() : e;
                LOG.error().$("table upload failed, will retry [error=").$safe(String.valueOf(cause.getMessage())).I$();
            }
        }
        for (TableUploader u : uploaders.values()) {
            hasPending |= u.hasPending();
        }
        final boolean isKeepaliveDue = System.nanoTime() - lastIndexWriteNanos >= TimeUnit.MILLISECONDS.toNanos(config.getKeepaliveInterval());
        if (hasPending || isKeepaliveDue) {
            if (commitIndex(false) && uploaded > 0) {
                status = "primary, epoch " + epoch;
                for (TableUploader u : uploaders.values()) {
                    synchronized (u) {
                        u.releaseMarkers(policy, path, engine.getConfiguration().getDbRoot());
                    }
                }
            }
        }
        return uploaded;
    }

    private void primaryMain() {
        // catch up, take the lease, open writes
        ReplicationIndex idx;
        while (true) {
            if (!isRunning) {
                return;
            }
            status = "primary catching up";
            try {
                idx = indexClient.read();
                if (idx != null) {
                    checkMayBecomePrimary(idx);
                    if (!engine.getDataID().isInitialized()) {
                        throw ReplicationException.divergence("this primary has no database id");
                    }
                    adoptDataId(idx);
                }
                if (!catchUp(idx)) {
                    fence("tables could not catch up with the object store; see the log");
                    return;
                }
                if (!acquireLease()) {
                    return;
                }
                // the previous owner may have committed more while this node waited: catch up again; a
                // commit that still slips in makes the index write below fail and the loop start over
                idx = indexClient.read();
                if (!catchUp(idx)) {
                    fence("tables could not catch up with the object store; see the log");
                    return;
                }
                epoch = (idx != null ? idx.getEpoch() : 0) + 1;
                sequence = idx != null ? idx.getSequence() : 0;
                uploaders.clear();
                prepareUploaders(idx);
                for (TableUploader u : uploaders.values()) {
                    if (!u.isDropCommitted()) {
                        u.loadPart(worker());
                    }
                }
                if (commitIndex(false)) {
                    break;
                }
            } catch (ObjectStoreException e) {
                // the store is unreachable or refuses the node: nothing is decided, so the node stays
                // read-only and keeps trying instead of fencing itself until a restart
                status = "primary catching up, object store error: " + e.getMessage();
                LOG.error().$("replication cannot reach the object store while starting, will retry [error=").$safe(e.getMessage()).I$();
                sleepQuietly(config.getRetryInterval());
            }
            if (!isRunning) {
                return;
            }
        }
        engine.setRole(ReplicatedCairoEngine.EngineRole.PRIMARY, null);
        status = "primary, epoch " + epoch;
        LOG.advisory().$("replication primary is writable [epoch=").$(epoch).$(", instance=").$(instanceId)
                .$(", store=").$(store.getDescription()).I$();
        consumeMigrationMarker();
        long firstDirtyNanos = 0;
        boolean wasLeaseLapsed = false;
        while (isRunning) {
            final long window = config.getThrottleWindowDuration();
            if (engine.isLeaseLapsed()) {
                // The node refuses writes by now. Another node may have taken the store over, so nothing
                // goes up and nothing is deleted until the store accepts this node's next index, which it
                // does only if nobody replaced the one this node wrote last. If somebody did, the node fences.
                if (!wasLeaseLapsed) {
                    wasLeaseLapsed = true;
                    status = "primary, lease lapsed: read-only until the object store accepts a renewal";
                    LOG.critical().$("replication lease lapsed, the primary is read-only until it renews it [epoch=").$(epoch).I$();
                }
                if (!renewLapsedLease()) {
                    if (isRunning) {
                        sleepQuietly(config.getRetryInterval());
                    }
                    continue;
                }
            }
            if (wasLeaseLapsed) {
                wasLeaseLapsed = false;
                status = "primary, epoch " + epoch;
                LOG.advisory().$("replication lease renewed, the primary accepts writes again [epoch=").$(epoch).I$();
            }
            if (!dirtyTables.isEmpty() && firstDirtyNanos == 0) {
                firstDirtyNanos = System.nanoTime();
            }
            final boolean isWindowDue = firstDirtyNanos != 0 && System.nanoTime() - firstDirtyNanos >= TimeUnit.MILLISECONDS.toNanos(window);
            final boolean isKeepaliveDue = System.nanoTime() - lastIndexWriteNanos >= TimeUnit.MILLISECONDS.toNanos(config.getKeepaliveInterval());
            final boolean isUploadDue = isNonDataPending || isWindowDue || !createdTables.isEmpty() && !config.isThrottleNonData();
            if (isUploadDue || isKeepaliveDue) {
                if (isUploadDue) {
                    isNonDataPending = false;
                    firstDirtyNanos = 0;
                }
                try {
                    // a keepalive alone renews the lease and leaves data to its window
                    primaryRound(isUploadDue);
                } catch (ObjectStoreException e) {
                    LOG.error().$("replication upload round failed, will retry [error=").$safe(e.getMessage()).I$();
                    sleepQuietly(config.getRetryInterval());
                } catch (Throwable th) {
                    // an unforeseen failure must not end the coordinator: the node would stay writable
                    // with nothing uploading and the lease running out
                    LOG.critical().$("replication upload round failed unexpectedly, will retry [error=").$(th).I$();
                    sleepQuietly(config.getRetryInterval());
                }
                continue;
            }
            runHousekeeping(true);
            awaitWake(Math.max(1, Math.min(window, config.getKeepaliveInterval()) / 4));
        }
        // drain what is committed, then hand the lease over
        if (engine.isPrimaryRole() && !isCrashed) {
            try {
                drainAndRelease();
            } catch (Throwable th) {
                LOG.error().$("could not drain replication uploads at shutdown [error=").$safe(String.valueOf(th.getMessage())).I$();
            }
        }
    }

    /**
     * Uploads every committed transaction, batch after batch, and only then releases the lease. A
     * released lease tells the next primary that the store is complete and that it need not wait; when
     * the drain falls short (a store failure, the deadline), the lease stays held and expires on its
     * own, as after a crash.
     */
    private void drainAndRelease() {
        if (engine.isLeaseLapsed() && !renewLapsedLease()) {
            LOG.error().$("replication stops with a lapsed lease it could not renew; nothing more is uploaded").$();
            return;
        }
        for (TableUploader u : uploaders.values()) {
            dirtyTables.put(u.getToken().getDirName(), Boolean.TRUE);
        }
        // close() frees what the drain uses once it has waited shutdownTimeoutMillis() for the
        // coordinator: no round starts in the second half of that wait, which is left to the round in
        // progress and to the release
        final long closeStarted = closeStartedNanos;
        final long deadline = (closeStarted != 0 ? closeStarted : System.nanoTime()) + TimeUnit.MILLISECONDS.toNanos(shutdownTimeoutMillis() / 2);
        while (System.nanoTime() < deadline) {
            final long uploaded = primaryRound(true);
            if (uploaded == 0 || dirtyTables.isEmpty() || !engine.isPrimaryRole()) {
                break;
            }
        }
        if (!engine.isPrimaryRole()) {
            return;
        }
        if (dirtyTables.isEmpty() && createdTables.isEmpty()) {
            if (commitIndex(true)) {
                LOG.info().$("replication drained and released the lease [epoch=").$(epoch).I$();
            } else {
                LOG.error().$("replication uploaded everything but could not release the lease; it stays held until it expires").$();
            }
            return;
        }
        LOG.error().$("replication stops with transactions it could not upload; the lease stays held until it expires [tables=")
                .$(dirtyTables.size() + createdTables.size()).I$();
    }

    /**
     * How long a primary may act on one accepted index write. Another node takes the store over once
     * the index has not changed for three keepalive intervals, counted from when that node first saw
     * it, which is never before the write. This primary stops half an interval earlier: the margin
     * covers a write that passed the lease check just before the bound.
     */
    private long leaseMillis() {
        return config.getKeepaliveInterval() * 5 / 2;
    }

    private long shutdownTimeoutMillis() {
        return Math.max(5_000, 3 * config.getRequestBaseTimeout());
    }

    /**
     * A node configured as primary takes a store over by itself only when it is the store's own
     * primary coming back: the index names it as the owner and holds nothing this node lacks. Any other
     * node needs an empty {@value #MIGRATE_PRIMARY_FILE} file in its install root, as QuestDB Enterprise
     * requires for a restart-based promotion. A replica whose role was set to primary by mistake, or a
     * restored copy started with the wrong role, therefore changes nothing in the store.
     */
    private void checkMayBecomePrimary(ReplicationIndex idx) {
        if (isMigrationAllowed) {
            return;
        }
        final String why;
        if (!instanceId.equals(idx.getOwnerInstanceId())) {
            why = "the object store belongs to another primary [owner=" + idx.getOwnerInstanceId() + ']';
        } else {
            final String behind = firstTableBehind(idx);
            if (behind == null) {
                return;
            }
            why = "this node is behind the object store [table=" + behind + ']';
        }
        throw ReplicationException.divergence(why + "; to make this node the primary, create an empty " + MIGRATE_PRIMARY_FILE
                + " file in " + engine.getConfiguration().getInstallRoot() + " and restart it");
    }

    // the marker allows one promotion
    private void consumeMigrationMarker() {
        if (isMigrationAllowed) {
            isMigrationAllowed = false;
            try {
                Files.deleteIfExists(Paths.get(engine.getConfiguration().getInstallRoot().toString(), MIGRATE_PRIMARY_FILE));
                LOG.advisory().$("replication promoted this node and removed ").$(MIGRATE_PRIMARY_FILE).$();
            } catch (java.io.IOException e) {
                LOG.error().$("could not remove ").$(MIGRATE_PRIMARY_FILE).$(" [error=").$safe(e.getMessage()).I$();
            }
        }
    }

    // the name of a table the store holds more of than this node, or null
    private String firstTableBehind(ReplicationIndex idx) {
        final ObjList<IndexTable> tables = idx.getTables();
        for (int i = 0, n = tables.size(); i < n; i++) {
            final IndexTable t = tables.getQuick(i);
            final TableToken token = engine.getTableTokenByDirName(t.getDirName());
            if (token == null) {
                if (!t.isDropped()) {
                    return t.getTableName();
                }
            } else if (t.isDropped() || engine.getTableSequencerAPI().lastTxn(token) < t.getLastTxn()) {
                return t.getTableName();
            }
        }
        return null;
    }

    // one index write with nothing new in it; true when the store accepted it
    private boolean renewLapsedLease() {
        try {
            return commitIndex(false) && !engine.isLeaseLapsed();
        } catch (ObjectStoreException e) {
            LOG.error().$("could not renew the replication lease, will retry [error=").$safe(e.getMessage()).I$();
            return false;
        } catch (Throwable th) {
            LOG.critical().$("could not renew the replication lease, will retry [error=").$(th).I$();
            return false;
        }
    }

    private long readCreateTimestamp(TableToken token) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        path.of(engine.getConfiguration().getDbRoot()).concat(token.getDirName()).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_FILE_NAME);
        final long fd = ff.openRO(path.$());
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not open sequencer log [path=").put(path).put(']');
        }
        try {
            final long v = ff.readNonNegativeLong(fd, 12);
            return v < 0 ? 0 : v;
        } finally {
            ff.close(fd);
        }
    }

    private long replicaRound(boolean isForced) {
        final ReplicationIndex idx = isForced ? indexClient.read() : indexClient.readIfChanged();
        if (idx == null && !isForced) {
            return hasReplicaWork() ? replicaApply(null) : 0;
        }
        if (idx == null) {
            return 0;
        }
        adoptDataId(idx);
        lastIndex = idx;
        return replicaApply(idx);
    }

    private boolean hasReplicaWork() {
        return lastIndex != null;
    }

    private long replicaApply(@Nullable ReplicationIndex fresh) {
        final ReplicationIndex idx = fresh != null ? fresh : lastIndex;
        long total = 0;
        // drops and existing tables first, so that a re-created name is free before its new table arrives
        for (int phase = 0; phase < 2; phase++) {
            final List<Future<Long>> futures = new ArrayList<>();
            final ObjList<IndexTable> tables = idx.getTables();
            for (int i = 0, n = tables.size(); i < n; i++) {
                final IndexTable t = tables.getQuick(i);
                final boolean isKnown = engine.getTableTokenByDirName(t.getDirName()) != null;
                if ((phase == 0) != (isKnown || t.isDropped())) {
                    continue;
                }
                final TableReplayer r = replayers.computeIfAbsent(t.getKey(), TableReplayer::new);
                if (r.isDone() || r.isStale()) {
                    continue;
                }
                futures.add(executor.submit(() -> r.replay(engine, t, worker(), pins, CATCHUP_BATCH)));
            }
            for (Future<Long> f : futures) {
                try {
                    total += f.get();
                } catch (Exception e) {
                    final Throwable cause = e.getCause() != null ? e.getCause() : e;
                    LOG.error().$("table replication failed, will retry [error=").$safe(String.valueOf(cause.getMessage())).I$();
                }
            }
        }
        return total;
    }

    private void replicaMain() {
        status = "replica";
        while (isRunning) {
            long applied = 0;
            try {
                applied = replicaRound(false);
            } catch (ObjectStoreException e) {
                LOG.error().$("replica poll failed, will retry [error=").$safe(e.getMessage()).I$();
            } catch (ReplicationException e) {
                LOG.critical().$("replication stopped [error=").$safe(e.getMessage()).I$();
                status = "stopped: " + e.getMessage();
                return;
            } catch (Throwable th) {
                LOG.critical().$("replica round failed unexpectedly, will retry [error=").$(th).I$();
            }
            runHousekeeping(false);
            if (applied == 0) {
                awaitWake(config.getReplicaPollInterval());
            }
        }
    }

    // checkpoint history uploads on every node, the WAL cleaner on the primary
    private void runHousekeeping(boolean isPrimaryNode) {
        final long now = System.nanoTime();
        if (!pendingCheckpoints.isEmpty() && now >= nextCheckpointUploadNanos) {
            try {
                uploadCheckpointHistory();
            } catch (Throwable th) {
                LOG.error().$("could not record the checkpoint history, will retry [error=").$(th).I$();
                nextCheckpointUploadNanos = now + TimeUnit.MILLISECONDS.toNanos(config.getCheckpointHistoryLongRetryInterval());
            }
        }
        if (isPrimaryNode && config.isCleanerEnabled()) {
            if (nextCleanerRunNanos == 0) {
                nextCleanerRunNanos = now + TimeUnit.MILLISECONDS.toNanos(config.getCleanerInterval());
            } else if (now >= nextCleanerRunNanos) {
                try {
                    runCleaner();
                } catch (Throwable th) {
                    LOG.error().$("WAL cleaner run failed, will retry [error=").$(th).I$();
                }
                nextCleanerRunNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getCleanerInterval());
            }
        }
    }

    private void runCleaner() {
        if (isCleanerCommitPending) {
            // an earlier run changed the entries in memory and could not commit them
            if (!commitIndex(false)) {
                return;
            }
            isCleanerCommitPending = false;
        }
        final ReplicationWorker w = worker();
        final Map<String, Long> cutoffs = config.isCleanerCheckpointSource() ? cleaner.computeCutoffs(w) : null;
        // what earlier runs could not delete goes first: no index names those objects any more, so no
        // later plan would find them
        final List<String> keys = new ArrayList<>(pendingDeletes);
        pendingDeletes.clear();
        boolean isIndexChanged = false;
        try {
            if (cutoffs != null) {
                for (TableUploader u : uploaders.values()) {
                    synchronized (u) {
                        if (u.isDropCommitted() || u.hasPending()) {
                            continue;
                        }
                        final Long cutoff = cutoffs.get(u.getEntry().getDirName());
                        if (cutoff == null || cutoff <= 0) {
                            continue;
                        }
                        final WalCleaner.TablePlan plan = cleaner.planTable(u.getEntry(), cutoff, w);
                        if (plan != null) {
                            u.getEntry().setFirstTxn(plan.getNewFirstTxn());
                            keys.addAll(plan.getKeys());
                            isIndexChanged = true;
                            LOG.info().$("WAL cleaner frees table history [table=").$(u.getToken()).$(", newFirstTxn=")
                                    .$(plan.getNewFirstTxn()).$(", objects=").$(plan.getKeys().size()).I$();
                        }
                    }
                }
            }
            final long nowMicros = engine.getConfiguration().getMicrosecondClock().getTicks();
            final long cooloffMicros = config.getCleanerDroppedTableCooloff() * 1000;
            for (Map.Entry<String, IndexTable> e : retainedEntries.entrySet()) {
                final IndexTable t = e.getValue();
                if (t.isDropped() && nowMicros - t.getDroppedAt() >= cooloffMicros) {
                    keys.addAll(cleaner.planDroppedTable(t));
                    retainedEntries.remove(e.getKey());
                    isIndexChanged = true;
                }
            }
            for (Map.Entry<String, TableUploader> e : uploaders.entrySet()) {
                final TableUploader u = e.getValue();
                if (u.isDropCommitted() && nowMicros - u.getEntry().getDroppedAt() >= cooloffMicros) {
                    keys.addAll(cleaner.planDroppedTable(u.getEntry()));
                    uploaders.remove(e.getKey());
                    isIndexChanged = true;
                }
            }
            // the index stops naming the objects before they go
            if (isIndexChanged && !commitIndex(false)) {
                pendingDeletes.addAll(keys);
                isCleanerCommitPending = true;
                return;
            }
        } catch (Throwable th) {
            // a failed listing or commit must not lose what was planned: the entries changed in memory
            // reach the index with the next commit, after which no plan finds these objects again
            pendingDeletes.addAll(keys);
            isCleanerCommitPending |= isIndexChanged;
            throw th;
        }
        deleteObjects(keys);
    }

    private void deleteObjects(List<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        int rps = config.getCleanerMaxRequestsPerSecond();
        if (rps < 0) {
            rps = store.getDefaultCleanerMaxRequestsPerSecond();
        }
        final int concurrency = Math.max(1, config.getCleanerDeleteConcurrency());
        final long startNanos = System.nanoTime();
        int deleted = 0;
        int next = 0;
        for (int n = keys.size(); next < n && isRunning; next += concurrency) {
            final int i = next;
            final List<Future<?>> futures = new ArrayList<>();
            for (int j = i; j < Math.min(n, i + concurrency); j++) {
                final String key = keys.get(j);
                futures.add(executor.submit(() -> store.delete(key)));
            }
            for (int j = 0, m = futures.size(); j < m; j++) {
                try {
                    futures.get(j).get();
                    deleted++;
                } catch (Exception e) {
                    pendingDeletes.add(keys.get(i + j));
                    LOG.error().$("could not delete an object, the next run retries [key=").$safe(keys.get(i + j))
                            .$(", error=").$(e.getCause() != null ? e.getCause() : e).I$();
                }
            }
            if (rps > 0) {
                final long dueNanos = startNanos + TimeUnit.SECONDS.toNanos(Math.min(keys.size(), i + concurrency)) / rps;
                final long sleepNanos = dueNanos - System.nanoTime();
                if (sleepNanos > 0) {
                    sleepQuietly(TimeUnit.NANOSECONDS.toMillis(sleepNanos) + 1);
                }
            }
        }
        // a stop cut the run short: the rest waits for the next one
        for (int n = keys.size(); next < n; next++) {
            pendingDeletes.add(keys.get(next));
        }
        LOG.info().$("WAL cleaner deleted objects [count=").$(deleted).$(", requested=").$(keys.size())
                .$(", left=").$(pendingDeletes.size()).I$();
    }

    private void uploadCheckpointHistory() {
        final ReplicationWorker w = worker();
        final String key = StoreKeys.checkpointHistory(instanceId);
        if (checkpointHistory == null) {
            if (w.getIo().get(key, w.getBuffer())) {
                final byte[] text = new byte[(int) w.getBuffer().size()];
                io.questdb.std.Unsafe.copyMemory(null, w.getBuffer().ptr(), text, io.questdb.std.Unsafe.BYTE_OFFSET, text.length);
                checkpointHistory = io.questdb.replication.codec.CheckpointHistory.decode(text, 0, text.length);
            } else {
                checkpointHistory = new io.questdb.replication.codec.CheckpointHistory();
            }
        }
        final int queued = pendingCheckpoints.size();
        for (int i = 0; i < queued; i++) {
            final io.questdb.replication.codec.CheckpointHistory.Checkpoint checkpoint = pendingCheckpoints.peek();
            final List<io.questdb.replication.codec.CheckpointHistory.Checkpoint> recorded = checkpointHistory.getCheckpoints();
            // a put that failed leaves the checkpoint in the history: the retry must not record it twice,
            // or the copies would pass for the distinct checkpoints the cleaner waits for
            if (recorded.isEmpty() || recorded.get(recorded.size() - 1) != checkpoint) {
                checkpointHistory.add(checkpoint);
                checkpointHistory.keepLatest(config.getCheckpointHistoryKeepCount());
            }
            final byte[] text = checkpointHistory.encode();
            w.getBuffer().clear();
            w.getBuffer().putBytes(text, 0, text.length);
            w.getIo().put(key, w.getBuffer(), false);
            pendingCheckpoints.poll();
            LOG.info().$("recorded a checkpoint in the replication history [instance=").$(instanceId)
                    .$(", checkpoints=").$(checkpointHistory.getCheckpoints().size()).I$();
        }
    }

    private void recordCheckpoint(long timestampMicros, io.questdb.std.CharSequenceLongHashMap tableDirNamesToSeqTxn) {
        final io.questdb.replication.codec.CheckpointHistory.Checkpoint c = new io.questdb.replication.codec.CheckpointHistory.Checkpoint(timestampMicros);
        final ObjList<CharSequence> keys = tableDirNamesToSeqTxn.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence dir = keys.getQuick(i);
            final long seqTxn = tableDirNamesToSeqTxn.get(dir);
            if (seqTxn != io.questdb.std.Numbers.LONG_NULL && seqTxn >= 0) {
                c.addTable(dir.toString(), seqTxn);
            }
        }
        pendingCheckpoints.add(c);
        nextCheckpointUploadNanos = 0;
        wake();
    }

    private void run() {
        try {
            if (isPrimary) {
                primaryMain();
            } else {
                replicaMain();
            }
        } catch (Throwable th) {
            LOG.critical().$("replication coordinator failed [error=").$(th).I$();
            status = "failed: " + th.getMessage();
            if (isPrimary) {
                // without its coordinator a primary uploads nothing and renews nothing: it must not go on
                // acknowledging writes
                fence(th instanceof ReplicationException ? th.getMessage() : "replication coordinator failed: " + th.getMessage());
            }
        } finally {
            releaseWorkerOfThisThread();
            Path.clearThreadLocals();
        }
    }

    private void wake() {
        lock.lock();
        try {
            wakeUp.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
