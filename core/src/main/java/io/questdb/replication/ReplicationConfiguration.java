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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Replication settings. Key names, defaults and meaning follow the replication section of QuestDB
 * Enterprise's {@code server.conf}; see {@code ReplicationConfigurationImpl} for the defaults.
 * Durations are in milliseconds.
 */
public interface ReplicationConfiguration {

    /**
     * {@code checkpoint.history.keep.count}: checkpoint records kept per instance.
     */
    int getCheckpointHistoryKeepCount();

    /**
     * {@code checkpoint.history.long.retry.interval}: retry interval for syncing the checkpoint
     * history to the object store after the short retries fail.
     */
    long getCheckpointHistoryLongRetryInterval();

    @NotNull
    ChecksumMode getChecksumMode();

    /**
     * {@code replication.primary.cleaner.backup.window.count}: complete checkpoints an instance
     * keeps before the WAL cleaner deletes the object store data none of them need.
     */
    int getCleanerBackupWindowCount();

    /**
     * {@code replication.primary.cleaner.delete.concurrency}: concurrent delete requests.
     */
    int getCleanerDeleteConcurrency();

    /**
     * {@code replication.primary.cleaner.dropped.table.cooloff}: wait after a DROP TABLE before the
     * cleaner removes the table's data from the object store.
     */
    long getCleanerDroppedTableCooloff();

    long getCleanerInterval();

    /**
     * {@code replication.primary.cleaner.max.requests.per.second}: delete request rate limit, 0 for
     * none, -1 for the object store's default.
     */
    int getCleanerMaxRequestsPerSecond();

    long getCleanerProgressWriteInterval();

    int getCleanerRetryAttempts();

    long getCleanerRetryInterval();

    int getCompressionLevel();

    int getCompressionThreads();

    /**
     * {@code replication.primary.index.upload.throttle.interval}: minimum gap between two uploads of
     * the index, -1 for the object store's default (no gap for S3 and the filesystem).
     */
    long getIndexUploadThrottleInterval();

    long getKeepaliveInterval();

    int getMaxBatchSizeFast();

    int getMaxBatchSizeSlow();

    int getMaxConcurrentRequests();

    int getMetricsDroppedTablePollCount();

    /**
     * {@code replication.requests.min.throughput}: bytes per second added to the base timeout of a
     * request for each byte of its body.
     */
    long getMinThroughput();

    /**
     * {@code replication.object.store}: the object store connection string, null when unset.
     */
    @Nullable
    String getObjectStore();

    long getReplicaPollInterval();

    long getRequestBaseTimeout();

    int getRequestBufferSize();

    int getRetryAttempts();

    long getRetryInterval();

    @NotNull
    ReplicationRole getRole();

    /**
     * {@code replication.primary.sequencer.part.txn.count}: transactions per uploaded transaction
     * log part, for tables created after the setting changes.
     */
    int getSequencerPartTxnCount();

    /**
     * {@code replication.summary.interval}: how often each table's replication progress is logged,
     * 0 to disable.
     */
    long getSummaryInterval();

    /**
     * {@code replication.primary.throttle.window.duration}: how long the primary groups transactions
     * into one upload.
     */
    long getThrottleWindowDuration();

    boolean isCheckpointHistoryEnabled();

    boolean isCleanerCheckpointSource();

    boolean isCleanerEnabled();

    boolean isMetricsPerTable();

    /**
     * {@code replication.primary.throttle.non.data}: when false, non-data transactions (create,
     * rename, drop, closed segments) upload at once instead of waiting for the throttle window.
     */
    boolean isThrottleNonData();
}
