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
 * Mutable {@link ReplicationConfiguration} holding QuestDB Enterprise's documented defaults.
 * {@code PropServerConfiguration} fills it from {@code server.conf}; tests set values directly.
 * Each setter returns {@code this} for chaining.
 */
public class ReplicationConfigurationImpl implements ReplicationConfiguration {
    private boolean checkpointHistoryEnabled = true;
    private int checkpointHistoryKeepCount = 100;
    private long checkpointHistoryLongRetryInterval = 60_000;
    private ChecksumMode checksumMode = ChecksumMode.SERVICE_DEPENDENT;
    private int cleanerBackupWindowCount = 5;
    private boolean cleanerCheckpointSource = true;
    private int cleanerDeleteConcurrency = 4;
    private long cleanerDroppedTableCooloff = 3_600_000;
    private boolean cleanerEnabled = true;
    private long cleanerInterval = 600_000;
    private int cleanerMaxRequestsPerSecond = -1;
    private long cleanerProgressWriteInterval = 5_000;
    private int cleanerRetryAttempts = 20;
    private long cleanerRetryInterval = 2_000;
    private int compressionLevel = 1;
    private int compressionThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    private long indexUploadThrottleInterval = -1;
    private long keepaliveInterval = 10_000;
    private int maxBatchSizeFast = 64;
    private int maxBatchSizeSlow = 2;
    private int maxConcurrentRequests = 128;
    private int metricsDroppedTablePollCount = 10;
    private boolean metricsPerTable = true;
    private long minThroughput = 262_144;
    private String objectStore = null;
    private long replicaPollInterval = 1_000;
    private long requestBaseTimeout = 10_000;
    private int requestBufferSize = 32_768;
    private int retryAttempts = 3;
    private long retryInterval = 200;
    private ReplicationRole role = ReplicationRole.NONE;
    private int sequencerPartTxnCount = 5_000;
    private long summaryInterval = 60_000;
    private boolean throttleNonData = false;
    private long throttleWindowDuration = 10_000;

    @Override
    public int getCheckpointHistoryKeepCount() {
        return checkpointHistoryKeepCount;
    }

    @Override
    public long getCheckpointHistoryLongRetryInterval() {
        return checkpointHistoryLongRetryInterval;
    }

    @Override
    public @NotNull ChecksumMode getChecksumMode() {
        return checksumMode;
    }

    @Override
    public int getCleanerBackupWindowCount() {
        return cleanerBackupWindowCount;
    }

    @Override
    public int getCleanerDeleteConcurrency() {
        return cleanerDeleteConcurrency;
    }

    @Override
    public long getCleanerDroppedTableCooloff() {
        return cleanerDroppedTableCooloff;
    }

    @Override
    public long getCleanerInterval() {
        return cleanerInterval;
    }

    @Override
    public int getCleanerMaxRequestsPerSecond() {
        return cleanerMaxRequestsPerSecond;
    }

    @Override
    public long getCleanerProgressWriteInterval() {
        return cleanerProgressWriteInterval;
    }

    @Override
    public int getCleanerRetryAttempts() {
        return cleanerRetryAttempts;
    }

    @Override
    public long getCleanerRetryInterval() {
        return cleanerRetryInterval;
    }

    @Override
    public int getCompressionLevel() {
        return compressionLevel;
    }

    @Override
    public int getCompressionThreads() {
        return compressionThreads;
    }

    @Override
    public long getIndexUploadThrottleInterval() {
        return indexUploadThrottleInterval;
    }

    @Override
    public long getKeepaliveInterval() {
        return keepaliveInterval;
    }

    @Override
    public int getMaxBatchSizeFast() {
        return maxBatchSizeFast;
    }

    @Override
    public int getMaxBatchSizeSlow() {
        return maxBatchSizeSlow;
    }

    @Override
    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    @Override
    public int getMetricsDroppedTablePollCount() {
        return metricsDroppedTablePollCount;
    }

    @Override
    public long getMinThroughput() {
        return minThroughput;
    }

    @Override
    public @Nullable String getObjectStore() {
        return objectStore;
    }

    @Override
    public long getReplicaPollInterval() {
        return replicaPollInterval;
    }

    @Override
    public long getRequestBaseTimeout() {
        return requestBaseTimeout;
    }

    @Override
    public int getRequestBufferSize() {
        return requestBufferSize;
    }

    @Override
    public int getRetryAttempts() {
        return retryAttempts;
    }

    @Override
    public long getRetryInterval() {
        return retryInterval;
    }

    @Override
    public @NotNull ReplicationRole getRole() {
        return role;
    }

    @Override
    public int getSequencerPartTxnCount() {
        return sequencerPartTxnCount;
    }

    @Override
    public long getSummaryInterval() {
        return summaryInterval;
    }

    @Override
    public long getThrottleWindowDuration() {
        return throttleWindowDuration;
    }

    @Override
    public boolean isCheckpointHistoryEnabled() {
        return checkpointHistoryEnabled;
    }

    @Override
    public boolean isCleanerCheckpointSource() {
        return cleanerCheckpointSource;
    }

    @Override
    public boolean isCleanerEnabled() {
        return cleanerEnabled;
    }

    @Override
    public boolean isMetricsPerTable() {
        return metricsPerTable;
    }

    @Override
    public boolean isThrottleNonData() {
        return throttleNonData;
    }

    public ReplicationConfigurationImpl setCheckpointHistoryEnabled(boolean checkpointHistoryEnabled) {
        this.checkpointHistoryEnabled = checkpointHistoryEnabled;
        return this;
    }

    public ReplicationConfigurationImpl setCheckpointHistoryKeepCount(int checkpointHistoryKeepCount) {
        this.checkpointHistoryKeepCount = checkpointHistoryKeepCount;
        return this;
    }

    public ReplicationConfigurationImpl setCheckpointHistoryLongRetryInterval(long checkpointHistoryLongRetryInterval) {
        this.checkpointHistoryLongRetryInterval = checkpointHistoryLongRetryInterval;
        return this;
    }

    public ReplicationConfigurationImpl setChecksumMode(@NotNull ChecksumMode checksumMode) {
        this.checksumMode = checksumMode;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerBackupWindowCount(int cleanerBackupWindowCount) {
        this.cleanerBackupWindowCount = cleanerBackupWindowCount;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerCheckpointSource(boolean cleanerCheckpointSource) {
        this.cleanerCheckpointSource = cleanerCheckpointSource;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerDeleteConcurrency(int cleanerDeleteConcurrency) {
        this.cleanerDeleteConcurrency = cleanerDeleteConcurrency;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerDroppedTableCooloff(long cleanerDroppedTableCooloff) {
        this.cleanerDroppedTableCooloff = cleanerDroppedTableCooloff;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerEnabled(boolean cleanerEnabled) {
        this.cleanerEnabled = cleanerEnabled;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerInterval(long cleanerInterval) {
        this.cleanerInterval = cleanerInterval;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerMaxRequestsPerSecond(int cleanerMaxRequestsPerSecond) {
        this.cleanerMaxRequestsPerSecond = cleanerMaxRequestsPerSecond;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerProgressWriteInterval(long cleanerProgressWriteInterval) {
        this.cleanerProgressWriteInterval = cleanerProgressWriteInterval;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerRetryAttempts(int cleanerRetryAttempts) {
        this.cleanerRetryAttempts = cleanerRetryAttempts;
        return this;
    }

    public ReplicationConfigurationImpl setCleanerRetryInterval(long cleanerRetryInterval) {
        this.cleanerRetryInterval = cleanerRetryInterval;
        return this;
    }

    public ReplicationConfigurationImpl setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
        return this;
    }

    public ReplicationConfigurationImpl setCompressionThreads(int compressionThreads) {
        this.compressionThreads = compressionThreads;
        return this;
    }

    public ReplicationConfigurationImpl setIndexUploadThrottleInterval(long indexUploadThrottleInterval) {
        this.indexUploadThrottleInterval = indexUploadThrottleInterval;
        return this;
    }

    public ReplicationConfigurationImpl setKeepaliveInterval(long keepaliveInterval) {
        this.keepaliveInterval = keepaliveInterval;
        return this;
    }

    public ReplicationConfigurationImpl setMaxBatchSizeFast(int maxBatchSizeFast) {
        this.maxBatchSizeFast = maxBatchSizeFast;
        return this;
    }

    public ReplicationConfigurationImpl setMaxBatchSizeSlow(int maxBatchSizeSlow) {
        this.maxBatchSizeSlow = maxBatchSizeSlow;
        return this;
    }

    public ReplicationConfigurationImpl setMaxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    public ReplicationConfigurationImpl setMetricsDroppedTablePollCount(int metricsDroppedTablePollCount) {
        this.metricsDroppedTablePollCount = metricsDroppedTablePollCount;
        return this;
    }

    public ReplicationConfigurationImpl setMetricsPerTable(boolean metricsPerTable) {
        this.metricsPerTable = metricsPerTable;
        return this;
    }

    public ReplicationConfigurationImpl setMinThroughput(long minThroughput) {
        this.minThroughput = minThroughput;
        return this;
    }

    public ReplicationConfigurationImpl setObjectStore(@Nullable String objectStore) {
        this.objectStore = objectStore;
        return this;
    }

    public ReplicationConfigurationImpl setReplicaPollInterval(long replicaPollInterval) {
        this.replicaPollInterval = replicaPollInterval;
        return this;
    }

    public ReplicationConfigurationImpl setRequestBaseTimeout(long requestBaseTimeout) {
        this.requestBaseTimeout = requestBaseTimeout;
        return this;
    }

    public ReplicationConfigurationImpl setRequestBufferSize(int requestBufferSize) {
        this.requestBufferSize = requestBufferSize;
        return this;
    }

    public ReplicationConfigurationImpl setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
        return this;
    }

    public ReplicationConfigurationImpl setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
        return this;
    }

    public ReplicationConfigurationImpl setRole(@NotNull ReplicationRole role) {
        this.role = role;
        return this;
    }

    public ReplicationConfigurationImpl setSequencerPartTxnCount(int sequencerPartTxnCount) {
        this.sequencerPartTxnCount = sequencerPartTxnCount;
        return this;
    }

    public ReplicationConfigurationImpl setSummaryInterval(long summaryInterval) {
        this.summaryInterval = summaryInterval;
        return this;
    }

    public ReplicationConfigurationImpl setThrottleNonData(boolean throttleNonData) {
        this.throttleNonData = throttleNonData;
        return this;
    }

    public ReplicationConfigurationImpl setThrottleWindowDuration(long throttleWindowDuration) {
        this.throttleWindowDuration = throttleWindowDuration;
        return this;
    }
}
