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


package io.questdb.test.replication;

import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.ServerConfigurationException;
import io.questdb.cairo.CommitMode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.ChecksumMode;
import io.questdb.replication.ReplicationConfiguration;
import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.ReplicationRole;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReplicationConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final Log LOG = LogFactory.getLog(ReplicationConfigurationTest.class);
    private static final String S3_STORE = "s3::bucket=b1;root=db1;region=eu-west-1;";
    private static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setUpRoot() throws Exception {
        final File dir = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(dir.getAbsolutePath());
        root = dir.getAbsolutePath();
    }

    @Test
    public void testAllSettingsParsed() throws Exception {
        final Properties p = primary();
        p.setProperty("replication.requests.max.concurrent", "16");
        p.setProperty("replication.requests.retry.attempts", "7");
        p.setProperty("replication.requests.retry.interval", "1s");
        p.setProperty("replication.primary.throttle.window.duration", "1000");
        p.setProperty("replication.primary.throttle.non.data", "true");
        p.setProperty("replication.primary.sequencer.part.txn.count", "2000");
        p.setProperty("replication.primary.compression.threads", "3");
        p.setProperty("replication.primary.compression.level", "-7");
        p.setProperty("replication.primary.checksum", "Always");
        p.setProperty("replication.primary.keepalive.interval", "2s");
        p.setProperty("replication.primary.index.upload.throttle.interval", "1s");
        p.setProperty("replication.replica.poll.interval", "250");
        p.setProperty("replication.requests.buffer.size", "65536");
        p.setProperty("replication.summary.interval", "0");
        p.setProperty("replication.metrics.per.table", "false");
        p.setProperty("replication.metrics.dropped.table.poll.count", "3");
        p.setProperty("replication.requests.max.batch.size.fast", "8");
        p.setProperty("replication.requests.max.batch.size.slow", "1");
        p.setProperty("replication.requests.base.timeout", "30s");
        p.setProperty("replication.requests.min.throughput", "1048576");
        p.setProperty("replication.primary.cleaner.enabled", "false");
        p.setProperty("replication.primary.cleaner.interval", "1h");
        p.setProperty("replication.primary.cleaner.checkpoint.source", "false");
        p.setProperty("replication.primary.cleaner.backup.window.count", "11");
        p.setProperty("replication.primary.cleaner.delete.concurrency", "12");
        p.setProperty("replication.primary.cleaner.max.requests.per.second", "0");
        p.setProperty("replication.primary.cleaner.progress.write.interval", "100ms");
        p.setProperty("replication.primary.cleaner.dropped.table.cooloff", "2h");
        p.setProperty("replication.primary.cleaner.retry.attempts", "0");
        p.setProperty("replication.primary.cleaner.retry.interval", "5m");
        p.setProperty("checkpoint.history.enabled", "false");
        p.setProperty("checkpoint.history.keep.count", "9");
        p.setProperty("checkpoint.history.long.retry.interval", "30s");

        final ReplicationConfiguration c = load(p).getReplicationConfiguration();
        Assert.assertEquals(ReplicationRole.PRIMARY, c.getRole());
        Assert.assertEquals(S3_STORE, c.getObjectStore());
        Assert.assertEquals(16, c.getMaxConcurrentRequests());
        Assert.assertEquals(7, c.getRetryAttempts());
        Assert.assertEquals(1_000, c.getRetryInterval());
        Assert.assertEquals(1_000, c.getThrottleWindowDuration());
        Assert.assertTrue(c.isThrottleNonData());
        Assert.assertEquals(2_000, c.getSequencerPartTxnCount());
        Assert.assertEquals(3, c.getCompressionThreads());
        Assert.assertEquals(-7, c.getCompressionLevel());
        Assert.assertEquals(ChecksumMode.ALWAYS, c.getChecksumMode());
        Assert.assertEquals(2_000, c.getKeepaliveInterval());
        Assert.assertEquals(1_000, c.getIndexUploadThrottleInterval());
        Assert.assertEquals(250, c.getReplicaPollInterval());
        Assert.assertEquals(65_536, c.getRequestBufferSize());
        Assert.assertEquals(0, c.getSummaryInterval());
        Assert.assertFalse(c.isMetricsPerTable());
        Assert.assertEquals(3, c.getMetricsDroppedTablePollCount());
        Assert.assertEquals(8, c.getMaxBatchSizeFast());
        Assert.assertEquals(1, c.getMaxBatchSizeSlow());
        Assert.assertEquals(30_000, c.getRequestBaseTimeout());
        Assert.assertEquals(1_048_576, c.getMinThroughput());
        Assert.assertFalse(c.isCleanerEnabled());
        Assert.assertEquals(3_600_000, c.getCleanerInterval());
        Assert.assertFalse(c.isCleanerCheckpointSource());
        Assert.assertEquals(11, c.getCleanerBackupWindowCount());
        Assert.assertEquals(12, c.getCleanerDeleteConcurrency());
        Assert.assertEquals(0, c.getCleanerMaxRequestsPerSecond());
        Assert.assertEquals(100, c.getCleanerProgressWriteInterval());
        Assert.assertEquals(7_200_000, c.getCleanerDroppedTableCooloff());
        Assert.assertEquals(0, c.getCleanerRetryAttempts());
        Assert.assertEquals(300_000, c.getCleanerRetryInterval());
        Assert.assertFalse(c.isCheckpointHistoryEnabled());
        Assert.assertEquals(9, c.getCheckpointHistoryKeepCount());
        Assert.assertEquals(30_000, c.getCheckpointHistoryLongRetryInterval());
    }

    @Test
    public void testChecksumModeNames() {
        Assert.assertEquals(ChecksumMode.SERVICE_DEPENDENT, ChecksumMode.fromConfig(" service-dependent "));
        Assert.assertEquals(ChecksumMode.NEVER, ChecksumMode.fromConfig("NEVER"));
        Assert.assertEquals(ChecksumMode.ALWAYS, ChecksumMode.fromConfig("always"));
        Assert.assertNull(ChecksumMode.fromConfig("sometimes"));
        Assert.assertNull(ChecksumMode.fromConfig(null));
        Assert.assertEquals("service-dependent", ChecksumMode.SERVICE_DEPENDENT.getConfigName());
    }

    @Test
    public void testRolloverDefaultFollowsRoleAndCommitModeDoesNot() throws Exception {
        // As QuestDB Enterprise: a primary rolls WAL segments at 2 MiB unless set, and the commit mode default
        // stays nosync with or without replication; operators set adaptive on nodes that can become primary.
        final Properties none = new Properties();
        ServerConfiguration c = load(none);
        Assert.assertEquals(50 * Numbers.SIZE_1MB, c.getCairoConfiguration().getWalSegmentRolloverSize());
        Assert.assertEquals(CommitMode.NOSYNC, c.getCairoConfiguration().getCommitMode());

        c = load(primary());
        Assert.assertEquals(2 * Numbers.SIZE_1MB, c.getCairoConfiguration().getWalSegmentRolloverSize());
        Assert.assertEquals(CommitMode.NOSYNC, c.getCairoConfiguration().getCommitMode());

        final Properties replica = new Properties();
        replica.setProperty("replication.role", "replica");
        replica.setProperty("replication.object.store", S3_STORE);
        c = load(replica);
        Assert.assertEquals(50 * Numbers.SIZE_1MB, c.getCairoConfiguration().getWalSegmentRolloverSize());
        Assert.assertEquals(CommitMode.NOSYNC, c.getCairoConfiguration().getCommitMode());

        // explicit settings win
        final Properties explicit = primary();
        explicit.setProperty("cairo.wal.segment.rollover.size", "8M");
        explicit.setProperty("cairo.commit.mode", "adaptive");
        c = load(explicit);
        Assert.assertEquals(8 * Numbers.SIZE_1MB, c.getCairoConfiguration().getWalSegmentRolloverSize());
        Assert.assertEquals(CommitMode.ADAPTIVE, c.getCairoConfiguration().getCommitMode());
    }

    @Test
    public void testDefaultsMatchEnterprise() throws Exception {
        // Values from the replication section of QuestDB Enterprise's server.conf.
        final ReplicationConfiguration c = load(new Properties()).getReplicationConfiguration();
        assertEnterpriseDefaults(c, ReplicationRole.NONE);
        Assert.assertNull(c.getObjectStore());
        Assert.assertEquals(4, c.getCleanerDeleteConcurrency());

        assertEnterpriseDefaults(new ReplicationConfigurationImpl(), ReplicationRole.NONE);
        assertEnterpriseDefaults(load(primary()).getReplicationConfiguration(), ReplicationRole.PRIMARY);
    }

    @Test
    public void testDeleteConcurrencyDerivedFromRequestConcurrency() throws Exception {
        final Properties p = primary();
        p.setProperty("replication.requests.max.concurrent", "4096");
        Assert.assertEquals(32, load(p).getReplicationConfiguration().getCleanerDeleteConcurrency());
        p.setProperty("replication.requests.max.concurrent", "512");
        Assert.assertEquals(16, load(p).getReplicationConfiguration().getCleanerDeleteConcurrency());
        p.setProperty("replication.requests.max.concurrent", "1");
        Assert.assertEquals(4, load(p).getReplicationConfiguration().getCleanerDeleteConcurrency());
    }

    @Test
    public void testDisabledByDefaultOnPlainServerConfiguration() {
        // An implementation that does not override the default method reports replication as disabled.
        final ServerConfiguration configuration = (ServerConfiguration) Proxy.newProxyInstance(
                ServerConfiguration.class.getClassLoader(),
                new Class<?>[]{ServerConfiguration.class},
                (proxy, method, args) -> {
                    if (method.isDefault()) {
                        return InvocationHandler.invokeDefault(proxy, method, args);
                    }
                    throw new UnsupportedOperationException(method.getName());
                }
        );
        final ReplicationConfiguration c = configuration.getReplicationConfiguration();
        Assert.assertEquals(ReplicationRole.NONE, c.getRole());
        Assert.assertNull(c.getObjectStore());
    }

    @Test
    public void testEnvOverridesConf() throws Exception {
        final Properties p = new Properties();
        p.setProperty("replication.role", "none");
        final Map<String, String> env = new HashMap<>();
        env.put("QDB_REPLICATION_ROLE", "Replica ");
        env.put("QDB_REPLICATION_OBJECT_STORE", "fs::root=/tmp/store;");
        env.put("QDB_REPLICATION_REPLICA_POLL_INTERVAL", "100");
        final ReplicationConfiguration c = load(p, env).getReplicationConfiguration();
        Assert.assertEquals(ReplicationRole.REPLICA, c.getRole());
        Assert.assertEquals("fs::root=/tmp/store;", c.getObjectStore());
        Assert.assertEquals(100, c.getReplicaPollInterval());
    }

    @Test
    public void testInvalidValuesRejected() throws Exception {
        assertInvalid("replication.role", "standby", "invalid configuration value [key=replication.role, value=standby], expected one of: none, primary, replica");
        assertInvalid("replication.primary.checksum", "sometimes", "invalid configuration value [key=replication.primary.checksum, value=sometimes], expected one of: service-dependent, never, always");
        assertInvalid("replication.primary.compression.level", "23", "configuration value out of range [key=replication.primary.compression.level, value=23, min=-7, max=22]");
        assertInvalid("replication.primary.compression.level", "-8", "configuration value out of range [key=replication.primary.compression.level, value=-8, min=-7, max=22]");
        assertInvalid("replication.requests.max.concurrent", "0", "configuration value out of range [key=replication.requests.max.concurrent, value=0, min=1, max=4096]");
        assertInvalid("replication.requests.retry.attempts", "-1", "configuration value out of range [key=replication.requests.retry.attempts, value=-1, min=0, max=1000]");
        assertInvalid("replication.primary.throttle.window.duration", "-1", "configuration value out of range [key=replication.primary.throttle.window.duration, value=-1ms, min=0ms, max=3600000ms]");
        assertInvalid("replication.primary.keepalive.interval", "99", "configuration value out of range [key=replication.primary.keepalive.interval, value=99ms, min=100ms, max=3600000ms]");
        assertInvalid("replication.primary.sequencer.part.txn.count", "0", "configuration value out of range [key=replication.primary.sequencer.part.txn.count, value=0, min=1, max=10000000]");
        assertInvalid("replication.replica.poll.interval", "9", "configuration value out of range [key=replication.replica.poll.interval, value=9ms, min=10ms, max=3600000ms]");
        assertInvalid("replication.requests.buffer.size", "1023", "configuration value out of range [key=replication.requests.buffer.size, value=1023, min=1024, max=67108864]");
        assertInvalid("replication.requests.min.throughput", "0", "configuration value out of range [key=replication.requests.min.throughput, value=0, min=1, max=9223372036854775807]");
        assertInvalid("replication.primary.cleaner.interval", "999ms", "configuration value out of range [key=replication.primary.cleaner.interval, value=999ms, min=1000ms, max=86400000ms]");
        assertInvalid("replication.primary.cleaner.interval", "25h", "configuration value out of range [key=replication.primary.cleaner.interval, value=90000000ms, min=1000ms, max=86400000ms]");
        assertInvalid("replication.primary.cleaner.delete.concurrency", "3", "configuration value out of range [key=replication.primary.cleaner.delete.concurrency, value=3, min=4, max=32]");
        assertInvalid("replication.primary.cleaner.delete.concurrency", "33", "configuration value out of range [key=replication.primary.cleaner.delete.concurrency, value=33, min=4, max=32]");
        assertInvalid("replication.primary.cleaner.max.requests.per.second", "10001", "configuration value out of range [key=replication.primary.cleaner.max.requests.per.second, value=10001, min=0, max=10000]");
        assertInvalid("replication.primary.cleaner.progress.write.interval", "61s", "configuration value out of range [key=replication.primary.cleaner.progress.write.interval, value=61000ms, min=100ms, max=60000ms]");
        assertInvalid("replication.primary.cleaner.retry.attempts", "101", "configuration value out of range [key=replication.primary.cleaner.retry.attempts, value=101, min=0, max=100]");
        assertInvalid("replication.primary.cleaner.retry.interval", "6m", "configuration value out of range [key=replication.primary.cleaner.retry.interval, value=360000ms, min=0ms, max=300000ms]");
        assertInvalid("replication.primary.cleaner.backup.window.count", "0", "configuration value out of range [key=replication.primary.cleaner.backup.window.count, value=0, min=1, max=100000]");
        assertInvalid("checkpoint.history.keep.count", "0", "configuration value out of range [key=checkpoint.history.keep.count, value=0, min=1, max=1000000]");
        assertInvalid("replication.requests.retry.interval", "abc", "invalid configuration value [key=replication.requests.retry.interval, value=abc]");
    }

    @Test
    public void testObjectStoreErrorsDoNotLeakSecrets() throws Exception {
        final Properties p = new Properties();
        p.setProperty("replication.role", "replica");
        p.setProperty("replication.object.store", "s3::bucket=b;secret_access_key=TOPSECRETVALUE;");
        try {
            load(p);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            Assert.assertEquals(
                    "invalid replication.object.store: object store settings access_key_id and secret_access_key must be set together",
                    e.getMessage()
            );
        }
        p.setProperty("replication.object.store", "s3::bucket=b;TOPSECRETVALUE;");
        try {
            load(p);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            Assert.assertEquals(
                    "invalid replication.object.store: object store setting must be 'key=value' [setting=<14 chars>]",
                    e.getMessage()
            );
        }
    }

    @Test
    public void testObjectStoreFromSecretFile() throws Exception {
        final File secret = temp.newFile("store-secret");
        Files.write(secret.toPath(), "fs::root=/data/store;\n".getBytes(StandardCharsets.UTF_8));
        final Properties p = new Properties();
        p.setProperty("replication.role", "primary");
        p.setProperty("replication.object.store.file", secret.getAbsolutePath());
        Assert.assertEquals("fs::root=/data/store;", load(p).getReplicationConfiguration().getObjectStore());
        Assert.assertTrue(PropertyKey.REPLICATION_OBJECT_STORE.isSensitive());
    }

    @Test
    public void testObjectStoreIsOptionalWithoutRole() throws Exception {
        final Properties p = new Properties();
        p.setProperty("replication.object.store", "not even parsed");
        final ReplicationConfiguration c = load(p).getReplicationConfiguration();
        Assert.assertEquals(ReplicationRole.NONE, c.getRole());
        Assert.assertEquals("not even parsed", c.getObjectStore());
    }

    @Test
    public void testReadOnlyCannotCombineWithRole() throws Exception {
        for (String role : new String[]{"primary", "replica"}) {
            final Properties p = new Properties();
            p.setProperty("readonly", "true");
            p.setProperty("replication.role", role);
            p.setProperty("replication.object.store", S3_STORE);
            try {
                load(p);
                Assert.fail();
            } catch (ServerConfigurationException e) {
                Assert.assertEquals(
                        "replication.role=" + role + " cannot be combined with readonly=true; a replica refuses writes by itself",
                        e.getMessage()
                );
            }
        }
        final Properties p = new Properties();
        p.setProperty("readonly", "true");
        p.setProperty("replication.role", "none");
        Assert.assertTrue(load(p).getCairoConfiguration().isReadOnlyInstance());
    }

    @Test
    public void testRoleNames() {
        Assert.assertEquals(ReplicationRole.NONE, ReplicationRole.fromConfig("none"));
        Assert.assertEquals(ReplicationRole.PRIMARY, ReplicationRole.fromConfig(" PRIMARY "));
        Assert.assertEquals(ReplicationRole.REPLICA, ReplicationRole.fromConfig("Replica"));
        Assert.assertNull(ReplicationRole.fromConfig("master"));
        Assert.assertNull(ReplicationRole.fromConfig(""));
        Assert.assertNull(ReplicationRole.fromConfig(null));
        Assert.assertFalse(ReplicationRole.NONE.isEnabled());
        Assert.assertTrue(ReplicationRole.PRIMARY.isEnabled());
        Assert.assertTrue(ReplicationRole.REPLICA.isEnabled());
        Assert.assertEquals("replica", ReplicationRole.REPLICA.getConfigName());
    }

    @Test
    public void testStoreRequiredWithRole() throws Exception {
        for (String role : new String[]{"primary", "replica"}) {
            final Properties p = new Properties();
            p.setProperty("replication.role", role);
            try {
                load(p);
                Assert.fail();
            } catch (ServerConfigurationException e) {
                Assert.assertEquals("replication.object.store must be set when replication.role=" + role, e.getMessage());
            }
            p.setProperty("replication.object.store", "");
            try {
                load(p);
                Assert.fail();
            } catch (ServerConfigurationException e) {
                Assert.assertEquals("replication.object.store must be set when replication.role=" + role, e.getMessage());
            }
            p.setProperty("replication.object.store", "azblob::endpoint=x;");
            try {
                load(p);
                Assert.fail();
            } catch (ServerConfigurationException e) {
                Assert.assertEquals("invalid replication.object.store: object store scheme is not supported in this build [scheme=azblob]", e.getMessage());
            }
        }
    }

    private static void assertEnterpriseDefaults(ReplicationConfiguration c, ReplicationRole role) {
        Assert.assertEquals(role, c.getRole());
        Assert.assertEquals(128, c.getMaxConcurrentRequests());
        Assert.assertEquals(3, c.getRetryAttempts());
        Assert.assertEquals(200, c.getRetryInterval());
        Assert.assertEquals(10_000, c.getThrottleWindowDuration());
        Assert.assertFalse(c.isThrottleNonData());
        Assert.assertEquals(5_000, c.getSequencerPartTxnCount());
        Assert.assertEquals(Math.max(1, Runtime.getRuntime().availableProcessors() / 2), c.getCompressionThreads());
        Assert.assertEquals(1, c.getCompressionLevel());
        Assert.assertEquals(ChecksumMode.SERVICE_DEPENDENT, c.getChecksumMode());
        Assert.assertEquals(10_000, c.getKeepaliveInterval());
        Assert.assertEquals(-1, c.getIndexUploadThrottleInterval());
        Assert.assertEquals(1_000, c.getReplicaPollInterval());
        Assert.assertEquals(32_768, c.getRequestBufferSize());
        Assert.assertEquals(60_000, c.getSummaryInterval());
        Assert.assertTrue(c.isMetricsPerTable());
        Assert.assertEquals(10, c.getMetricsDroppedTablePollCount());
        Assert.assertEquals(64, c.getMaxBatchSizeFast());
        Assert.assertEquals(2, c.getMaxBatchSizeSlow());
        Assert.assertEquals(10_000, c.getRequestBaseTimeout());
        Assert.assertEquals(262_144, c.getMinThroughput());
        Assert.assertTrue(c.isCleanerEnabled());
        Assert.assertEquals(600_000, c.getCleanerInterval());
        Assert.assertTrue(c.isCleanerCheckpointSource());
        Assert.assertEquals(5, c.getCleanerBackupWindowCount());
        Assert.assertEquals(-1, c.getCleanerMaxRequestsPerSecond());
        Assert.assertEquals(5_000, c.getCleanerProgressWriteInterval());
        Assert.assertEquals(3_600_000, c.getCleanerDroppedTableCooloff());
        Assert.assertEquals(20, c.getCleanerRetryAttempts());
        Assert.assertEquals(2_000, c.getCleanerRetryInterval());
        Assert.assertTrue(c.isCheckpointHistoryEnabled());
        Assert.assertEquals(100, c.getCheckpointHistoryKeepCount());
        Assert.assertEquals(60_000, c.getCheckpointHistoryLongRetryInterval());
    }

    private static void assertInvalid(String key, String value, String expectedMessage) throws Exception {
        final Properties p = primary();
        p.setProperty(key, value);
        try {
            load(p);
            Assert.fail("expected failure for " + key + '=' + value);
        } catch (ServerConfigurationException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }

    private static PropServerConfiguration load(Properties properties) throws Exception {
        return load(properties, null);
    }

    private static PropServerConfiguration load(Properties properties, Map<String, String> env) throws Exception {
        return new PropServerConfiguration(root, properties, env, LOG, new BuildInformationHolder());
    }

    private static Properties primary() {
        final Properties p = new Properties();
        p.setProperty("replication.role", "primary");
        p.setProperty("replication.object.store", S3_STORE);
        return p;
    }
}
