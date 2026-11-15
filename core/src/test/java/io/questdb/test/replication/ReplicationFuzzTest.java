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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestServerMain;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.replication.store.FakeS3Server;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Random WAL transactions from the storage fuzz harness run on a primary, over one to three tables:
 * inserts in and out of order, rollbacks, column adds, drops, renames and type changes, partition drops,
 * conversions to Parquet and back, truncates, TTL, replace-range commits, and tables dropped and created
 * again under the same name. Between batches the replica stops, starts or restarts; depending on the
 * test the primary restarts too (gracefully or as after a crash), the object store fails requests at
 * random before or after it served them, or the store is S3 behind the real S3 client. Each replica
 * table must end with the primary's columns and rows, in the primary's order.
 * <p>
 * {@code -Dquestdb.fuzz.replication.txns=N} sets the number of transactions per table; the seeds are
 * logged.
 */
public class ReplicationFuzzTest extends AbstractReplicationTest {
    private static final Log LOG = LogFactory.getLog(ReplicationFuzzTest.class);
    private static final int TXN_COUNT = Integer.getInteger("questdb.fuzz.replication.txns", 60);

    @Test
    public void testPrimaryRestartsDuringRandomTransactions() throws Exception {
        run(true, false, false);
    }

    @Test
    public void testRandomTransactionsThroughS3() throws Exception {
        run(false, false, true);
    }

    @Test
    public void testReplicaRestartsDuringRandomTransactions() throws Exception {
        run(false, false, false);
    }

    @Test
    public void testStoreFaultsDuringRandomTransactions() throws Exception {
        run(true, true, false);
    }

    private static String print(TestServerMain server, String sql) {
        final StringSink sink = new StringSink();
        try {
            TestUtils.printSql(server.getEngine(), server.getSqlExecutionContext(), sql, sink);
        } catch (Exception e) {
            throw new AssertionError(sql, e);
        }
        return sink.toString();
    }

    private static TestServerMain startNode(String root, RandomFaults faults) {
        return faults != null ? start(root, faults) : start(root);
    }

    private void run(boolean isPrimaryRestarting, boolean isStoreFaulty, boolean isS3) throws Exception {
        final FuzzRunner fuzzer = new FuzzRunner();
        final Rnd rnd = fuzzer.generateRandom(LOG);
        // QuestDB refuses a replace-range commit over a Parquet partition, so a run either converts
        // partitions or replaces ranges, as the storage fuzz tests do; the Parquet runs keep their tables
        final boolean isParquet = rnd.nextBoolean();
        fuzzer.setFuzzProbabilities(
                0.5 * rnd.nextDouble(), // cancel rows
                rnd.nextDouble(), // column not set
                rnd.nextDouble(), // column set to NULL
                0.5 * rnd.nextDouble(), // rollback
                rnd.nextDouble(), // add column
                rnd.nextDouble(), // remove column
                rnd.nextDouble(), // rename column
                rnd.nextDouble(), // change column type
                rnd.nextDouble(), // add data
                0.01, // equal timestamps
                rnd.nextDouble(), // drop partition
                isParquet ? 0.5 * rnd.nextDouble() : 0.0, // partition to parquet
                isParquet ? 0.5 * rnd.nextDouble() : 0.0, // partition to native
                0.1 * rnd.nextDouble(), // truncate
                isParquet ? 0.0 : 0.1 * rnd.nextDouble(), // drop the table and create it again
                rnd.nextDouble(), // set TTL
                isParquet ? 0.0 : rnd.nextDouble(), // replace-range insert
                0.1 * rnd.nextDouble(), // symbol access validation
                0.1 * rnd.nextDouble() // query
        );
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                1 + rnd.nextInt(50_000),
                TXN_COUNT / 2 + rnd.nextInt(TXN_COUNT),
                fuzzer.randomiseStringLengths(rnd, 200),
                1 + rnd.nextInt(50),
                5 + rnd.nextInt(200),
                rnd.nextInt(5_000),
                5 + rnd.nextInt(10)
        );
        final int tableCount = 1 + rnd.nextInt(3);
        final int partTxnCount = 1 + rnd.nextInt(32);
        // faults draw from their own generator: the store is called from several threads
        final RandomFaults faults = isStoreFaulty ? new RandomFaults(new Rnd(rnd.nextLong(), rnd.nextLong()), 0.15 * rnd.nextDouble(), 0.1 * rnd.nextDouble()) : null;
        final String p = nodeRoot("p");
        final String r = nodeRoot("r");
        TestUtils.assertMemoryLeak(() -> {
            TestServerMain primary = null;
            TestServerMain replica = null;
            final ObjList<ObjList<FuzzTransaction>> transactions = new ObjList<>();
            final FakeS3Server s3 = isS3 ? new FakeS3Server() : null;
            try {
                final String store = s3 != null
                        ? PropertyKey.REPLICATION_OBJECT_STORE.getPropertyPath() + "=s3::bucket=" + FakeS3Server.BUCKET + ";root=fuzz;region="
                        + FakeS3Server.REGION + ";endpoint=" + s3.getEndpoint() + ";access_key_id=" + FakeS3Server.ACCESS_KEY_ID
                        + ";secret_access_key=" + FakeS3Server.SECRET_ACCESS_KEY + ';'
                        : PropertyKey.REPLICATION_OBJECT_STORE.getPropertyPath() + "=fs::root=" + storeRoot + ';';
                final String[] primarySettings = {
                        store,
                        PropertyKey.REPLICATION_PRIMARY_SEQUENCER_PART_TXN_COUNT.getPropertyPath() + '=' + partTxnCount,
                        // the harness builds its tables with SELECT rnd_...() columns named the way the test suite configures
                        PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getPropertyPath() + "=false"
                };
                writeConfiguration(p, "primary", primarySettings);
                writeConfiguration(r, "replica", store);
                primary = startNode(p, faults);
                awaitRole(primary, EngineRole.PRIMARY);
                fuzzer.withDb(primary.getEngine(), primary.getSqlExecutionContext());
                final String[] tables = new String[tableCount];
                final int[] positions = new int[tableCount];
                int remaining = 0;
                for (int i = 0; i < tableCount; i++) {
                    tables[i] = "fuzz_t" + i;
                    fuzzer.createInitialTableWal(tables[i]);
                    transactions.add(fuzzer.generateTransactions(tables[i], rnd));
                    remaining += transactions.getQuick(i).size();
                }
                LOG.info().$("replication fuzz [tables=").$(tableCount).$(", transactions=").$(remaining).$(", partTxnCount=").$(partTxnCount)
                        .$(", primaryRestarts=").$(isPrimaryRestarting).$(", storeFaults=").$(isStoreFaulty).$(", s3=").$(isS3).$(", parquet=").$(isParquet).I$();
                if (rnd.nextBoolean()) {
                    replica = startNode(r, faults);
                }
                final ObjList<FuzzTransaction> batch = new ObjList<>();
                while (remaining > 0) {
                    int t = rnd.nextInt(tableCount);
                    while (positions[t] == transactions.getQuick(t).size()) {
                        t = (t + 1) % tableCount;
                    }
                    final ObjList<FuzzTransaction> all = transactions.getQuick(t);
                    final int n = all.size();
                    final int to = Math.min(n, positions[t] + 1 + rnd.nextInt(Math.max(1, n / 5)));
                    batch.clear();
                    for (int i = positions[t]; i < to; i++) {
                        batch.add(all.getQuick(i));
                    }
                    fuzzer.applyToWal(batch, tables[t], 1 + rnd.nextInt(3), rnd);
                    remaining -= to - positions[t];
                    positions[t] = to;

                    if (isPrimaryRestarting && rnd.nextInt(3) == 0) {
                        // a crash keeps what the upload window still held local; the restart uploads it
                        if (rnd.nextBoolean()) {
                            primary.getReplicationService().crashForTesting();
                        }
                        primary = Misc.free(primary);
                        writeConfiguration(p, "primary", primarySettings);
                        primary = startNode(p, faults);
                        awaitRole(primary, EngineRole.PRIMARY);
                        fuzzer.withDb(primary.getEngine(), primary.getSqlExecutionContext());
                    }
                    switch (rnd.nextInt(4)) {
                        case 0 -> replica = Misc.free(replica);
                        case 1 -> {
                            if (replica == null) {
                                replica = startNode(r, faults);
                            }
                        }
                        case 2 -> {
                            replica = Misc.free(replica);
                            replica = startNode(r, faults);
                        }
                        default -> {
                        }
                    }
                }
                // the store recovers; whatever the faults delayed must arrive now
                if (faults != null) {
                    faults.stop();
                    LOG.info().$("replication fuzz injected store faults [count=").$(faults.getCount()).I$();
                }
                if (replica == null) {
                    replica = startNode(r, faults);
                }
                awaitRole(replica, EngineRole.REPLICA);
                // no store fault, restart or crash costs the primary its lease
                Assert.assertEquals(EngineRole.PRIMARY, role(primary));
                Assert.assertNull(((ReplicatedCairoEngine) primary.getEngine()).getFenceReason());

                final TestServerMain primaryNode = primary;
                final TestServerMain replicaNode = replica;
                for (int i = 0; i < tableCount; i++) {
                    final String table = tables[i];
                    awaitApplied(primaryNode, table);
                    final TableToken primaryToken = primaryNode.getEngine().verifyTableName(table);
                    final String expectedColumns = print(primaryNode, "SHOW COLUMNS FROM " + table);
                    final String expectedRows = print(primaryNode, "SELECT * FROM " + table);
                    TestUtils.assertEventually(() -> {
                        // the same incarnation of the table, at the same transaction, applied
                        final TableToken replicaToken = replicaNode.getEngine().getTableTokenIfExists(table);
                        Assert.assertNotNull(replicaToken);
                        Assert.assertEquals(primaryToken.getDirName(), replicaToken.getDirName());
                        Assert.assertEquals(
                                primaryNode.getEngine().getTableSequencerAPI().lastTxn(primaryToken),
                                replicaNode.getEngine().getTableSequencerAPI().lastTxn(replicaToken)
                        );
                        replicaNode.assertSql("SELECT writerTxn = sequencerTxn ok FROM wal_tables() WHERE name = '" + table + "'", "ok\ntrue\n");
                    }, 120);
                    Assert.assertEquals(table, expectedColumns, print(replicaNode, "SHOW COLUMNS FROM " + table));
                    TestUtils.assertEquals(expectedRows, print(replicaNode, "SELECT * FROM " + table));
                    Assert.assertFalse(table, replicaNode.getEngine().getTableSequencerAPI().isSuspended(replicaNode.getEngine().verifyTableName(table)));
                }
                if (s3 != null) {
                    // every request of both nodes carried a valid signature
                    Assert.assertEquals(0, s3.getSignatureFailures());
                }
            } finally {
                for (int i = 0, n = transactions.size(); i < n; i++) {
                    Misc.freeObjList(transactions.getQuick(i));
                }
                Misc.free(replica);
                Misc.free(primary);
                Misc.free(s3);
                fuzzer.after();
            }
        });
    }

    /**
     * Fails a share of the object store's requests: before the store sees them, with an error that is
     * worth retrying or one that is not, or after the store served them, as a lost response.
     */
    private static class RandomFaults implements FaultyObjectStore.Hook {
        private final double afterProbability;
        private final double beforeProbability;
        private final Rnd rnd;
        private int count;
        private boolean isStopped;

        RandomFaults(Rnd rnd, double beforeProbability, double afterProbability) {
            this.rnd = rnd;
            this.beforeProbability = beforeProbability;
            this.afterProbability = afterProbability;
        }

        @Override
        public synchronized void after(String op, String key) {
            if (!isStopped && rnd.nextDouble() < afterProbability) {
                count++;
                throw ObjectStoreException.retryable("injected lost response [op=" + op + ", key=" + key + ']', null);
            }
        }

        @Override
        public synchronized void before(String op, String key) {
            if (!isStopped && rnd.nextDouble() < beforeProbability) {
                count++;
                if (rnd.nextBoolean()) {
                    throw ObjectStoreException.retryable("injected failure [op=" + op + ", key=" + key + ']', null);
                }
                throw ObjectStoreException.fatal("injected fatal failure [op=" + op + ", key=" + key + ']');
            }
        }

        synchronized int getCount() {
            return count;
        }

        synchronized void stop() {
            isStopped = true;
        }
    }
}
