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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestServerMain;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Random WAL transactions from the storage fuzz harness (inserts in and out of order, rollbacks, column
 * adds, drops, renames and type changes, partition drops, truncates, TTL, replace-range commits) run on
 * a primary while the replica, or the primary itself, stops and restarts at random points. The replica
 * must end up with the primary's table: the same rows in the same order and the same columns.
 * <p>
 * {@code -Dquestdb.fuzz.replication.txns=N} sets the number of transactions; the seeds are logged.
 */
public class ReplicationFuzzTest extends AbstractReplicationTest {
    private static final Log LOG = LogFactory.getLog(ReplicationFuzzTest.class);
    private static final String TABLE = "fuzz_t";
    private static final int TXN_COUNT = Integer.getInteger("questdb.fuzz.replication.txns", 60);

    @Test
    public void testPrimaryRestartsDuringRandomTransactions() throws Exception {
        run(true);
    }

    @Test
    public void testReplicaRestartsDuringRandomTransactions() throws Exception {
        run(false);
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

    private void run(boolean isPrimaryRestarting) throws Exception {
        final FuzzRunner fuzzer = new FuzzRunner();
        final Rnd rnd = fuzzer.generateRandom(LOG);
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
                0.0, // partition to parquet
                0.0, // partition to native
                0.1 * rnd.nextDouble(), // truncate
                0.0, // drop table
                rnd.nextDouble(), // set TTL
                rnd.nextDouble(), // replace-range insert
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
        final int partTxnCount = 1 + rnd.nextInt(32);
        final String[] primarySettings = {
                PropertyKey.REPLICATION_PRIMARY_SEQUENCER_PART_TXN_COUNT.getPropertyPath() + '=' + partTxnCount,
                // the harness builds its tables with SELECT rnd_...() columns named the way the test suite configures
                PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getPropertyPath() + "=false"
        };
        final String p = nodeRoot("p");
        final String r = nodeRoot("r");
        TestUtils.assertMemoryLeak(() -> {
            writeConfiguration(p, "primary", primarySettings);
            writeConfiguration(r, "replica");
            TestServerMain primary = null;
            TestServerMain replica = null;
            ObjList<FuzzTransaction> transactions = null;
            try {
                primary = start(p);
                awaitRole(primary, EngineRole.PRIMARY);
                fuzzer.withDb(primary.getEngine(), primary.getSqlExecutionContext());
                fuzzer.createInitialTableWal(TABLE);
                transactions = fuzzer.generateTransactions(TABLE, rnd);
                final int n = transactions.size();
                LOG.info().$("replication fuzz [transactions=").$(n).$(", partTxnCount=").$(partTxnCount)
                        .$(", primaryRestarts=").$(isPrimaryRestarting).I$();
                if (rnd.nextBoolean()) {
                    replica = start(r);
                }
                final ObjList<FuzzTransaction> batch = new ObjList<>();
                int from = 0;
                while (from < n) {
                    final int to = Math.min(n, from + 1 + rnd.nextInt(Math.max(1, n / 5)));
                    batch.clear();
                    for (int i = from; i < to; i++) {
                        batch.add(transactions.getQuick(i));
                    }
                    fuzzer.applyToWal(batch, TABLE, 1 + rnd.nextInt(3), rnd);
                    from = to;

                    if (isPrimaryRestarting && rnd.nextInt(3) == 0) {
                        // a crash keeps what the upload window still held local; the restart uploads it
                        if (rnd.nextBoolean()) {
                            primary.getReplicationService().crashForTesting();
                        }
                        primary = Misc.free(primary);
                        writeConfiguration(p, "primary", primarySettings);
                        primary = start(p);
                        awaitRole(primary, EngineRole.PRIMARY);
                        fuzzer.withDb(primary.getEngine(), primary.getSqlExecutionContext());
                    }
                    switch (rnd.nextInt(4)) {
                        case 0 -> replica = Misc.free(replica);
                        case 1 -> {
                            if (replica == null) {
                                replica = start(r);
                            }
                        }
                        case 2 -> {
                            replica = Misc.free(replica);
                            replica = start(r);
                        }
                        default -> {
                        }
                    }
                }
                if (replica == null) {
                    replica = start(r);
                }
                awaitRole(replica, EngineRole.REPLICA);
                awaitApplied(primary, TABLE);
                Assert.assertNull(((io.questdb.replication.ReplicatedCairoEngine) primary.getEngine()).getFenceReason());

                final TestServerMain primaryNode = primary;
                final TestServerMain replicaNode = replica;
                final String expectedColumns = print(primaryNode, "SHOW COLUMNS FROM " + TABLE);
                final String expectedRows = print(primaryNode, "SELECT * FROM " + TABLE);
                TestUtils.assertEventually(() -> {
                    Assert.assertEquals(
                            primaryNode.getEngine().getTableSequencerAPI().lastTxn(primaryNode.getEngine().verifyTableName(TABLE)),
                            replicaNode.getEngine().getTableSequencerAPI().lastTxn(replicaNode.getEngine().verifyTableName(TABLE))
                    );
                    replicaNode.assertSql("SELECT writerTxn = sequencerTxn ok FROM wal_tables() WHERE name = '" + TABLE + "'", "ok\ntrue\n");
                }, 120);
                Assert.assertEquals(expectedColumns, print(replicaNode, "SHOW COLUMNS FROM " + TABLE));
                TestUtils.assertEquals(expectedRows, print(replicaNode, "SELECT * FROM " + TABLE));
                Assert.assertFalse(replicaNode.getEngine().getTableSequencerAPI().isSuspended(replicaNode.getEngine().verifyTableName(TABLE)));
            } finally {
                Misc.freeObjList(transactions);
                Misc.free(replica);
                Misc.free(primary);
                fuzzer.after();
            }
        });
    }
}
