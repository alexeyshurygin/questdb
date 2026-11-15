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
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.replication.ReplicationWalDirectoryPolicy;
import io.questdb.std.Rnd;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Data shapes through replication: every column type, schema changes, WAL segment and transaction
 * log part rollover, concurrent writers, and tables that existed before replication.
 */
public class ReplicationDataTest extends AbstractReplicationTest {

    @Test
    public void testAllColumnTypesAndSchemaChanges() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("""
                        CREATE TABLE types (
                            ts TIMESTAMP, b BOOLEAN, bt BYTE, sh SHORT, ch CHAR, i INT, l LONG, f FLOAT, d DOUBLE,
                            s STRING, vc VARCHAR, sym SYMBOL, dt DATE, tn TIMESTAMP_NS, u UUID, ip IPV4, l256 LONG256,
                            g1 GEOHASH(5b), g2 GEOHASH(3c), g4 GEOHASH(6c), g8 GEOHASH(12c), bin BINARY,
                            dec8 DECIMAL(2, 1), dec16 DECIMAL(4, 2), dec32 DECIMAL(9, 3), dec64 DECIMAL(18, 4),
                            dec128 DECIMAL(38, 5), dec256 DECIMAL(76, 6), arr DOUBLE[], arr2 DOUBLE[][]
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
                for (int batch = 0; batch < 5; batch++) {
                    primary.execute("""
                            INSERT INTO types SELECT
                                timestamp_sequence('2024-01-0%d', 7_000_000_000), rnd_boolean(), rnd_byte(), rnd_short(), rnd_char(),
                                rnd_int(0, 1000, 2), rnd_long(0, 100000, 2), rnd_float(2), rnd_double(2), rnd_str(3, 20, 2),
                                rnd_varchar(1, 40, 2), rnd_symbol(20, 1, 8, 2), rnd_date(), rnd_timestamp('2020-01-01'::timestamp_ns, '2030-01-01'::timestamp_ns, 2),
                                rnd_uuid4(2), rnd_ipv4(), rnd_long256(), rnd_geohash(5), rnd_geohash(15), rnd_geohash(30), rnd_geohash(60),
                                rnd_bin(1, 30, 2), rnd_decimal(2, 1, 2), rnd_decimal(4, 2, 2), rnd_decimal(9, 3, 2), rnd_decimal(18, 4, 2),
                                rnd_decimal(38, 5, 2), rnd_decimal(76, 6, 2), rnd_double_array(1, 2), rnd_double_array(2, 2)
                            FROM long_sequence(200)""".formatted(batch + 1));
                }
                primary.execute("ALTER TABLE types ADD COLUMN added LONG");
                primary.execute("INSERT INTO types (ts, l, added) VALUES ('2024-02-01', 1, 2)");
                primary.execute("ALTER TABLE types RENAME COLUMN added TO renamed");
                primary.execute("ALTER TABLE types DROP COLUMN bin");
                primary.execute("ALTER TABLE types ALTER COLUMN i TYPE LONG");
                primary.execute("ALTER TABLE types DEDUP ENABLE UPSERT KEYS(ts)");
                primary.execute("INSERT INTO types (ts, l, renamed) VALUES ('2024-02-01', 3, 4), ('2024-02-02', 5, 6)");
                primary.execute("UPDATE types SET vc = 'updated', renamed = 7 WHERE ts >= '2024-02-01'");
                primary.execute("ALTER TABLE types DROP PARTITION LIST '2024-01-01'");
                awaitSame(primary, replica, "types");

                primary.execute("CREATE TABLE trunc (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO trunc VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                primary.execute("TRUNCATE TABLE trunc");
                primary.execute("INSERT INTO trunc VALUES ('2024-01-03', 3)");
                awaitSame(primary, replica, "trunc");
            }
        });
    }

    @Test
    public void testExistingTableNeedsCheckpointCopy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            // the table exists before replication is turned on
            writeConfiguration(p, "none");
            try (TestServerMain plain = start(p)) {
                plain.execute("CREATE TABLE old (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                plain.execute("INSERT INTO old VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                awaitApplied(plain, "old");
            }
            writeConfiguration(p, "primary");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("INSERT INTO old VALUES ('2024-01-03', 3)");
                awaitApplied(primary, "old");
                writeConfiguration(r, "replica");
                // an empty replica cannot build the table from the store: its history starts later
                try (TestServerMain replica = start(r)) {
                    awaitRole(replica, EngineRole.REPLICA);
                    Thread.sleep(1_000);
                    Assert.assertNull(replica.getEngine().getTableTokenIfExists("old"));
                }
            }
            // bootstrap the replica from a copy of the primary's database, then it follows
            TestUtils.removeTestPath(r + File.separator + "db");
            copyDirectory(Paths.get(p, "db"), Paths.get(r, "db"));
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                awaitRole(replica, EngineRole.REPLICA);
                primary.execute("INSERT INTO old VALUES ('2024-01-04', 4)");
                awaitSame(primary, replica, "old");
                replica.assertSql("SELECT count() n FROM old", "n\n4\n");
            }
        });
    }

    @Test
    public void testRolloverPartsAndConcurrentWriters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            final String[] small = {
                    PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT.getPropertyPath() + "=25",
                    PropertyKey.REPLICATION_PRIMARY_SEQUENCER_PART_TXN_COUNT.getPropertyPath() + "=3"
            };
            writeConfiguration(p, "primary", small);
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE a (ts TIMESTAMP, w INT, v LONG, s SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                primary.execute("CREATE TABLE b (ts TIMESTAMP, w INT, v VARCHAR) TIMESTAMP(ts) PARTITION BY HOUR WAL");
                final int writers = 4;
                final CyclicBarrier barrier = new CyclicBarrier(writers);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread[] threads = new Thread[writers];
                for (int w = 0; w < writers; w++) {
                    final int writer = w;
                    threads[w] = new Thread(() -> {
                        try {
                            barrier.await();
                            final Rnd rnd = new Rnd(writer, writer + 7);
                            for (int i = 0; i < 20; i++) {
                                final String table = (i + writer) % 2 == 0 ? "a" : "b";
                                final StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
                                final int rows = 1 + rnd.nextInt(40);
                                for (int j = 0; j < rows; j++) {
                                    if (j > 0) {
                                        sql.append(',');
                                    }
                                    final long ts = 1_704_067_200_000_000L + (long) rnd.nextInt(72) * 3_600_000_000L + rnd.nextInt(1_000_000);
                                    sql.append('(').append(ts).append("::timestamp, ").append(writer).append(", ");
                                    if (table.equals("a")) {
                                        sql.append(rnd.nextLong()).append(", 'sym").append(rnd.nextInt(30)).append("')");
                                    } else {
                                        sql.append('\'').append("v").append(rnd.nextInt()).append("')");
                                    }
                                }
                                primary.execute(sql.toString());
                            }
                        } catch (Throwable th) {
                            failure.compareAndSet(null, th);
                        } finally {
                            io.questdb.std.str.Path.clearThreadLocals();
                        }
                    });
                    threads[w].start();
                }
                for (Thread t : threads) {
                    t.join();
                }
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                awaitSame(primary, replica, "a");
                awaitSame(primary, replica, "b");
            }
            // a restart mid-part re-uploads the part in the new epoch; the replica keeps up
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                for (int i = 0; i < 7; i++) {
                    primary.execute("INSERT INTO a VALUES ('2024-02-01T0" + i + "', 9, " + i + ", 'z')");
                }
                awaitSame(primary, replica, "a");
            }
        });
    }

    @Test
    public void testUploadMarkersComeOff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            writeConfiguration(p, "primary", PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT.getPropertyPath() + "=5");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE m (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (int i = 0; i < 10; i++) {
                    primary.execute("INSERT INTO m SELECT timestamp_sequence('2024-01-01', 1_000_000), x FROM long_sequence(6)");
                }
                awaitApplied(primary, "m");
                final Path tableDir = Paths.get(p, "db", primary.getEngine().getTableTokenIfExists("m").getDirName());
                // closed segments that the store holds lose their markers; the WAL pool's writer keeps its current one
                TestUtils.assertEventually(() -> Assert.assertTrue("markers: " + markers(tableDir), markers(tableDir) <= 2), 60);
            }
        });
    }

    private static void copyDirectory(Path from, Path to) throws Exception {
        try (Stream<Path> paths = Files.walk(from)) {
            for (Path src : (Iterable<Path>) paths::iterator) {
                final Path dst = to.resolve(from.relativize(src).toString());
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dst);
                } else {
                    Files.copy(src, dst);
                }
            }
        }
    }

    private static long markers(Path tableDir) {
        try (Stream<Path> paths = Files.walk(tableDir)) {
            return paths.filter(x -> x.getFileName().toString().equals(ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER)
                    && !x.getParent().getFileName().toString().equals("txn_seq")).count();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
