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
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Checkpoint history and the WAL cleaner.
 */
public class ReplicationCleanerTest extends AbstractReplicationTest {

    @Test
    public void testCleanerFreesHistoryOlderThanCheckpoints() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            final String r2 = nodeRoot("r2");
            writeConfiguration(
                    p, "primary",
                    PropertyKey.REPLICATION_PRIMARY_SEQUENCER_PART_TXN_COUNT.getPropertyPath() + "=2",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_INTERVAL.getPropertyPath() + "=1s",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_BACKUP_WINDOW_COUNT.getPropertyPath() + "=1",
                    PropertyKey.REPLICATION_PRIMARY_CLEANER_DROPPED_TABLE_COOLOFF.getPropertyPath() + "=0"
            );
            writeConfiguration(r, "replica");
            writeConfiguration(r2, "replica");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("CREATE TABLE gone (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO gone VALUES ('2024-01-01', 1)");
                for (int i = 0; i < 9; i++) {
                    primary.execute("INSERT INTO t VALUES ('2024-01-0" + (i + 1) + "', " + i + ')');
                }
                awaitSame(primary, replica, "t");
                awaitSame(primary, replica, "gone");
                final String tKey = tableKey(primary, "t");
                final String goneKey = tableKey(primary, "gone");
                Assert.assertEquals(5, seqObjects(tKey));
                // a checkpoint at txn 9 makes parts 0..3 (txns 1..8) unnecessary
                primary.execute("CHECKPOINT CREATE");
                primary.execute("CHECKPOINT RELEASE");
                TestUtils.assertEventually(() -> Assert.assertTrue(Files.exists(Paths.get(storeRoot, "_checkpoints"))), 30);
                TestUtils.assertEventually(() -> Assert.assertEquals(1, seqObjects(tKey)), 30);
                // the dropped table's objects go after the (zero) cooloff
                primary.execute("DROP TABLE gone");
                TestUtils.assertEventually(() -> Assert.assertEquals(0, objectCount(goneKey, null)), 30);

                // the caught-up replica keeps following
                primary.execute("INSERT INTO t VALUES ('2024-02-01', 100)");
                awaitSame(primary, replica, "t");
                Assert.assertNull(replica.getEngine().getTableTokenIfExists("gone"));

                // an empty replica cannot rebuild the table: its first transactions are gone
                try (TestServerMain fresh = start(r2)) {
                    awaitRole(fresh, EngineRole.REPLICA);
                    Thread.sleep(1_000);
                    Assert.assertNull(fresh.getEngine().getTableTokenIfExists("t"));
                }
            }
        });
    }

    private static String tableKey(TestServerMain server, String table) {
        // store keys are <table id>_<create timestamp>
        return server.getEngine().getTableTokenIfExists(table).getTableId() + "_";
    }

    private long objectCount(String tableKeyPrefix, String subDir) throws Exception {
        final Path tablesDir = Paths.get(storeRoot, "t");
        try (Stream<Path> dirs = Files.list(tablesDir)) {
            final Path tableDir = dirs.filter(d -> d.getFileName().toString().startsWith(tableKeyPrefix)).findFirst().orElse(null);
            if (tableDir == null) {
                return 0;
            }
            final Path root = subDir == null ? tableDir : tableDir.resolve(subDir);
            if (!Files.exists(root)) {
                return 0;
            }
            try (Stream<Path> files = Files.walk(root)) {
                return files.filter(Files::isRegularFile).count();
            }
        }
    }

    private long seqObjects(String tableKeyPrefix) throws Exception {
        return objectCount(tableKeyPrefix, "seq");
    }
}
