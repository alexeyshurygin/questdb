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
import io.questdb.cairo.TableUtils;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * Seeding nodes from a checkpoint copy: a new replica, and disaster recovery of a lost primary.
 */
public class ReplicationBootstrapTest extends AbstractReplicationTest {

    @Test
    public void testReplicationUnderTheDefaultCommitMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            // nosync, what a node runs with when its configuration does not name a commit mode
            final String nosync = PropertyKey.CAIRO_COMMIT_MODE.getPropertyPath() + "=nosync";
            writeConfiguration(p, "primary", nosync);
            writeConfiguration(r, "replica", nosync);
            try (TestServerMain replica = start(r)) {
                try (TestServerMain primary = start(p)) {
                    awaitRole(primary, io.questdb.replication.ReplicatedCairoEngine.EngineRole.PRIMARY);
                    Assert.assertEquals(io.questdb.cairo.CommitMode.NOSYNC, primary.getEngine().getConfiguration().getCommitMode());
                    Assert.assertEquals(io.questdb.cairo.CommitMode.NOSYNC, replica.getEngine().getConfiguration().getCommitMode());
                    primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                    awaitSame(primary, replica, "t");
                }
                // the primary stops and comes back: it owns the store, holds all of it, and carries on
                writeConfiguration(p, "primary", nosync);
                try (TestServerMain primary = start(p)) {
                    awaitRole(primary, io.questdb.replication.ReplicatedCairoEngine.EngineRole.PRIMARY);
                    primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                    primary.execute("ALTER TABLE t ADD COLUMN s SYMBOL");
                    primary.execute("INSERT INTO t VALUES ('2024-01-04', 4, 'a')");
                    awaitSame(primary, replica, "t");
                    replica.assertSql("SELECT count() n FROM t", "n\n4\n");
                }
            }
        });
    }

    @Test
    public void testCheckpointCopyBootstrapsReplica() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1), ('2024-01-02', 2)");
                awaitApplied(primary, "t");
                final String primaryId = primary.getReplicationService().getInstanceId();

                // the copy carries everything, the source's instance id included
                primary.execute("CHECKPOINT CREATE");
                copyInstall(p, r, "db", TableUtils.CHECKPOINT_DIRECTORY, ".replication");
                primary.execute("CHECKPOINT RELEASE");
                Files.createFile(Paths.get(r, TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME));
                Assert.assertEquals(primaryId, Files.readString(Paths.get(r, ".replication", "instance")).trim());

                // the primary moves on after the checkpoint
                primary.execute("INSERT INTO t VALUES ('2024-01-03', 3)");
                primary.execute("ALTER TABLE t ADD COLUMN s STRING");
                primary.execute("INSERT INTO t VALUES ('2024-01-04', 4, 'four')");
                awaitApplied(primary, "t");

                try (TestServerMain replica = start(r)) {
                    awaitRole(replica, EngineRole.REPLICA);
                    // a restored copy is another instance
                    final String replicaId = replica.getReplicationService().getInstanceId();
                    Assert.assertNotNull(replicaId);
                    Assert.assertNotEquals(primaryId, replicaId);
                    Assert.assertEquals(replicaId, Files.readString(Paths.get(r, ".replication", "instance")).trim());
                    // it resumes at the checkpoint's transaction and follows
                    awaitSame(primary, replica, "t");
                    primary.execute("INSERT INTO t VALUES ('2024-01-05', 5, 'five')");
                    awaitSame(primary, replica, "t");
                    replica.assertSql("SELECT count() n FROM t", "n\n5\n");
                    // a replica uploads nothing
                    Assert.assertEquals(-1, replica.getReplicationService().getUploadedTxn(replica.getEngine().verifyTableName("t")));
                }
            }
            // after a plain restart the replica keeps the id it minted
            try (TestServerMain replica = start(r)) {
                awaitRole(replica, EngineRole.REPLICA);
                Assert.assertEquals(Files.readString(Paths.get(r, ".replication", "instance")).trim(),
                        replica.getReplicationService().getInstanceId());
            }
        });
    }

    @Test
    public void testCheckpointCopyRecoversLostPrimary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String d = nodeRoot("d");
            writeConfiguration(p, "primary");
            writeConfiguration(d, "primary");
            allowPromotion(d);
            try (TestServerMain primary = start(p)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitApplied(primary, "t");
                // a volume snapshot taken under CHECKPOINT
                primary.execute("CHECKPOINT CREATE");
                copyInstall(p, d, "db", TableUtils.CHECKPOINT_DIRECTORY);
                primary.execute("CHECKPOINT RELEASE");
                // uploaded after the snapshot, then the primary is lost without releasing its lease
                primary.execute("INSERT INTO t VALUES ('2024-01-02', 2), ('2024-01-03', 3)");
                primary.execute("CREATE TABLE u (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO u VALUES ('2024-01-01', 10)");
                awaitUploaded(primary, "t");
                awaitUploaded(primary, "u");
                primary.getReplicationService().crashForTesting();
            }
            Files.createFile(Paths.get(d, TableUtils.RESTORE_FROM_CHECKPOINT_TRIGGER_FILE_NAME));
            try (TestServerMain recovered = start(d)) {
                // rolls forward from the store, then takes the expired lease and opens writes
                awaitRole(recovered, EngineRole.PRIMARY);
                awaitApplied(recovered, "t");
                awaitApplied(recovered, "u");
                recovered.assertSql("SELECT count() n, max(v) m FROM t", "n\tm\n3\t3\n");
                recovered.assertSql("SELECT v FROM u", "v\n10\n");
                recovered.execute("INSERT INTO t VALUES ('2024-01-04', 4)");
                awaitApplied(recovered, "t");
                recovered.assertSql("SELECT count() n FROM t", "n\n4\n");
                Assert.assertNull(((ReplicatedCairoEngine) recovered.getEngine()).getFenceReason());
            }
        });
    }

    private static void copyInstall(String from, String to, String... dirs) throws IOException {
        for (String dir : dirs) {
            final Path src = Paths.get(from, dir);
            if (!Files.exists(src)) {
                continue;
            }
            final Path dst = Paths.get(to, dir);
            try (Stream<Path> files = Files.walk(src)) {
                for (Path f : (Iterable<Path>) files::iterator) {
                    final Path target = dst.resolve(src.relativize(f).toString());
                    if (Files.isDirectory(f)) {
                        Files.createDirectories(target);
                    } else {
                        Files.copy(f, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
            }
        }
    }
}
