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

import io.questdb.cairo.CairoException;
import io.questdb.replication.ReplicatedCairoEngine.EngineRole;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * What clients see on a replica: queries work over PG-wire and HTTP, and every write and DDL is refused.
 */
public class ReplicationClientAccessTest extends AbstractReplicationTest {
    private static final String[] WRITES = {
            "INSERT INTO t VALUES ('2024-01-02', 2)",
            "UPDATE t SET v = 0",
            "ALTER TABLE t ADD COLUMN x INT",
            "TRUNCATE TABLE t",
            "RENAME TABLE t TO u",
            "DROP TABLE t",
            "CREATE TABLE t2 (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL",
            "CREATE TABLE t3 AS (SELECT * FROM t)",
            "CREATE VIEW v AS SELECT ts FROM t",
            "CREATE MATERIALIZED VIEW mv AS SELECT ts, max(v) m FROM t SAMPLE BY 1d"
    };

    @Test
    public void testReplicaRefusesClientWrites() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String p = nodeRoot("p");
            final String r = nodeRoot("r");
            writeConfiguration(p, "primary");
            writeConfiguration(r, "replica");
            try (TestServerMain primary = start(p); TestServerMain replica = start(r)) {
                awaitRole(primary, EngineRole.PRIMARY);
                primary.execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primary.execute("INSERT INTO t VALUES ('2024-01-01', 1)");
                awaitSame(primary, replica, "t");
                awaitRole(replica, EngineRole.REPLICA);

                final HttpClient http = HttpClient.newHttpClient();
                final int httpPort = replica.getHttpServerPort();
                try (Connection pg = getConnection("admin", "quest", replica.getPgWireServerPort());
                     Statement stmt = pg.createStatement()) {
                    for (String sql : WRITES) {
                        try {
                            stmt.execute(sql);
                            Assert.fail("PG-wire accepted on a replica: " + sql);
                        } catch (SQLException e) {
                            TestUtils.assertContains(sql, e.getMessage(), CairoException.READ_ONLY_ACCESS_MESSAGE);
                        }
                        final String body = exec(http, httpPort, sql);
                        Assert.assertTrue("HTTP accepted on a replica: " + sql + " -> " + body, body.contains("\"error\""));
                        TestUtils.assertContains(sql, body, CairoException.READ_ONLY_ACCESS_MESSAGE);
                    }
                    // nothing changed, and queries work over both protocols
                    try (ResultSet rs = stmt.executeQuery("SELECT count() n FROM t")) {
                        Assert.assertTrue(rs.next());
                        Assert.assertEquals(1, rs.getLong(1));
                    }
                    TestUtils.assertContains(exec(http, httpPort, "SELECT count() n FROM t"), "\"dataset\":[[1]]");
                    for (String name : new String[]{"u", "t2", "t3", "v", "mv"}) {
                        Assert.assertNull(name, replica.getEngine().getTableTokenIfExists(name));
                    }
                }
            }
        });
    }

    private static String exec(HttpClient http, int port, String sql) throws Exception {
        final HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/exec?query="
                + URLEncoder.encode(sql, StandardCharsets.UTF_8))).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }
}
