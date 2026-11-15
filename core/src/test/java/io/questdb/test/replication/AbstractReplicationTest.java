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

import io.questdb.Bootstrap;
import io.questdb.PropertyKey;
import io.questdb.replication.ReplicatedCairoEngine;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base of the replication end-to-end tests: servers in one JVM, each in its own root with its own
 * random loopback ports, sharing a filesystem object store.
 */
public abstract class AbstractReplicationTest extends AbstractBootstrapTest {
    protected String storeRoot;

    protected static void awaitApplied(TestServerMain server, String table) throws Exception {
        TestUtils.assertEventually(
                () -> server.assertSql("SELECT writerTxn = sequencerTxn ok FROM wal_tables() WHERE name = '" + table + "'", "ok\ntrue\n"),
                60
        );
    }

    /**
     * Waits until the primary's object store holds every transaction of the table.
     */
    protected static void awaitUploaded(TestServerMain primary, String table) throws Exception {
        TestUtils.assertEventually(() -> {
            final io.questdb.cairo.TableToken token = primary.getEngine().verifyTableName(table);
            Assert.assertEquals(primary.getEngine().getTableSequencerAPI().lastTxn(token),
                    primary.getReplicationService().getUploadedTxn(token));
        }, 60);
    }

    protected static void awaitRole(TestServerMain server, ReplicatedCairoEngine.EngineRole role) throws Exception {
        TestUtils.assertEventually(() -> Assert.assertEquals(role, role(server)), 60);
    }

    protected static void awaitSame(TestServerMain primary, TestServerMain replica, String table) throws Exception {
        awaitApplied(primary, table);
        final String expected = select(primary, table);
        TestUtils.assertEventually(() -> Assert.assertEquals(expected, select(replica, table)), 60);
    }

    protected static int freePort() {
        try (var socket = new java.net.ServerSocket(0, 0, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        } catch (java.io.IOException e) {
            throw new AssertionError("could not obtain a free port", e);
        }
    }

    protected static ReplicatedCairoEngine.EngineRole role(TestServerMain server) {
        return ((ReplicatedCairoEngine) server.getEngine()).getRole();
    }

    protected static String select(TestServerMain server, String table) {
        final StringSink sink = new StringSink();
        try {
            TestUtils.printSql(server.getEngine(), server.getSqlExecutionContext(), "SELECT * FROM '" + table + "' ORDER BY ts", sink);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return sink.toString();
    }

    protected static TestServerMain start(String serverRoot) {
        final TestServerMain server = new TestServerMain(Bootstrap.getServerMainArgs(serverRoot));
        try {
            server.start();
            return server;
        } catch (Throwable th) {
            server.close();
            throw th;
        }
    }

    /**
     * Copies a directory tree, replacing the target.
     */
    protected static void copyTree(String from, String to) throws Exception {
        TestUtils.removeTestPath(to);
        final java.nio.file.Path src = java.nio.file.Paths.get(from);
        final java.nio.file.Path dst = java.nio.file.Paths.get(to);
        try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(src)) {
            for (java.nio.file.Path f : (Iterable<java.nio.file.Path>) files::iterator) {
                final java.nio.file.Path target = dst.resolve(src.relativize(f).toString());
                if (java.nio.file.Files.isDirectory(f)) {
                    java.nio.file.Files.createDirectories(target);
                } else {
                    java.nio.file.Files.copy(f, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    /**
     * Lets the node take over an object store it does not own, on its next start as primary.
     */
    protected static void allowPromotion(String serverRoot) throws Exception {
        TestUtils.createTestPath(serverRoot);
        final java.nio.file.Path marker = java.nio.file.Paths.get(serverRoot, io.questdb.replication.ReplicationService.MIGRATE_PRIMARY_FILE);
        if (!java.nio.file.Files.exists(marker)) {
            java.nio.file.Files.createFile(marker);
        }
    }

    /**
     * The index the object store holds now.
     */
    protected io.questdb.replication.codec.ReplicationIndex readIndex() {
        try (io.questdb.replication.store.FsObjectStore store = new io.questdb.replication.store.FsObjectStore(
                io.questdb.replication.store.ObjectStoreSettings.parse("fs::root=" + storeRoot + ';'),
                new io.questdb.replication.ReplicationConfigurationImpl());
             io.questdb.replication.IndexClient client = new io.questdb.replication.IndexClient(store, true)) {
            return client.read();
        }
    }

    /**
     * Starts a node whose object store runs every request past the hook.
     */
    protected static TestServerMain start(String serverRoot, FaultyObjectStore.Hook hook) {
        final TestServerMain server = new TestServerMain(Bootstrap.getServerMainArgs(serverRoot)) {
            @Override
            protected io.questdb.replication.store.ObjectStore newReplicationObjectStore(io.questdb.replication.ReplicationConfiguration replicationConfig) {
                return new FaultyObjectStore(super.newReplicationObjectStore(replicationConfig), hook);
            }
        };
        try {
            server.start();
            return server;
        } catch (Throwable th) {
            server.close();
            throw th;
        }
    }

    protected String nodeRoot(String name) {
        return root + File.separator + name;
    }

    @Before
    public void setUpStore() {
        storeRoot = root + File.separator + "store";
        TestUtils.removeTestPath(storeRoot);
        new File(storeRoot).mkdirs();
    }

    /**
     * Writes server.conf for a node; {@code role} may be none, primary or replica.
     */
    protected void writeConfiguration(String serverRoot, String role, String... extra) throws Exception {
        final int httpPort = freePort();
        final int httpMinPort = freePort();
        final int pgPort = freePort();
        final int ilpPort = freePort();
        TestUtils.createTestPath(serverRoot);
        final List<String> settings = new ArrayList<>();
        Collections.addAll(
                settings,
                PropertyKey.HTTP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + httpPort,
                PropertyKey.HTTP_MIN_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + httpMinPort,
                PropertyKey.PG_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + pgPort,
                PropertyKey.LINE_TCP_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort,
                PropertyKey.LINE_UDP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort,
                PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getPropertyPath() + "=true",
                PropertyKey.REPLICATION_ROLE.getPropertyPath() + '=' + role,
                PropertyKey.REPLICATION_OBJECT_STORE.getPropertyPath() + "=fs::root=" + storeRoot + ';',
                PropertyKey.REPLICATION_PRIMARY_THROTTLE_WINDOW_DURATION.getPropertyPath() + "=100",
                PropertyKey.REPLICATION_PRIMARY_KEEPALIVE_INTERVAL.getPropertyPath() + "=1000",
                PropertyKey.REPLICATION_REPLICA_POLL_INTERVAL.getPropertyPath() + "=50"
        );
        Collections.addAll(settings, extra);
        createDummyConfiguration(httpPort, httpMinPort, pgPort, ilpPort, serverRoot, settings.toArray(new String[0]));
    }
}
