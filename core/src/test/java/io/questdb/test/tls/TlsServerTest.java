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

package io.questdb.test.tls;

import io.questdb.Bootstrap;
import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.TlsBootstrapConfiguration;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

/**
 * Starts a real server through {@link TlsBootstrapConfiguration}, the way {@code TlsServerMain} does, and talks to
 * it over every TLS listener with real clients.
 */
public class TlsServerTest extends AbstractBootstrapTest {
    private static final long AWAIT_NANOS = 30_000_000_000L;
    private static String certPath;
    private static int httpMinPort;
    private static int httpPort;
    private static int ilpPort;
    private static String keyPath;
    private static boolean opensslAvailable;
    private static int pgPort;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        final var certDir = temp.newFolder("tls-certs");
        final var certFile = new File(certDir, "server.crt");
        final var keyFile = new File(certDir, "server.key");
        try {
            final var proc = new ProcessBuilder(
                    "openssl", "req", "-x509", "-newkey", "rsa:2048",
                    "-keyout", keyFile.getAbsolutePath(),
                    "-out", certFile.getAbsolutePath(),
                    "-days", "1", "-nodes",
                    "-subj", "/CN=localhost"
            ).redirectErrorStream(true).start();
            opensslAvailable = proc.waitFor() == 0;
        } catch (Exception e) {
            opensslAvailable = false;
        }
        certPath = certFile.getAbsolutePath();
        keyPath = keyFile.getAbsolutePath();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // OS-assigned free ports, not the base class's fixed range: a fixed port can collide with another
        // listener on the host (a tunnel to a real server, for one) and the test would talk to that instead
        httpPort = freePort();
        httpMinPort = freePort();
        pgPort = freePort();
        ilpPort = freePort();
        writeConfiguration(
                "tls.enabled=true",
                "tls.cert.path=" + certPath,
                "tls.private.key.path=" + keyPath,
                // plaintext health endpoint, as deployments use it
                "http.min.tls.enabled=false",
                // one network worker: a client that holds it shows up as latency for every other client
                "shared.network.worker.count=1",
                "cairo.wal.enabled.default=true"
        );
        dbPath.parent().$();
    }

    @Test
    public void testHttpsQuery() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                TestUtils.assertContains(httpsGet("/exec?query=SELECT%2042%20AS%20val"), "\"dataset\":[[42]]");
            }
        });
    }

    @Test
    public void testIlpOverHttps() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                try (Sender sender = Sender.fromConfig("https::addr=127.0.0.1:" + httpPort + ";tls_verify=unsafe_off;")) {
                    writeRows(sender, "ilp_https");
                }
                awaitCount("ilp_https", 10);
            }
        });
    }

    @Test
    public void testIlpOverTcpTls() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                try (Sender sender = Sender.fromConfig("tcps::addr=127.0.0.1:" + ilpPort + ";tls_verify=unsafe_off;")) {
                    writeRows(sender, "ilp_tcps");
                }
                awaitCount("ilp_tcps", 10);
            }
        });
    }

    @Test
    public void testPgOverTlsAndPlaintextRefused() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                try (Connection connection = tlsPgConnection();
                     Statement statement = connection.createStatement();
                     ResultSet rs = statement.executeQuery("SELECT 7 AS seven")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(7, rs.getInt(1));
                }
                try (Connection ignored2 = DriverManager.getConnection(
                        "jdbc:postgresql://127.0.0.1:" + pgPort + "/qdb?sslmode=disable", "admin", "quest"
                )) {
                    Assert.fail("plaintext PGWire must be refused on a TLS listener");
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "request SSL message expected");
                }
            }
        });
    }

    @Test
    public void testQwpBrowserOriginOverNativeTls() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                // native TLS makes the connection secure, so the server's own https origin is same-origin
                TestUtils.assertContains(
                        qwpUpgradeStatusLine("https://localhost:" + httpPort),
                        "101 Switching Protocols"
                );
                // an http origin does not match a TLS connection: a browser upgrade from it is cross-origin
                final var refused = qwpUpgradeStatusLine("http://localhost:" + httpPort);
                Assert.assertFalse("cross-scheme origin must be refused: " + refused, refused.contains("101"));
            }
        });
    }

    @Test
    public void testQwpOverTls() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                try (Sender sender = Sender.fromConfig("wss::addr=127.0.0.1:" + httpPort + ";tls_verify=unsafe_off;")) {
                    writeRows(sender, "qwp_wss");
                }
                awaitCount("qwp_wss", 10);
            }
        });
    }

    @Test
    public void testStalledHandshakesDoNotBlockOtherClients() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            try (TestServerMain ignored = startTlsServer()) {
                final var stalled = new ArrayList<java.net.Socket>();
                try {
                    // PGWire clients that ask for TLS and then never send a ClientHello
                    for (var i = 0; i < 3; i++) {
                        final var socket = new java.net.Socket("127.0.0.1", pgPort);
                        stalled.add(socket);
                        final OutputStream out = socket.getOutputStream();
                        out.write(new byte[]{0, 0, 0, 8, 0x04, (byte) 0xd2, 0x16, 0x2f});
                        out.flush();
                        final InputStream in = socket.getInputStream();
                        Assert.assertEquals('S', in.read());
                    }
                    // HTTPS clients that send the first byte of a TLS record and stop
                    for (var i = 0; i < 3; i++) {
                        final var socket = new java.net.Socket("127.0.0.1", httpPort);
                        stalled.add(socket);
                        socket.getOutputStream().write(0x16);
                        socket.getOutputStream().flush();
                    }
                    // Before the handshake became non-blocking, each stalled client held the single network worker
                    // for the whole handshake spin (seconds per client), and these calls queued behind them.
                    final var start = System.nanoTime();
                    try (Connection connection = tlsPgConnection();
                         Statement statement = connection.createStatement();
                         ResultSet rs = statement.executeQuery("SELECT 1")) {
                        Assert.assertTrue(rs.next());
                    }
                    TestUtils.assertContains(httpsGet("/exec?query=SELECT%201"), "\"dataset\":[[1]]");
                    final var elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                    Assert.assertTrue(
                            "stalled TLS handshakes delayed other clients by " + elapsedMillis + "ms",
                            elapsedMillis < 5_000
                    );
                } finally {
                    for (var socket : stalled) {
                        socket.close();
                    }
                }
                // the server keeps serving after the stalled clients leave
                TestUtils.assertContains(httpsGet("/exec?query=SELECT%202"), "\"dataset\":[[2]]");
            }
        });
    }

    @Test
    public void testTlsConfiguredThroughEnvironment() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        assertMemoryLeak(() -> {
            // server.conf without TLS settings: the environment enables TLS for the HTTP listener only
            writeConfiguration("shared.network.worker.count=1");
            final var env = new java.util.HashMap<String, String>();
            env.put("QDB_HTTP_TLS_ENABLED", "true");
            env.put("QDB_HTTP_TLS_CERT_PATH", certPath);
            env.put("QDB_HTTP_TLS_PRIVATE_KEY_PATH", keyPath);
            final var bootstrapConfiguration = new TlsBootstrapConfiguration() {
                @Override
                public java.util.Map<String, String> getEnv() {
                    return env;
                }
            };
            try (TestServerMain serverMain = new TestServerMain(new Bootstrap(bootstrapConfiguration, getServerMainArgs()))) {
                serverMain.start();
                TestUtils.assertContains(httpsGet("/exec?query=SELECT%203"), "\"dataset\":[[3]]");
                // PGWire stays plaintext
                try (Connection connection = DriverManager.getConnection(
                        "jdbc:postgresql://127.0.0.1:" + pgPort + "/qdb?sslmode=disable", "admin", "quest");
                     Statement statement = connection.createStatement();
                     ResultSet rs = statement.executeQuery("SELECT 4")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(4, rs.getInt(1));
                }
            }
        });
    }

    private static void awaitCount(String table, long expected) throws Exception {
        final var deadline = System.nanoTime() + AWAIT_NANOS;
        long count = -1;
        while (System.nanoTime() < deadline) {
            try (Connection connection = tlsPgConnection();
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT count() FROM '" + table + "'")) {
                Assert.assertTrue(rs.next());
                count = rs.getLong(1);
                if (count == expected) {
                    return;
                }
            } catch (SQLException e) {
                // the table does not exist yet
            }
            Thread.sleep(50);
        }
        Assert.fail("expected " + expected + " rows in " + table + ", got " + count);
    }

    private static int freePort() {
        try (var socket = new java.net.ServerSocket(0, 0, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        } catch (java.io.IOException e) {
            throw new AssertionError("could not obtain a free port", e);
        }
    }

    private static String httpsGet(String pathAndQuery) throws Exception {
        final var connection = (HttpsURLConnection) URI.create("https://127.0.0.1:" + httpPort + pathAndQuery).toURL().openConnection();
        try {
            connection.setSSLSocketFactory(trustAllContext().getSocketFactory());
            connection.setHostnameVerifier((hostname, session) -> true);
            connection.setConnectTimeout(10_000);
            connection.setReadTimeout(10_000);
            try (InputStream in = connection.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static String qwpUpgradeStatusLine(String origin) throws Exception {
        try (var socket = (javax.net.ssl.SSLSocket) trustAllContext().getSocketFactory().createSocket("127.0.0.1", httpPort)) {
            socket.setSoTimeout(10_000);
            final var request = "GET /write/v4 HTTP/1.1\r\n"
                    + "Host: localhost:" + httpPort + "\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Sec-WebSocket-Version: 13\r\n"
                    + "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
                    + "Origin: " + origin + "\r\n"
                    + "\r\n";
            socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            final var line = new StringBuilder();
            final InputStream in = socket.getInputStream();
            for (int b = in.read(); b >= 0 && b != '\n'; b = in.read()) {
                line.append((char) b);
            }
            return line.toString();
        }
    }

    private static TestServerMain startTlsServer() {
        final var serverMain = new TestServerMain(new Bootstrap(new TlsBootstrapConfiguration(), getServerMainArgs()));
        try {
            serverMain.start();
            return serverMain;
        } catch (Throwable th) {
            serverMain.close();
            throw th;
        }
    }

    private static Connection tlsPgConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:" + pgPort + "/qdb?sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory",
                "admin",
                "quest"
        );
    }

    private static SSLContext trustAllContext() throws Exception {
        final var context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[]{new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }}, new SecureRandom());
        return context;
    }

    /**
     * Writes server.conf with this test's ports, every listener bound to the loopback address only.
     */
    private static void writeConfiguration(String... extra) {
        final var settings = new String[extra.length + 5];
        System.arraycopy(extra, 0, settings, 0, extra.length);
        settings[extra.length] = PropertyKey.HTTP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + httpPort;
        settings[extra.length + 1] = PropertyKey.HTTP_MIN_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + httpMinPort;
        settings[extra.length + 2] = PropertyKey.PG_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + pgPort;
        settings[extra.length + 3] = PropertyKey.LINE_TCP_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort;
        settings[extra.length + 4] = PropertyKey.LINE_UDP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort;
        TestUtils.unchecked(() -> createDummyConfiguration(httpPort, httpMinPort, pgPort, ilpPort, root, settings));
    }

    private static void writeRows(Sender sender, String table) {
        for (var i = 0; i < 10; i++) {
            sender.table(table)
                    .symbol("sym", "s" + (i % 3))
                    .longColumn("id", i)
                    .doubleColumn("value", i * 1.5)
                    .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
        }
        sender.flush();
    }
}
