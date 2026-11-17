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
import io.questdb.BootstrapConfiguration;
import io.questdb.DynamicPropServerConfiguration;
import io.questdb.FactoryProvider;
import io.questdb.FreeOnExit;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.client.Sender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.SocketFactory;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.ServerTlsConfiguration;
import io.questdb.tls.TlsBootstrapConfiguration;
import io.questdb.tls.TlsFactoryProvider;
import io.questdb.tls.TlsFactoryProviderFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSocket;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fuzzes the TLS listeners of a real server, started through the production wiring of
 * {@link TlsBootstrapConfiguration}: honest clients of every protocol run next to hostile ones, and every honest
 * result must come out as plain arithmetic says. The in-process test, {@link JavaTlsServerSocketFuzzTest}, covers
 * the socket itself; this one covers what only a server has: the IO dispatchers, pooled connection contexts and
 * the places where each protocol starts TLS.
 * <p>
 * Every listener binds the loopback address on a port the OS picks. A test socket factory puts a facade under
 * the TLS sockets that cuts their raw reads and writes short and reports a full socket now and then, so the
 * ciphertext is fragmented the way a slow network fragments it. The debug chunk-size settings fragment the
 * plaintext side on top of that.
 * <p>
 * {@code -Dquestdb.fuzz.tls.iterations=N} scales the work per client; {@code -Dfuzz.s0=... -Dfuzz.s1=...} replays
 * the draws of a run, though not the interleaving of its threads.
 */
public class TlsServerFuzzTest extends AbstractBootstrapTest {
    private static final long BASE_TIMESTAMP_MICROS = 1_700_000_000_000_000L;
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    private static final int IO_TIMEOUT_MILLIS = 60_000;
    private static final int ITERATIONS = TlsFuzzSupport.iterations(4);
    private static final long JOIN_TIMEOUT_MILLIS = 600_000;
    private static final int LISTENER_HTTP = 0;
    private static final int LISTENER_ILP = 2;
    private static final int LISTENER_PG = 1;
    private static final Log LOG = LogFactory.getLog(TlsServerFuzzTest.class);
    // an address literal: no name lookup decides where the test connects
    private static final String LOOPBACK = "127.0.0.1";
    // far more than a server has to say to a client that misbehaved
    private static final long MAX_DRAINED_BYTES = 16 * 1024 * 1024;
    private static final int MAX_ROWS = 5_000;
    private static final String TLS_12 = "TLSv1.2";
    private static final String TLS_13 = "TLSv1.3";
    private static String certPath;
    private static String keyPath;
    private static SSLContext trustAll;
    private int ilpPort;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        final File certDir = temp.newFolder("tls-fuzz-certs");
        final File certFile = new File(certDir, "server.crt");
        final File keyFile = new File(certDir, "server.key");
        // PEM files from the test keystore: the server reads them through PemReader, as in production
        TlsFuzzSupport.writePemFiles(certFile, keyFile);
        certPath = certFile.getAbsolutePath();
        keyPath = keyFile.getAbsolutePath();
        // one context for every client, so later connections resume the sessions of earlier ones
        trustAll = TlsFuzzSupport.newTrustAllContext();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // The line receiver does not report the port it bound, so it cannot bind port 0. The OS picks a free
        // loopback port here and the server binds it right after; a fixed port could belong to anything.
        ilpPort = probeFreePort();
    }

    /**
     * Honest clients of every protocol and hostile clients work the TLS listeners at the same time. Every honest
     * result is checked against values computed here; afterwards every listener still serves, and the server has
     * freed the TLS buffers of every connection.
     */
    @Test
    public void testConcurrentClients() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final FragmentingNetworkFacade facade = FragmentingNetworkFacade.random(rnd);
            final ObjList<String> settings = new ObjList<>();
            if (rnd.nextBoolean()) {
                // one network worker: a connection that captures it starves every other client
                settings.add(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath() + "=1");
            }
            if (rnd.nextBoolean()) {
                // these cap the lengths the protocol handlers pass to recv() and send() of the TLS socket
                settings.add(PropertyKey.DEBUG_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getPropertyPath() + '=' + (1 + rnd.nextInt(500)));
                settings.add(PropertyKey.DEBUG_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getPropertyPath() + '=' + (64 + rnd.nextInt(437)));
            }
            writeConfiguration(settings);
            try (TestServerMain serverMain = startServer(facade)) {
                final int httpPort = serverMain.getHttpServerPort();
                final int pgPort = serverMain.getPgWireServerPort();
                final long tlsMemBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);

                // every client gets its own generator before any thread starts
                final ObjList<NamedTask> tasks = new ObjList<>();
                final ObjList<IngestPlan> ingestPlans = new ObjList<>();
                final int honestCount = 4 + rnd.nextInt(5);
                for (int i = 0; i < honestCount; i++) {
                    final Rnd clientRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                    final int requests = 1 + clientRnd.nextInt(2 * ITERATIONS);
                    switch (i % 4) {
                        case 0 -> tasks.add(new NamedTask("PGWire client " + i, () -> pgQueries(pgPort, clientRnd, requests)));
                        case 1 -> tasks.add(new NamedTask("HTTPS client " + i, () -> httpsQueries(httpPort, clientRnd, requests)));
                        case 2 -> {
                            // line protocol over HTTPS, line protocol over TCP with TLS, or QWP over a secure WebSocket
                            final String transport = switch (clientRnd.nextInt(3)) {
                                case 0 -> "https";
                                case 1 -> "tcps";
                                default -> "wss";
                            };
                            final IngestPlan plan = new IngestPlan(
                                    "tls_fuzz_" + i,
                                    transport,
                                    200 + clientRnd.nextInt(1_800),
                                    1 + clientRnd.nextInt(3)
                            );
                            ingestPlans.add(plan);
                            tasks.add(new NamedTask("ingestion client " + i + " over " + plan.transport, () -> ingest(plan, httpPort, ilpPort, pgPort)));
                        }
                        default -> tasks.add(new NamedTask("slow HTTPS client " + i, () -> slowHttpsQueries(httpPort, clientRnd, requests)));
                    }
                }
                for (int i = 0; i < 3; i++) {
                    final Rnd clientRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                    final int listener = i;
                    final int port = listener == LISTENER_HTTP ? httpPort : listener == LISTENER_PG ? pgPort : ilpPort;
                    tasks.add(new NamedTask("hostile client on listener " + listener, () -> {
                        for (int k = 0, n = 6 * ITERATIONS; k < n; k++) {
                            hostileConnection(port, listener, clientRnd);
                        }
                    }));
                }
                runConcurrently(tasks);

                // what the ingestion clients sent is all there, and nothing else is
                for (int i = 0, n = ingestPlans.size(); i < n; i++) {
                    final IngestPlan plan = ingestPlans.getQuick(i);
                    awaitTable(pgPort, plan.table, plan.rows);
                }
                assertListenersServe(httpPort, pgPort, ilpPort, "tls_fuzz_after");
                assertTlsBuffersFreed(tlsMemBaseline);
                LOG.info().$("TLS server fuzz [facade=").$(facade.toString()).I$();
            }
        });
    }

    /**
     * Nothing but hostile connections, one after another, on every TLS listener. Each listener must go on
     * serving, and the server must let go of every hostile connection and of its TLS buffers.
     */
    @Test
    public void testListenersSurviveHostileClients() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<String> settings = new ObjList<>();
            if (rnd.nextBoolean()) {
                settings.add(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath() + "=1");
            }
            writeConfiguration(settings);
            try (TestServerMain serverMain = startServer(FragmentingNetworkFacade.random(rnd))) {
                final int httpPort = serverMain.getHttpServerPort();
                final int pgPort = serverMain.getPgWireServerPort();
                final long tlsMemBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
                for (int i = 0, n = 24 * ITERATIONS; i < n; i++) {
                    final int listener = i % 3;
                    hostileConnection(listener == LISTENER_HTTP ? httpPort : listener == LISTENER_PG ? pgPort : ilpPort, listener, rnd);
                    if (rnd.nextInt(8) == 0) {
                        // a well-behaved client in between gets its answer
                        final int rows = 1 + rnd.nextInt(MAX_ROWS);
                        Assert.assertEquals(expectedCsv(rows), httpsGet(httpPort, expPath(rows)));
                    }
                }
                assertListenersServe(httpPort, pgPort, ilpPort, "tls_fuzz_survivor");
                assertTlsBuffersFreed(tlsMemBaseline);
            }
        });
    }

    /**
     * With keep-alive off the server disconnects as soon as it has handed the last bytes of a response to the
     * socket. A plain socket leaves those bytes to the kernel, which delivers them; the TLS socket must not
     * lose what it took either, also when the transport takes its ciphertext only piece by piece.
     */
    @Test
    public void testResponseArrivesInFullWithKeepAliveOff() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final ObjList<String> settings = new ObjList<>();
            settings.add(PropertyKey.HTTP_SERVER_KEEP_ALIVE.getPropertyPath() + "=false");
            writeConfiguration(settings);
            final FragmentingNetworkFacade facade = FragmentingNetworkFacade.shortWrites(rnd);
            try (TestServerMain serverMain = startServer(facade)) {
                final int httpPort = serverMain.getHttpServerPort();
                for (int i = 0, n = 2 * ITERATIONS; i < n; i++) {
                    final int rows = 1 + rnd.nextInt(4 * MAX_ROWS);
                    Assert.assertEquals(
                            "response to a query of " + rows + " rows",
                            expectedCsv(rows),
                            slowHttpsGet(httpPort, rnd, expPath(rows))
                    );
                }
                Assert.assertTrue("the transport never took a write only in part", facade.shortSends.get() > 0);
            }
        });
    }

    private static void assertListenersServe(int httpPort, int pgPort, int ilpPort, String table) throws Exception {
        TestUtils.assertContains(httpsGet(httpPort, "/exec?query=SELECT%2042%20AS%20val"), "\"dataset\":[[42]]");
        try (Connection connection = pgConnection(pgPort);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT 7 AS seven")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(7, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
        // the line receiver last: the query above proves that PGWire can check on it
        final IngestPlan plan = new IngestPlan(table, "tcps", 10, 1);
        ingest(plan, httpPort, ilpPort, pgPort);
        awaitTable(pgPort, plan.table, plan.rows);
    }

    /**
     * Once every client is gone the server has closed every connection, and a closed TLS socket holds no buffers.
     */
    private static void assertTlsBuffersFreed(long baseline) throws Exception {
        TestUtils.assertEventually(
                () -> Assert.assertEquals(
                        "TLS buffers of connections whose clients have left",
                        baseline,
                        Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS)
                ),
                120
        );
    }

    /**
     * Waits for the rows an ingestion client sent: ids 0 to {@code rows - 1}, each exactly once.
     */
    private static void awaitTable(int pgPort, String table, int rows) throws Exception {
        final long expectedSum = (long) rows * (rows - 1) / 2;
        TestUtils.assertEventually(() -> {
            try (Connection connection = pgConnection(pgPort);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT count(), sum(id) FROM '" + table + "'")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("rows in " + table, rows, rs.getLong(1));
                Assert.assertEquals("sum(id) in " + table, expectedSum, rs.getLong(2));
            } catch (SQLException e) {
                // the table is not there yet
                throw new AssertionError(table + " cannot be queried yet", e);
            }
        }, 120);
    }

    private static String expPath(int rows) {
        return "/exp?query=SELECT%20x%20FROM%20long_sequence(" + rows + ')';
    }

    /**
     * @return the CSV export of {@code SELECT x FROM long_sequence(rows)}
     */
    private static String expectedCsv(int rows) {
        final StringBuilder sink = new StringBuilder(8 * rows + 8);
        sink.append("\"x\"\r\n");
        for (int i = 1; i <= rows; i++) {
            sink.append(i).append("\r\n");
        }
        return sink.toString();
    }

    /**
     * @return the rows and the count of the JSON answer to {@code SELECT x FROM long_sequence(rows)}
     */
    private static String expectedDataset(int rows) {
        final StringBuilder sink = new StringBuilder(8 * rows + 32);
        sink.append("\"dataset\":[");
        for (int i = 1; i <= rows; i++) {
            if (i > 1) {
                sink.append(',');
            }
            sink.append('[').append(i).append(']');
        }
        return sink.append("],\"count\":").append(rows).toString();
    }

    /**
     * One connection of a client that misbehaves and then hangs up. Whatever it sent, the server must let go of
     * the connection: the client reads the end of the stream or a reset, never a timeout.
     */
    private static void hostileConnection(int port, int listener, Rnd rnd) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(LOOPBACK, port), IO_TIMEOUT_MILLIS);
            socket.setSoTimeout(IO_TIMEOUT_MILLIS);
            final InputStream in = socket.getInputStream();
            final OutputStream out = socket.getOutputStream();
            try {
                if (listener == LISTENER_PG && rnd.nextInt(4) != 0) {
                    // ask for TLS the PGWire way, then misbehave where the ClientHello belongs
                    out.write(TlsFuzzSupport.PG_SSL_REQUEST);
                    out.flush();
                    Assert.assertEquals("the answer to SSLRequest", 'S', in.read());
                }
                misbehave(socket, out, rnd);
                out.flush();
            } catch (SocketTimeoutException e) {
                throw e;
            } catch (IOException e) {
                // the server hung up first, which is its right, or the client's own engine gave up
            }
            try {
                // the client hangs up on every path: a server that still waits for it must see the end of the stream
                socket.shutdownOutput();
            } catch (IOException e) {
                // the connection is gone already
            }
            final byte[] sink = new byte[4096];
            try {
                // whatever the server still had to say; a server that never stops talking fails the test
                long drained = 0;
                for (int n = in.read(sink); n >= 0; n = in.read(sink)) {
                    drained += n;
                    if (drained > MAX_DRAINED_BYTES) {
                        throw new AssertionError("the server keeps sending to a hostile connection on listener " + listener);
                    }
                }
            } catch (SocketTimeoutException e) {
                throw e;
            } catch (IOException e) {
                // a reset ends the connection as well as an orderly close does
            }
        } catch (SocketTimeoutException e) {
            throw new AssertionError("the server neither answered nor closed a hostile connection on listener " + listener, e);
        } catch (IOException e) {
            throw new AssertionError("could not connect to listener " + listener, e);
        }
    }

    private static byte[] httpGet(String pathAndQuery) {
        return ("GET " + pathAndQuery + " HTTP/1.1\r\nHost: localhost\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static String httpsGet(int port, String pathAndQuery) throws Exception {
        return whileServerHasRoom(() -> httpsGetOnce(port, pathAndQuery));
    }

    private static String httpsGetOnce(int port, String pathAndQuery) throws Exception {
        final HttpsURLConnection connection = (HttpsURLConnection) URI.create("https://127.0.0.1:" + port + pathAndQuery).toURL().openConnection();
        try {
            connection.setSSLSocketFactory(trustAll.getSocketFactory());
            connection.setHostnameVerifier((hostname, session) -> true);
            connection.setConnectTimeout(IO_TIMEOUT_MILLIS);
            connection.setReadTimeout(IO_TIMEOUT_MILLIS);
            // the client closes the connection after the response, so the server sees every client leave
            connection.setRequestProperty("Connection", "close");
            final int status = connection.getResponseCode();
            if (status == 429) {
                throw new ServerBusyException();
            }
            Assert.assertEquals("status of " + pathAndQuery, 200, status);
            try (InputStream in = connection.getInputStream()) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static void httpsQueries(int httpPort, Rnd rnd, int requests) throws Exception {
        for (int i = 0; i < requests; i++) {
            final int rows = 1 + rnd.nextInt(MAX_ROWS);
            switch (rnd.nextInt(3)) {
                case 0 -> TestUtils.assertContains(
                        httpsGet(httpPort, "/exec?query=SELECT%20x%20FROM%20long_sequence(" + rows + ')'),
                        expectedDataset(rows)
                );
                case 1 -> Assert.assertEquals(expectedCsv(rows), httpsGet(httpPort, expPath(rows)));
                default -> Assert.assertEquals(expectedCsv(rows), keyUpdatesThenGet(httpPort, rnd, expPath(rows)));
            }
        }
    }

    /**
     * Sends ids 0 to {@code rows - 1}, each batch over a connection of its own.
     * <p>
     * Over TCP with TLS the client keeps its connection open until the server holds the batch. Its close never
     * reads the session ticket a TLS 1.3 server sends after the handshake, so the close resets the connection and
     * the kernel discards whatever the client had not sent yet: a client defect, not the server's.
     */
    private static void ingest(IngestPlan plan, int httpPort, int ilpPort, int pgPort) throws Exception {
        final int port = "tcps".equals(plan.transport) ? ilpPort : httpPort;
        final int batchSize = (plan.rows + plan.batches - 1) / plan.batches;
        // The HTTP transport sends a batch again when it gave up on the answer, and a batch the server had
        // committed all the same would then be in the table twice. Without retries such a batch fails its client.
        final String config = plan.transport + "::addr=" + LOOPBACK + ':' + port + ";tls_verify=unsafe_off;"
                + ("https".equals(plan.transport) ? "retry_timeout=0;" : "");
        for (int from = 0; from < plan.rows; from += batchSize) {
            final int to = Math.min(plan.rows, from + batchSize);
            try (Sender sender = Sender.fromConfig(config)) {
                for (int id = from; id < to; id++) {
                    sender.table(plan.table)
                            .symbol("sym", "s" + (id % 3))
                            .longColumn("id", id)
                            .at(BASE_TIMESTAMP_MICROS + id, ChronoUnit.MICROS);
                }
                sender.flush();
                if ("tcps".equals(plan.transport)) {
                    awaitTable(pgPort, plan.table, to);
                }
            }
        }
    }

    /**
     * A TLS 1.3 client that asks for a few key updates before its request. That is legal: the answer must come
     * and must be right.
     */
    private static String keyUpdatesThenGet(int port, Rnd rnd, String pathAndQuery) throws Exception {
        return whileServerHasRoom(() -> keyUpdatesThenGetOnce(port, rnd, pathAndQuery));
    }

    private static String keyUpdatesThenGetOnce(int port, Rnd rnd, String pathAndQuery) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(LOOPBACK, port), IO_TIMEOUT_MILLIS);
            socket.setSoTimeout(IO_TIMEOUT_MILLIS);
            final RawTlsClient tls = new RawTlsClient(socket, TLS_13);
            tls.handshake();
            final OutputStream out = socket.getOutputStream();
            for (int i = 0, n = 1 + rnd.nextInt(32); i < n; i++) {
                out.write(tls.startNewHandshake());
            }
            out.write(tls.wrap(httpGet(pathAndQuery)));
            out.flush();
            return readHttpBody(tls.plaintext(), rnd);
        }
    }

    /**
     * The ways a client misbehaves on a real connection: bytes that are not TLS, a handshake cut short, records
     * that are corrupt, replayed or incomplete, junk inside a good session, and a second handshake.
     */
    private static void misbehave(Socket socket, OutputStream out, Rnd rnd) throws IOException {
        switch (rnd.nextInt(8)) {
            case 0 -> out.write(TlsFuzzSupport.hostileFirstFlight(rnd));
            case 1 -> {
                // nothing at all: connect and leave
            }
            case 2 -> {
                // a ClientHello cut short
                final byte[] hello = new RawTlsClient(socket, null).wrapControl();
                out.write(hello, 0, 1 + rnd.nextInt(hello.length - 1));
            }
            case 3 -> {
                // a good handshake, then a record with one bit flipped
                final byte[] record = handshake(socket, rnd).wrap(httpGet(expPath(1)));
                record[rnd.nextInt(record.length)] ^= (byte) (1 << rnd.nextInt(8));
                out.write(record);
            }
            case 4 -> {
                // a good handshake, then the same record twice
                final byte[] record = handshake(socket, rnd).wrap(httpGet(expPath(1)));
                out.write(record);
                out.write(record);
            }
            case 5 -> {
                // a good handshake, then part of a record and nothing more
                final byte[] record = handshake(socket, rnd).wrap(httpGet(expPath(1)));
                out.write(record, 0, 1 + rnd.nextInt(record.length - 1));
            }
            case 6 -> {
                // a good handshake, then bytes that are a request of no protocol
                final byte[] junk = new byte[1 + rnd.nextInt(2_000)];
                TlsFuzzSupport.fill(rnd, junk);
                out.write(handshake(socket, rnd).wrap(junk));
            }
            default -> {
                // a second handshake on a live session: a renegotiation in TLS 1.2, key updates in TLS 1.3
                final boolean isTls13 = rnd.nextBoolean();
                final RawTlsClient tls = new RawTlsClient(socket, isTls13 ? TLS_13 : TLS_12);
                tls.handshake();
                for (int i = 0, n = isTls13 ? 1 + rnd.nextInt(64) : 1; i < n; i++) {
                    out.write(tls.startNewHandshake());
                }
            }
        }
    }

    private static RawTlsClient handshake(Socket socket, Rnd rnd) throws IOException {
        final RawTlsClient tls = new RawTlsClient(socket, switch (rnd.nextInt(3)) {
            case 0 -> TLS_13;
            case 1 -> TLS_12;
            default -> null;
        });
        tls.handshake();
        return tls;
    }

    private static Connection pgConnection(int port) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:" + port + "/qdb?sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory"
                        + "&connectTimeout=60&loginTimeout=60&socketTimeout=120",
                "admin",
                "quest"
        );
    }

    private static void pgQueries(int pgPort, Rnd rnd, int requests) throws Exception {
        for (int done = 0; done < requests; ) {
            // one connection serves a few queries, then the next one handshakes afresh
            try (Connection connection = pgConnection(pgPort)) {
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n && done < requests; i++, done++) {
                    final int rows = 1 + rnd.nextInt(4 * MAX_ROWS);
                    try (Statement statement = connection.createStatement();
                         ResultSet rs = statement.executeQuery("SELECT x FROM long_sequence(" + rows + ')')) {
                        long row = 0;
                        while (rs.next()) {
                            row++;
                            if (rs.getLong(1) != row) {
                                throw new AssertionError("row " + row + " of " + rows + " carries " + rs.getLong(1));
                            }
                        }
                        Assert.assertEquals("rows of a PGWire result", rows, row);
                    }
                }
            }
        }
    }

    /**
     * @return a suite the server's RSA certificate can serve under the protocol, or null for the defaults
     */
    private static String pickSuite(String[] enabledSuites, boolean isTls13, Rnd rnd) {
        final ObjList<String> fitting = new ObjList<>();
        for (String suite : enabledSuites) {
            final boolean isTls13Suite = suite.startsWith("TLS_AES_") || suite.startsWith("TLS_CHACHA20_");
            final boolean isEphemeralRsaSuite = suite.startsWith("TLS_ECDHE_RSA_WITH_") || suite.startsWith("TLS_DHE_RSA_WITH_");
            if (isTls13 ? isTls13Suite : isEphemeralRsaSuite) {
                fitting.add(suite);
            }
        }
        return fitting.size() == 0 || rnd.nextInt(3) == 0 ? null : fitting.getQuick(rnd.nextInt(fitting.size()));
    }

    private static int probeFreePort() {
        try (ServerSocket probe = new ServerSocket()) {
            probe.bind(new InetSocketAddress(LOOPBACK, 0));
            return probe.getLocalPort();
        } catch (IOException e) {
            throw new AssertionError("could not obtain a free loopback port", e);
        }
    }

    private static void readFully(InputStream in, ByteArrayOutputStream sink, int len, Rnd rnd, byte[] buffer) throws IOException {
        for (int remaining = len; remaining > 0; ) {
            // a slow reader: a few bytes at a time
            final int n = in.read(buffer, 0, Math.min(remaining, 1 + rnd.nextInt(buffer.length)));
            if (n < 0) {
                throw new EOFException("the response ended " + remaining + " bytes short of a chunk of " + len + ", after " + sink.size() + " bytes of the body");
            }
            sink.write(buffer, 0, n);
            remaining -= n;
        }
    }

    /**
     * Reads one HTTP response the slow way and returns its body. The framing says where the body ends, so a
     * response the server cut short fails here instead of passing for a shorter one.
     */
    private static String readHttpBody(InputStream in, Rnd rnd) throws IOException {
        final ByteArrayOutputStream head = new ByteArrayOutputStream();
        // how much of the blank line that ends the headers has been seen
        int matched = 0;
        while (matched < 4) {
            final int b = in.read();
            if (b < 0) {
                throw new EOFException("the response ended inside its headers: " + head);
            }
            head.write(b);
            if (b == (matched % 2 == 0 ? '\r' : '\n')) {
                matched++;
            } else {
                matched = b == '\r' ? 1 : 0;
            }
        }
        final String headers = head.toString(StandardCharsets.US_ASCII);
        if (headers.startsWith("HTTP/1.1 429 ")) {
            throw new ServerBusyException();
        }
        Assert.assertTrue("unexpected response: " + headers, headers.startsWith("HTTP/1.1 200 "));
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        final byte[] buffer = new byte[512];
        if (headers.contains("Transfer-Encoding: chunked")) {
            for (; ; ) {
                final int size = Integer.parseInt(readLine(in), 16);
                if (size == 0) {
                    // the blank line behind the last chunk
                    Assert.assertEquals("", readLine(in));
                    break;
                }
                readFully(in, body, size, rnd, buffer);
                Assert.assertEquals("", readLine(in));
            }
        } else {
            final int at = headers.indexOf("Content-Length: ");
            Assert.assertTrue("a response without a length: " + headers, at >= 0);
            final int length = Integer.parseInt(headers.substring(at + 16, headers.indexOf('\r', at)));
            readFully(in, body, length, rnd, buffer);
        }
        return body.toString(StandardCharsets.UTF_8);
    }

    private static String readLine(InputStream in) throws IOException {
        final StringBuilder line = new StringBuilder();
        for (; ; ) {
            final int b = in.read();
            if (b < 0) {
                throw new EOFException("the response ended inside a line: " + line);
            }
            if (b == '\n') {
                return line.toString();
            }
            if (b != '\r') {
                line.append((char) b);
            }
        }
    }

    /**
     * Starts every task on a barrier and waits for all of them. A task that fails does not stop the others;
     * the first failure is thrown and carries the rest.
     */
    private static void runConcurrently(ObjList<NamedTask> tasks) throws Exception {
        final int n = tasks.size();
        final CyclicBarrier barrier = new CyclicBarrier(n);
        final CountDownLatch done = new CountDownLatch(n);
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final ObjList<Thread> threads = new ObjList<>();
        for (int i = 0; i < n; i++) {
            final NamedTask task = tasks.getQuick(i);
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await(JOIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                    task.body.run();
                } catch (Throwable th) {
                    errors.add(new AssertionError(task.name + " failed: " + th, th));
                } finally {
                    done.countDown();
                }
            }, "tls-fuzz-client-" + i);
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
        }
        if (!done.await(JOIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            errors.add(new AssertionError("clients did not finish within " + JOIN_TIMEOUT_MILLIS + " ms"));
            for (int i = 0; i < n; i++) {
                threads.getQuick(i).interrupt();
            }
        }
        for (int i = 0; i < n; i++) {
            threads.getQuick(i).join(10_000);
        }
        final Throwable first = errors.poll();
        if (first != null) {
            for (Throwable other : errors) {
                first.addSuppressed(other);
            }
            throw new AssertionError(first.getMessage() + " [failures=" + (errors.size() + 1) + ']', first);
        }
    }

    /**
     * An HTTPS request over a JDK socket pinned to one protocol version and, mostly, one cipher suite. The
     * request trickles out and the response is read a few bytes at a time.
     */
    private static String slowHttpsGet(int port, Rnd rnd, String pathAndQuery) throws Exception {
        return whileServerHasRoom(() -> slowHttpsGetOnce(port, rnd, pathAndQuery));
    }

    private static String slowHttpsGetOnce(int port, Rnd rnd, String pathAndQuery) throws Exception {
        try (SSLSocket socket = (SSLSocket) trustAll.getSocketFactory().createSocket()) {
            socket.connect(new InetSocketAddress(LOOPBACK, port), IO_TIMEOUT_MILLIS);
            socket.setSoTimeout(IO_TIMEOUT_MILLIS);
            final boolean isTls13 = rnd.nextBoolean();
            final String suite = pickSuite(socket.getEnabledCipherSuites(), isTls13, rnd);
            socket.setEnabledProtocols(new String[]{isTls13 ? TLS_13 : TLS_12});
            if (suite != null) {
                socket.setEnabledCipherSuites(new String[]{suite});
            }
            socket.startHandshake();
            Assert.assertEquals(isTls13 ? TLS_13 : TLS_12, socket.getSession().getProtocol());
            if (suite != null) {
                Assert.assertEquals(suite, socket.getSession().getCipherSuite());
            }
            final byte[] request = httpGet(pathAndQuery);
            final OutputStream out = socket.getOutputStream();
            for (int sent = 0; sent < request.length; ) {
                final int n = Math.min(request.length - sent, 1 + rnd.nextInt(64));
                out.write(request, sent, n);
                out.flush();
                sent += n;
            }
            return readHttpBody(socket.getInputStream(), rnd);
        }
    }

    /**
     * The export endpoint admits a limited number of connections at once and answers 429 beyond that. The server
     * releases a connection's place a moment after the client has gone, so a client that follows many short-lived
     * ones can be refused for a while. It asks again; places that never came back would fail the test here.
     */
    private static String whileServerHasRoom(java.util.concurrent.Callable<String> request) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        for (; ; ) {
            try {
                return request.call();
            } catch (ServerBusyException e) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("the server kept answering 429 for a minute: connections it closed still hold their places");
                }
                io.questdb.std.Os.sleep(20);
            }
        }
    }

    private static void slowHttpsQueries(int httpPort, Rnd rnd, int requests) throws Exception {
        for (int i = 0; i < requests; i++) {
            final int rows = 1 + rnd.nextInt(MAX_ROWS);
            Assert.assertEquals(expectedCsv(rows), slowHttpsGet(httpPort, rnd, expPath(rows)));
        }
    }

    /**
     * Starts the server the way TlsServerMain does. With a facade, the TLS sockets of the HTTP, PGWire and line
     * listeners do their raw IO through it.
     */
    private static TestServerMain startServer(@Nullable NetworkFacade cipherFacade) {
        final BootstrapConfiguration configuration = cipherFacade != null
                ? new FragmentingTlsBootstrapConfiguration(cipherFacade)
                : new TlsBootstrapConfiguration();
        final TestServerMain serverMain = new TestServerMain(new Bootstrap(configuration, getServerMainArgs()));
        try {
            serverMain.start();
            return serverMain;
        } catch (Throwable th) {
            serverMain.close();
            throw th;
        }
    }

    /**
     * Writes server.conf: TLS on every listener, each listener on the loopback address, HTTP and PGWire on
     * ports the OS picks when the server binds them.
     */
    private void writeConfiguration(ObjList<String> extra) throws Exception {
        final ObjList<String> settings = new ObjList<>();
        settings.add(PropertyKey.TLS_ENABLED.getPropertyPath() + "=true");
        settings.add(PropertyKey.TLS_CERT_PATH.getPropertyPath() + '=' + certPath);
        settings.add(PropertyKey.TLS_PRIVATE_KEY_PATH.getPropertyPath() + '=' + keyPath);
        settings.add(PropertyKey.HTTP_MIN_ENABLED.getPropertyPath() + "=false");
        settings.add(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT.getPropertyPath() + "=true");
        settings.add(PropertyKey.HTTP_BIND_TO.getPropertyPath() + "=127.0.0.1:0");
        settings.add(PropertyKey.HTTP_MIN_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:0");
        settings.add(PropertyKey.PG_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:0");
        settings.add(PropertyKey.LINE_TCP_NET_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort);
        settings.add(PropertyKey.LINE_UDP_BIND_TO.getPropertyPath() + "=127.0.0.1:" + ilpPort);
        settings.addAll(extra);
        final String[] lines = new String[settings.size()];
        for (int i = 0; i < lines.length; i++) {
            lines[i] = settings.getQuick(i);
        }
        // the base class writes its own bind addresses first; a setting written later wins
        createDummyConfiguration(0, 0, 0, ilpPort, root, lines);
    }

    @FunctionalInterface
    private interface ClientTask {
        void run() throws Exception;
    }

    /**
     * Sits under the server's TLS sockets and cuts their raw IO short: a read returns fewer bytes than the
     * kernel holds, a write takes only part of the ciphertext, and now and then a write takes nothing, as a full
     * socket does. A read never reports 0 while the socket has bytes, which would tell the server it had drained it.
     */
    private static final class FragmentingNetworkFacade extends NetworkFacadeImpl {
        final AtomicLong shortRecvs = new AtomicLong();
        final AtomicLong shortSends = new AtomicLong();
        final AtomicLong wouldBlockSends = new AtomicLong();
        private final int maxRecvChunk;
        private final int maxSendChunk;
        private final int minSendChunk;
        // network workers call in from several threads: each draws from a generator of its own
        private final ThreadLocal<Rnd> rnd;
        private final int sendWouldBlockPct;
        private final AtomicLong threadCount = new AtomicLong();

        private FragmentingNetworkFacade(long seed0, long seed1, int maxRecvChunk, int minSendChunk, int maxSendChunk, int sendWouldBlockPct) {
            this.maxRecvChunk = maxRecvChunk;
            this.minSendChunk = minSendChunk;
            this.maxSendChunk = maxSendChunk;
            this.sendWouldBlockPct = sendWouldBlockPct;
            this.rnd = ThreadLocal.withInitial(() -> new Rnd(seed0 + threadCount.incrementAndGet(), seed1));
        }

        /**
         * Chunk sizes stay large enough for a response to get through in a few thousand writes: a write that
         * takes one byte costs a trip through the dispatcher for every byte.
         */
        static FragmentingNetworkFacade random(Rnd rnd) {
            final int maxRecvChunk = switch (rnd.nextInt(3)) {
                case 0 -> 1 + rnd.nextInt(16);
                case 1 -> 16 + rnd.nextInt(497);
                default -> Integer.MAX_VALUE;
            };
            final int sendClass = rnd.nextInt(3);
            return new FragmentingNetworkFacade(
                    rnd.nextLong(),
                    rnd.nextLong(),
                    maxRecvChunk,
                    sendClass == 0 ? 256 : 1_024,
                    sendClass == 0 ? 1_024 : sendClass == 1 ? 16_384 : Integer.MAX_VALUE,
                    rnd.nextBoolean() ? rnd.nextInt(21) : 0
            );
        }

        static FragmentingNetworkFacade shortWrites(Rnd rnd) {
            return new FragmentingNetworkFacade(rnd.nextLong(), rnd.nextLong(), Integer.MAX_VALUE, 256, 1_024, 10);
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (maxRecvChunk == Integer.MAX_VALUE || bufferLen <= 1) {
                return super.recvRaw(fd, buffer, bufferLen);
            }
            final int len = Math.min(bufferLen, 1 + rnd.get().nextInt(maxRecvChunk));
            if (len < bufferLen) {
                shortRecvs.incrementAndGet();
            }
            return super.recvRaw(fd, buffer, len);
        }

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            if (bufferLen <= 0) {
                return super.sendRaw(fd, buffer, bufferLen);
            }
            final Rnd rnd = this.rnd.get();
            if (sendWouldBlockPct > 0 && rnd.nextInt(100) < sendWouldBlockPct) {
                wouldBlockSends.incrementAndGet();
                return 0;
            }
            if (maxSendChunk == Integer.MAX_VALUE) {
                return super.sendRaw(fd, buffer, bufferLen);
            }
            final int len = Math.min(bufferLen, minSendChunk + rnd.nextInt(maxSendChunk - minSendChunk + 1));
            if (len < bufferLen) {
                shortSends.incrementAndGet();
            }
            return super.sendRaw(fd, buffer, len);
        }

        @Override
        public String toString() {
            return "maxRecvChunk=" + maxRecvChunk
                    + ", sendChunk=" + minSendChunk + ".." + maxSendChunk
                    + ", sendWouldBlockPct=" + sendWouldBlockPct
                    + ", shortRecvs=" + shortRecvs.get()
                    + ", shortSends=" + shortSends.get()
                    + ", wouldBlockSends=" + wouldBlockSends.get();
        }
    }

    /**
     * What TlsBootstrapConfiguration does, with one difference: the factory provider factory it installs puts
     * the test's facade under the TLS sockets.
     */
    private static final class FragmentingTlsBootstrapConfiguration extends PropBootstrapConfiguration {
        private final NetworkFacade cipherFacade;

        private FragmentingTlsBootstrapConfiguration(NetworkFacade cipherFacade) {
            this.cipherFacade = cipherFacade;
        }

        @Override
        public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
            final var properties = bootstrap.loadProperties();
            final var rootDir = bootstrap.getRootDirectory();
            final var env = getEnv();
            final ServerTlsConfiguration.GetStringFn getString = TlsBootstrapConfiguration::readSetting;
            final var globalTls = ServerTlsConfiguration.parse(
                    properties, env, rootDir,
                    PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                    ServerTlsConfiguration.DISABLED, getString
            );
            final var httpTls = ServerTlsConfiguration.parse(
                    properties, env, rootDir,
                    PropertyKey.HTTP_TLS_ENABLED, PropertyKey.HTTP_TLS_CERT_PATH, PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
                    globalTls, getString
            );
            final var httpMinTls = ServerTlsConfiguration.parse(
                    properties, env, rootDir,
                    PropertyKey.HTTP_MIN_TLS_ENABLED, PropertyKey.HTTP_MIN_TLS_CERT_PATH, PropertyKey.HTTP_MIN_TLS_PRIVATE_KEY_PATH,
                    globalTls, getString
            );
            final var lineTcpTls = ServerTlsConfiguration.parse(
                    properties, env, rootDir,
                    PropertyKey.LINE_TCP_TLS_ENABLED, PropertyKey.LINE_TCP_TLS_CERT_PATH, PropertyKey.LINE_TCP_TLS_PRIVATE_KEY_PATH,
                    globalTls, getString
            );
            final var pgTls = ServerTlsConfiguration.parse(
                    properties, env, rootDir,
                    PropertyKey.PG_TLS_ENABLED, PropertyKey.PG_TLS_CERT_PATH, PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
                    globalTls, getString
            );
            return new DynamicPropServerConfiguration(
                    rootDir,
                    properties,
                    env,
                    bootstrap.getLog(),
                    bootstrap.getBuildInformation(),
                    FilesFacadeImpl.INSTANCE,
                    bootstrap.getMicrosecondClock(),
                    new FragmentingTlsFactoryProviderFactory(httpTls, httpMinTls, lineTcpTls, pgTls, cipherFacade)
            );
        }
    }

    /**
     * A subclass of the production factory, as the startup guard asks for: it wraps the production provider and
     * hands the production TLS sockets the test's facade instead of the listener's own.
     */
    private static final class FragmentingTlsFactoryProviderFactory extends TlsFactoryProviderFactory {
        private final NetworkFacade cipherFacade;

        private FragmentingTlsFactoryProviderFactory(
                ServerTlsConfiguration httpTls,
                ServerTlsConfiguration httpMinTls,
                ServerTlsConfiguration lineTcpTls,
                ServerTlsConfiguration pgTls,
                NetworkFacade cipherFacade
        ) {
            super(httpTls, httpMinTls, lineTcpTls, pgTls);
            this.cipherFacade = cipherFacade;
        }

        @Override
        public @NotNull FactoryProvider getInstance(ServerConfiguration configuration, CairoEngine engine, FreeOnExit freeOnExit) {
            final FactoryProvider production = super.getInstance(configuration, engine, freeOnExit);
            return new TlsFactoryProvider(
                    production,
                    over(production.getHttpSocketFactory()),
                    production.getHttpMinSocketFactory(),
                    over(production.getLineSocketFactory()),
                    over(production.getPGWireSocketFactory())
            );
        }

        private SocketFactory over(SocketFactory production) {
            return (nf, log) -> production.newInstance(cipherFacade, log);
        }
    }

    /**
     * What one ingestion client sends: ids 0 to {@code rows - 1} into a table of its own.
     */
    private static final class IngestPlan {
        final int batches;
        final int rows;
        final String table;
        final String transport;

        private IngestPlan(String table, String transport, int rows, int batches) {
            this.table = table;
            this.transport = transport;
            this.rows = rows;
            this.batches = batches;
        }
    }

    private static final class NamedTask {
        final ClientTask body;
        final String name;

        private NamedTask(String name, ClientTask body) {
            this.name = name;
            this.body = body;
        }
    }

    /**
     * A TLS client on a blocking socket that hands out the records it wraps, so a hostile client can change,
     * repeat or withhold them, and that reads the server's answer like any client.
     */
    private static final class RawTlsClient {
        private final ByteBuffer appIn = ByteBuffer.allocate(64 * 1024);
        private final SSLEngine engine;
        private final InputStream in;
        private final ByteBuffer netIn = ByteBuffer.allocate(256 * 1024);
        private final ByteBuffer netOut = ByteBuffer.allocate(64 * 1024);
        private final OutputStream out;

        private RawTlsClient(Socket socket, @Nullable String protocol) throws IOException {
            in = socket.getInputStream();
            out = socket.getOutputStream();
            engine = trustAll.createSSLEngine();
            engine.setUseClientMode(true);
            if (protocol != null) {
                engine.setEnabledProtocols(new String[]{protocol});
            }
            engine.beginHandshake();
            // appIn holds decrypted bytes for the reader: empty for now
            appIn.flip();
        }

        void handshake() throws IOException {
            for (int guard = 0; ; guard++) {
                if (guard > 100_000) {
                    throw new IOException("the handshake does not come to an end");
                }
                switch (engine.getHandshakeStatus()) {
                    case NEED_TASK -> {
                        Runnable task;
                        while ((task = engine.getDelegatedTask()) != null) {
                            task.run();
                        }
                    }
                    case NEED_WRAP -> {
                        out.write(wrapControl());
                        out.flush();
                    }
                    case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> {
                        if (!unwrapRecord()) {
                            throw new EOFException("the server closed the connection during the handshake");
                        }
                    }
                    default -> {
                        return;
                    }
                }
            }
        }

        /**
         * @return what the server sends, decrypted
         */
        InputStream plaintext() {
            return new InputStream() {
                @Override
                public int read() throws IOException {
                    while (!appIn.hasRemaining()) {
                        if (!unwrapRecord()) {
                            return -1;
                        }
                    }
                    return appIn.get() & 0xff;
                }

                @Override
                public int read(byte @NotNull [] target, int off, int len) throws IOException {
                    if (len == 0) {
                        return 0;
                    }
                    while (!appIn.hasRemaining()) {
                        if (!unwrapRecord()) {
                            return -1;
                        }
                    }
                    final int n = Math.min(len, appIn.remaining());
                    appIn.get(target, off, n);
                    return n;
                }
            };
        }

        /**
         * Starts a handshake on the live session and returns its first record: a KeyUpdate in TLS 1.3, the
         * ClientHello of a renegotiation in TLS 1.2.
         */
        byte[] startNewHandshake() throws IOException {
            engine.beginHandshake();
            return wrapControl();
        }

        byte[] wrap(byte[] plain) throws IOException {
            final ByteArrayOutputStream wire = new ByteArrayOutputStream();
            final ByteBuffer source = ByteBuffer.wrap(plain);
            for (int guard = 0; source.hasRemaining(); guard++) {
                if (guard > 10_000) {
                    throw new IOException("the engine takes no application data");
                }
                netOut.clear();
                final SSLEngineResult result = engine.wrap(source, netOut);
                if (result.getStatus() != SSLEngineResult.Status.OK) {
                    throw new IOException("could not wrap: " + result.getStatus());
                }
                netOut.flip();
                wire.write(netOut.array(), 0, netOut.remaining());
            }
            return wire.toByteArray();
        }

        /**
         * @return one record of handshake output, empty when the engine has none
         */
        byte[] wrapControl() throws IOException {
            netOut.clear();
            engine.wrap(EMPTY_BUFFER, netOut);
            netOut.flip();
            return Arrays.copyOf(netOut.array(), netOut.remaining());
        }

        /**
         * Unwraps the next record, reading from the socket until it is whole.
         *
         * @return false when the server has closed the stream
         */
        private boolean unwrapRecord() throws IOException {
            for (; ; ) {
                netIn.flip();
                appIn.compact();
                final SSLEngineResult result;
                try {
                    result = engine.unwrap(netIn, appIn);
                } finally {
                    netIn.compact();
                    appIn.flip();
                }
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW -> {
                        final int n = in.read(netIn.array(), netIn.position(), netIn.remaining());
                        if (n < 0) {
                            return false;
                        }
                        netIn.position(netIn.position() + n);
                    }
                    case CLOSED -> {
                        return false;
                    }
                    case BUFFER_OVERFLOW -> throw new IOException("the reader left no room for a record");
                    default -> {
                        return true;
                    }
                }
            }
        }
    }

    private static class ServerBusyException extends RuntimeException {
    }
}
