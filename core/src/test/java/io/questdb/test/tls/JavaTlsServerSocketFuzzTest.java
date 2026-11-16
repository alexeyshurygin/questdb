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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.Socket;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.JavaTlsServerSocket;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Fuzzes {@link JavaTlsServerSocket} over a transport that lives in the test: a {@link NetworkFacadeImpl} subclass
 * serves the socket's raw reads and writes from two byte queues, and a JDK {@link SSLEngine} client sits on the
 * other end of them. Everything runs on the test thread, so the test decides how many bytes each raw call moves,
 * when a call would block and where the peer disappears, and a run replays from the seeds it prints
 * ({@code -Dfuzz.s0=... -Dfuzz.s1=...}). JSSE draws its own randomness, so the ciphertext differs between runs
 * even though every decision the test makes does not.
 * <p>
 * The test calls the socket only the way the IO dispatchers and the protocol handlers do: {@code recv()} when the
 * link has something to read or the socket reported buffered plaintext after the previous call, {@code send()}
 * when there is plaintext to send, {@code tlsIO()} to flush when the requested operation is not ready, and a
 * negative result ends the connection. Under that contract a byte the socket strands shows up as a deadlock, which
 * the test reports the moment nobody can act.
 * <p>
 * One socket object and one server {@link SSLContext} serve every connection of a test, as a pooled connection
 * context does, so state that survives {@code close()} breaks the next connection.
 * <p>
 * {@code -Dquestdb.fuzz.tls.iterations=N} scales the number of connections, and
 * {@code -Dquestdb.fuzz.tls.max.payload=N} the largest payload.
 */
public class JavaTlsServerSocketFuzzTest {
    // the most plaintext one TLS record carries
    private static final int MAX_RECORD_PLAINTEXT = 16_384;
    private static final String BUFFER_SIZE_PROPERTY = "questdb.experimental.tls.buffersize";
    private static final int[] BUFFER_SIZES = {1, 2, 5, 64, 2048, 16_383, 16_384, 16_385, 16_709, 262_143, 262_144, 262_145};
    private static final byte[] EMPTY = new byte[0];
    private static final long FIRST_FAKE_FD = 1_000_000;
    private static final int IO_LEN_CLASSES = 6;
    private static final int ITERATIONS = TlsFuzzSupport.iterations(16);
    // From this size on the output buffer holds a whole TLS record before the handshake has grown it, which the
    // engine needs to wrap an alert.
    private static final int LARGE_BUFFER_SIZE = 262_143;
    private static final Log LOG = LogFactory.getLog(JavaTlsServerSocketFuzzTest.class);
    private static final int MAX_COVERAGE_ROUNDS = 1_500;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 2_048;
    private static final int MAX_IDLE_STEPS = 50_000;
    private static final int MAX_PAYLOAD = Math.max(64 * 1024, Integer.getInteger("questdb.fuzz.tls.max.payload", 1024 * 1024));
    // What one send() may put on the transport on behalf of earlier KeyUpdate requests: one reply and the record.
    private static final int MAX_REPLY_BACKLOG_BYTES = 1_024;
    private static final int MAX_REPLY_BACKLOG_CALLS = 8;
    private static final long MAX_SESSION_STEPS = 20_000_000L;
    private static final long PINNED_SEED_0 = 6_407_195_113_827_044_571L;
    private static final long PINNED_SEED_1 = 1_794_762_270_801L;
    private static final byte[] PLAINTEXT_CLOSE_NOTIFY = {0x15, 0x03, 0x03, 0x00, 0x02, 0x01, 0x00};
    private static final byte[] PROBE = {'?'};
    private static final byte[] SSL_SUPPORTED = {'S'};
    private static final String TLS_12 = "TLSv1.2";
    private static final String TLS_13 = "TLSv1.3";
    private static byte[] certificateDer;
    private static String[] compatibleSuites;
    private static String[] defaultSuites;
    private static SSLContext serverContext;
    // A loop that never touches the transport would otherwise hang the build instead of failing it.
    @Rule
    public final Timeout timeout = Timeout.seconds(Math.max(600, 60L * ITERATIONS));

    @BeforeClass
    public static void setUpStatic() throws Exception {
        serverContext = TlsFuzzSupport.newServerContext();
        certificateDer = TlsFuzzSupport.serverCertificateDer();
        final SSLEngine probe = TlsFuzzSupport.newTrustAllContext().createSSLEngine();
        probe.setUseClientMode(true);
        defaultSuites = probe.getEnabledCipherSuites();
        var compatibleCount = 0;
        final var compatible = new String[defaultSuites.length];
        for (String suite : defaultSuites) {
            if (isTls13Suite(suite) || isEphemeralRsaSuite(suite)) {
                compatible[compatibleCount++] = suite;
            }
        }
        compatibleSuites = Arrays.copyOf(compatible, compatibleCount);
        assertTrue("this JDK enables no cipher suite an RSA certificate can serve", compatibleCount > 0);
        // the logger allocates on first use: do that before any test takes its leak baseline
        LOG.info().$("TLS fuzz setup [suites=").$(defaultSuites.length).$(", compatible=").$(compatibleCount).I$();
    }

    /**
     * A TLS 1.3 client asks for key updates, and the server, which has had nothing to send since, ends the
     * connection. The answers to the key updates are still owed; close_notify must go out behind them, whether
     * the server shuts its write side down or closes outright.
     */
    @Test
    public void testCloseNotifyFollowsOwedKeyUpdateReply() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (Harness h = new Harness(new Coverage())) {
                for (int i = 0, n = Math.max(4, ITERATIONS / 2); i < n; i++) {
                    final int updates = 1 + rnd.nextInt(4);
                    final boolean isShutdown = (i & 1) == 0;
                    runIteration(
                            h,
                            i,
                            "close after KeyUpdate [updates=" + updates + ", shutdown=" + isShutdown + ']',
                            () -> runCloseAfterKeyUpdate(h, rnd, updates, isShutdown)
                    );
                }
            }
        });
    }

    /**
     * Pins what the generators of the other tests produce. A fuzzer finds bugs only in what it can generate, so
     * this test runs the generators from fixed seeds until every shape has come up, and fails when one never does.
     */
    @Test
    public void testGeneratorCoverage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG, PINNED_SEED_0, PINNED_SEED_1);
            final Coverage coverage = new Coverage();
            var rounds = 0;
            try (Harness h = new Harness(coverage)) {
                for (; rounds < MAX_COVERAGE_ROUNDS && !coverage.isComplete(); rounds++) {
                    final Rnd roundRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                    final Scenario scenario = Scenario.roundTrip(roundRnd, ClientConfig.randomAccepted(roundRnd), 64 * 1024);
                    runIteration(h, rounds, scenario, () -> runCleanConnection(h, scenario, null));
                    runHandshakeIteration(h, rounds, roundRnd);
                    final HostileSession session = new HostileSession(h, roundRnd, rounds % HostileSession.KIND_COUNT, null);
                    runIteration(h, rounds, session, session::run);
                }
            }
            LOG.info().$("TLS fuzz generator coverage [rounds=").$(rounds).$(", ").$(coverage.toString()).I$();
            assertEquals("shapes the generators never produced in " + rounds + " rounds", "", coverage.missing());
        });
    }

    /**
     * Handshakes with random client configurations: protocol versions, cipher suite subsets the server can and
     * cannot serve, session resumption, PGWire-style upgrade from plaintext, any amount of the ClientHello on the
     * wire before {@code startTlsSession()}, and {@code recv()}, {@code send()} or both driving the handshake.
     */
    @Test
    public void testHandshakeUnderRandomFragmentationAndClientConfig() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final Coverage coverage = new Coverage();
            try (Harness h = new Harness(coverage)) {
                for (int i = 0, n = 3 * ITERATIONS; i < n; i++) {
                    runHandshakeIteration(h, i, new Rnd(rnd.nextLong(), rnd.nextLong()));
                }
            }
            LOG.info().$("TLS handshake fuzz [").$(coverage.toString()).I$();
        });
    }

    /**
     * Hostile clients: bytes that are not TLS, tampered handshakes and records, peers that vanish at a byte offset,
     * close_notify in odd places, renegotiation and KeyUpdate. Every scenario ends with the client hanging up, so
     * a terminal outcome is due: {@code startTlsSession()} throws, or a call returns a negative result. A
     * well-behaved connection follows each hostile one on the same socket object.
     */
    @Test
    public void testHostileInput() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final Coverage coverage = new Coverage();
            try (Harness h = new Harness(coverage)) {
                for (int i = 0, n = 4 * ITERATIONS; i < n; i++) {
                    final Rnd iterationRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                    final HostileSession session = new HostileSession(h, iterationRnd, iterationRnd.nextInt(HostileSession.KIND_COUNT), null);
                    runIteration(h, i, session, session::run);
                    // the pooled socket and the shared SSLContext must serve a well-behaved client right after
                    final Scenario scenario = Scenario.handshake(iterationRnd, ClientConfig.defaults(iterationRnd));
                    runIteration(h, i, scenario, () -> runCleanConnection(h, scenario, null));
                }
            }
            LOG.info().$("TLS hostile input fuzz [").$(coverage.toString()).I$();
        });
    }

    /**
     * A TLS 1.3 client may ask for a key update as often as it likes, and each request obliges the server to
     * answer. A server that only reads must not let the answers pile up where nothing reports them: either
     * {@code wantsTlsWrite()} announces them so the dispatcher flushes them, or the server refuses the flood.
     */
    @Test
    public void testKeyUpdateFloodDoesNotQueueReplies() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final int updates = 256 + rnd.nextInt(64 * ITERATIONS);
            try (Harness h = new Harness(new Coverage())) {
                runIteration(h, 0, "KeyUpdate flood [updates=" + updates + ']', () -> runKeyUpdateFlood(h, rnd, updates));
            }
        });
    }

    /**
     * Long random sequences of socket calls, client actions and transport changes on one connection, checked
     * after every step against a model that knows nothing about TLS: two byte streams and a few flags.
     */
    @Test
    public void testRandomOperationSequences() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final Coverage coverage = new Coverage();
            try (Harness h = new Harness(coverage)) {
                for (int i = 0; i < ITERATIONS; i++) {
                    final OperationSequence sequence = new OperationSequence(h, new Rnd(rnd.nextLong(), rnd.nextLong()));
                    runIteration(h, i, sequence, sequence::run);
                }
            }
        });
    }

    /**
     * Payloads around every record and buffer boundary travel both ways while the transport hands out random
     * chunk sizes and would-block results, with random buffer sizes on the socket and random lengths on every
     * {@code recv()} and {@code send()}. Every byte must arrive in order, and the session must end clean.
     */
    @Test
    public void testRoundTripUnderRandomFragmentation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final Coverage coverage = new Coverage();
            try (Harness h = new Harness(coverage)) {
                for (int i = 0; i < ITERATIONS; i++) {
                    final Rnd iterationRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
                    final Scenario scenario = Scenario.roundTrip(iterationRnd, ClientConfig.randomAccepted(iterationRnd), MAX_PAYLOAD);
                    runIteration(h, i, scenario, () -> runCleanConnection(h, scenario, null));
                }
            }
            LOG.info().$("TLS round trip fuzz [").$(coverage.toString()).I$();
        });
    }

    private static byte[] concat(byte[] head, byte[] tail) {
        final byte[] joined = Arrays.copyOf(head, head.length + tail.length);
        System.arraycopy(tail, 0, joined, head.length, tail.length);
        return joined;
    }

    private static boolean contains(String[] values, String value) {
        for (String candidate : values) {
            if (candidate.equals(value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Drives an undisturbed handshake to its end the way a dispatcher would: {@code recv()} when the link has
     * bytes, {@code tlsIO()} to flush what the transport did not take.
     */
    private static void driveHandshake(Harness h, EngineClient client) {
        final JavaTlsServerSocket socket = h.socket;
        for (int steps = 0; !client.isHandshakeDone() || socket.isHandshakeInProgress(); steps++) {
            assertTrue("the handshake did not complete", steps < 10_000_000);
            client.pump();
            client.assertHealthy();
            client.flush();
            if (h.isRecvDue()) {
                assertEquals("recv() must deliver nothing during the handshake", 0, h.recv(64));
            } else if (socket.wantsTlsWrite()) {
                assertEquals(0, h.tlsIO(Socket.WRITE_FLAG));
            }
        }
    }

    private static void flipBit(byte[] bytes, int at) {
        bytes[at] ^= (byte) (1 << (at & 7));
    }

    private static boolean isEphemeralRsaSuite(String suite) {
        return suite.startsWith("TLS_ECDHE_RSA_WITH_") || suite.startsWith("TLS_DHE_RSA_WITH_");
    }

    private static boolean isTls13Suite(String suite) {
        return suite.startsWith("TLS_AES_") || suite.startsWith("TLS_CHACHA20_");
    }

    /**
     * The start of a PGWire connection on a TLS listener: the server reads the 8-byte SSLRequest and answers 'S',
     * both in plaintext through the same socket, and only then starts TLS.
     */
    private static void pgUpgrade(Harness h, Link link) {
        final byte[] request = new byte[TlsFuzzSupport.PG_SSL_REQUEST.length];
        var got = 0;
        for (int tries = 0; got < request.length; tries++) {
            assertTrue("plaintext recv() made no progress", tries < 100_000);
            final int n = h.recv(request.length - got);
            assertTrue("plaintext recv() failed: " + n, n >= 0);
            System.arraycopy(h.tmp, 0, request, got, n);
            got += n;
        }
        assertArrayEquals("plaintext recv() before the handshake must pass bytes through", TlsFuzzSupport.PG_SSL_REQUEST, request);
        assertFalse(h.socket.isTlsSessionStarted());
        assertFalse(h.socket.isMorePlaintextBuffered());
        var sent = 0;
        for (int tries = 0; sent == 0; tries++) {
            assertTrue("plaintext send() made no progress", tries < 100_000);
            sent = h.send(SSL_SUPPORTED, 0, 1);
            assertTrue("plaintext send() failed: " + sent, sent >= 0);
        }
        assertEquals(1, sent);
        assertFalse(h.socket.wantsTlsWrite());
        assertEquals("the dispatcher hooks are inert before the handshake", 0, h.tlsIO(Socket.WRITE_FLAG | Socket.READ_FLAG));
        // the client takes the answer off the wire before it speaks TLS
        assertEquals(1, link.toClient.available());
        assertEquals('S', link.toClient.readByte());
    }

    private static byte[] randomBytes(Rnd rnd, int len) {
        final byte[] bytes = new byte[len];
        TlsFuzzSupport.fill(rnd, bytes);
        return bytes;
    }

    /**
     * @return the offset of every TLS record in {@code wire} followed by the length of {@code wire}, or null when
     * the bytes are not a run of whole records
     */
    private static int[] recordStarts(byte[] wire) {
        var count = 0;
        for (int at = 0; at < wire.length; count++) {
            if (at + TlsFuzzSupport.RECORD_HEADER_LENGTH > wire.length) {
                return null;
            }
            at += TlsFuzzSupport.RECORD_HEADER_LENGTH + (((wire[at + 3] & 0xff) << 8) | (wire[at + 4] & 0xff));
            if (at > wire.length) {
                return null;
            }
        }
        final int[] starts = new int[count + 1];
        var at = 0;
        for (int i = 0; i < count; i++) {
            starts[i] = at;
            at += TlsFuzzSupport.RECORD_HEADER_LENGTH + (((wire[at + 3] & 0xff) << 8) | (wire[at + 4] & 0xff));
        }
        starts[count] = wire.length;
        return starts;
    }

    /**
     * One well-behaved connection: handshake, both payloads, an orderly end, and the checks that the socket
     * came back clean.
     */
    private static void runCleanConnection(Harness h, Scenario s, ConnectionInfo info) throws Exception {
        final Coverage coverage = h.coverage;
        final JavaTlsServerSocket socket = h.socket;
        final Rnd scheduleRnd = new Rnd(s.scheduleSeed0, s.scheduleSeed1);
        final Link link = h.open(new Rnd(s.transportSeed0, s.transportSeed1));
        link.recvChunk = s.recvChunk;
        link.recvWouldBlockPct = s.recvWouldBlockPct;
        link.sendChunk = s.sendChunk;
        link.sendWouldBlockPct = s.sendWouldBlockPct;
        link.sendErrorCode = scheduleRnd.nextBoolean() ? Net.EPEERDISCONNECT : Net.EOTHERDISCONNECT;
        final SSLContext clientContext = s.clientContext != null ? s.clientContext : TlsFuzzSupport.newTrustAllContext();
        final EngineClient client = new EngineClient(clientContext, link, s.protocols, s.suites, s.peerPort);
        client.expectPlaintext(s.down);
        client.pump();
        client.assertHealthy();
        assertTrue("the client produced no ClientHello", client.stagedSize() > 1);
        coverage.hit(Coverage.DRIVER_RECV + s.handshakeDriver);
        if (s.recvChunk == 1 || s.sendChunk == 1) {
            coverage.hit(Coverage.ONE_BYTE_CHUNKS);
        }

        // what is on the wire before the server starts TLS: nothing, part of the ClientHello, or all of it
        if (s.isPgStyle) {
            link.toServer.append(TlsFuzzSupport.PG_SSL_REQUEST, 0, TlsFuzzSupport.PG_SSL_REQUEST.length);
            coverage.hit(Coverage.PG_STYLE);
        }
        switch (s.preQueued) {
            case Scenario.QUEUED_ALL -> {
                client.flush();
                coverage.hit(Coverage.QUEUED_ALL);
            }
            case Scenario.QUEUED_PART -> {
                client.flush(1 + scheduleRnd.nextInt(client.stagedSize() - 1));
                coverage.hit(Coverage.QUEUED_PART);
            }
            default -> coverage.hit(Coverage.QUEUED_NONE);
        }
        if (s.isPgStyle) {
            pgUpgrade(h, link);
        }
        h.startTls(s.bufferSize);

        final byte[] up = s.up;
        final byte[] down = s.down;
        final long budget = s.stepBudget();
        var clientSent = 0;
        var serverReceived = 0;
        var serverSent = 0;
        var isMeasured = !s.isHandshakeMeasured;
        var lastSignature = -1L;
        var idleSteps = 0;
        for (long steps = 0; ; steps++) {
            final boolean isHandshaking = socket.isHandshakeInProgress();
            if (!isMeasured && !isHandshaking && !socket.wantsTlsWrite()) {
                // the server's whole handshake is on the wire and nothing else is
                info.handshakeBytesToClient = link.toClient.totalWritten;
                isMeasured = true;
            }
            final boolean isClientDue = client.isWorkPending()
                    || link.toClient.available() > 0
                    || client.stagedSize() > 0
                    || (client.isHandshakeDone() && clientSent < up.length);
            if (!isHandshaking && isMeasured && !isClientDue && client.isHandshakeDone()
                    && serverReceived == up.length && serverSent == down.length && client.received == down.length
                    && !socket.wantsTlsWrite() && !h.isReadRepublished) {
                break;
            }
            // the handshake driver decides who may move it: a handler that reads first, one that writes first, or both
            final boolean isRecvDue = (!isHandshaking || s.handshakeDriver != Scenario.DRIVER_SEND) && h.isRecvDue();
            final boolean isSendDue = isMeasured
                    && (!isHandshaking || s.handshakeDriver != Scenario.DRIVER_RECV)
                    && serverSent < down.length;
            // the dispatcher flushes on its own only while the requested operation is not ready
            final boolean isFlushDue = socket.wantsTlsWrite() && !isRecvDue;
            final int enabled = (isClientDue ? 1 : 0) + (isRecvDue ? 1 : 0) + (isSendDue ? 1 : 0) + (isFlushDue ? 1 : 0);
            if (enabled == 0 || steps >= budget) {
                fail((enabled == 0 ? "deadlock: neither side can act" : "step budget exhausted")
                        + " [clientSent=" + clientSent + '/' + up.length
                        + ", serverReceived=" + serverReceived
                        + ", serverSent=" + serverSent + '/' + down.length
                        + ", clientReceived=" + client.received
                        + ", serverHandshaking=" + isHandshaking
                        + ", clientHandshakeDone=" + client.isHandshakeDone()
                        + ", wantsTlsWrite=" + socket.wantsTlsWrite()
                        + ", morePlaintextBuffered=" + socket.isMorePlaintextBuffered()
                        + ", toServer=" + link.toServer.available()
                        + ", toClient=" + link.toClient.available()
                        + ", steps=" + steps + ']');
            }
            var pick = scheduleRnd.nextInt(enabled);
            if (isClientDue && pick-- == 0) {
                client.pump();
                client.assertHealthy();
                // send() reports plaintext once the socket has all of its ciphertext, so the client can be up to
                // one record ahead of what send() has returned
                assertTrue("the client received plaintext the server has not sent", client.received <= serverSent + MAX_RECORD_PLAINTEXT);
                if (client.isHandshakeDone() && clientSent < up.length && scheduleRnd.nextInt(4) != 0) {
                    // right behind the Finished when the handshake has just completed
                    clientSent += client.wrap(up, clientSent, Math.min(up.length - clientSent, s.clientRecordLen(scheduleRnd)));
                }
                client.flush();
            } else if (isRecvDue && pick-- == 0) {
                final int len = scheduleRnd.nextInt(32) == 0 ? 0 : Scenario.ioLen(scheduleRnd, s.recvLenClass);
                final int n = h.recv(len);
                if (n < 0) {
                    fail("recv() failed on a healthy connection: " + n);
                }
                if (socket.isHandshakeInProgress()) {
                    assertEquals("recv() must deliver nothing during the handshake", 0, n);
                }
                if (len == 0) {
                    assertEquals(0, n);
                    coverage.hit(Coverage.ZERO_LENGTH_RECV);
                }
                if (n > 0) {
                    assertTrue("recv() returned more plaintext than the client has sent", serverReceived + n <= clientSent);
                    final int mismatch = Arrays.mismatch(h.tmp, 0, n, up, serverReceived, serverReceived + n);
                    if (mismatch != -1) {
                        fail("recv() returned a wrong byte at stream offset " + (serverReceived + mismatch));
                    }
                    serverReceived += n;
                    // below the staging buffer's capacity the record goes through it, above it straight to the caller
                    if (len < 16_384) {
                        coverage.hit(Coverage.RECV_STAGED);
                    } else if (len >= 32_768) {
                        coverage.hit(Coverage.RECV_DIRECT);
                    }
                }
            } else if (isSendDue && pick-- == 0) {
                final int len = scheduleRnd.nextInt(32) == 0 ? 0 : Math.min(down.length - serverSent, Scenario.ioLen(scheduleRnd, s.sendLenClass));
                final int n = h.send(down, serverSent, len);
                if (n < 0) {
                    fail("send() failed on a healthy connection: " + n);
                }
                if (socket.isHandshakeInProgress()) {
                    assertEquals("send() must take nothing during the handshake", 0, n);
                }
                if (len == 0) {
                    coverage.hit(Coverage.ZERO_LENGTH_SEND);
                }
                serverSent += n;
            } else {
                assertEquals("tlsIO() failed on a healthy connection", 0, h.tlsIO(Socket.WRITE_FLAG));
            }
            final long signature = link.toServer.totalRead + link.toServer.totalWritten
                    + link.toClient.totalRead + link.toClient.totalWritten
                    + serverReceived + serverSent + clientSent + client.received;
            if (signature != lastSignature) {
                lastSignature = signature;
                idleSteps = 0;
            } else if (++idleSteps > MAX_IDLE_STEPS) {
                fail("no byte has moved for " + MAX_IDLE_STEPS + " steps [clientSent=" + clientSent
                        + ", serverReceived=" + serverReceived + ", serverSent=" + serverSent
                        + ", clientReceived=" + client.received + ", serverHandshaking=" + socket.isHandshakeInProgress() + ']');
            }
        }

        // both sides have everything and the socket holds nothing back
        assertTrue(socket.isTlsSessionStarted());
        assertFalse(socket.wantsTlsWrite());
        assertFalse(socket.wantsTlsRead());
        assertFalse(socket.isMorePlaintextBuffered());
        assertEquals("recv() returned data that nothing announced", 0, h.recv(64));
        assertFalse(socket.isMorePlaintextBuffered());
        final String protocol = client.engine.getSession().getProtocol();
        final String suite = client.engine.getSession().getCipherSuite();
        assertTrue("negotiated a protocol the client did not offer: " + protocol, contains(s.protocols, protocol));
        assertTrue("negotiated a cipher suite the client did not offer: " + suite, contains(s.suites != null ? s.suites : defaultSuites, suite));
        coverage.hit(TLS_13.equals(protocol) ? Coverage.PROTOCOL_TLS13 : Coverage.PROTOCOL_TLS12);
        if (suite.contains("_GCM_")) {
            coverage.hit(Coverage.SUITE_GCM);
        } else if (suite.contains("CHACHA20")) {
            coverage.hit(Coverage.SUITE_CHACHA20);
        } else if (suite.contains("_CBC_")) {
            coverage.hit(Coverage.SUITE_CBC);
        }
        if (info != null) {
            info.protocol = protocol;
            info.sessionId = client.engine.getSession().getId();
            info.suite = suite;
        }

        switch (s.ending) {
            case Scenario.ENDING_SHUTDOWN -> {
                // shutdown() queues close_notify and tries once to send it: the client sees it exactly when
                // nothing stayed behind
                assertEquals(0, h.shutdown(Net.SHUT_WR));
                assertEquals(1, link.shutdownWriteCalls);
                final boolean isPending = socket.wantsTlsWrite();
                client.pump();
                client.assertHealthy();
                assertEquals("close_notify delivery must match wantsTlsWrite() after shutdown()", !isPending, client.isCloseNotifySeen);
                coverage.hit(isPending ? Coverage.CLOSE_NOTIFY_LOST : Coverage.CLOSE_NOTIFY_DELIVERED);
                // nothing more can be sent: the engine is closed, and so is the write side of the transport
                assertEquals(isPending ? link.sendErrorCode : -1, h.send(PROBE, 0, 1));
                h.closeAndVerify();
            }
            case Scenario.ENDING_CLOSE_CLEAN -> {
                // close() sends close_notify on a best-effort basis; a transport that takes it must carry it
                link.clearFaults();
                h.closeAndVerify();
                client.pump();
                client.assertHealthy();
                assertTrue("close() must deliver close_notify when the transport takes it", client.isCloseNotifySeen);
            }
            default -> {
                h.closeAndVerify();
                client.pump();
                client.assertHealthy();
            }
        }
        assertEquals(down.length, client.received);
    }

    private static void runCloseAfterKeyUpdate(Harness h, Rnd rnd, int updates, boolean isShutdown) throws Exception {
        final JavaTlsServerSocket socket = h.socket;
        // a transport without faults: whatever the socket means to send arrives
        final Link link = h.open(new Rnd(rnd.nextLong(), rnd.nextLong()));
        final EngineClient client = new EngineClient(TlsFuzzSupport.newTrustAllContext(), link, new String[]{TLS_13}, null, 1);
        client.pump();
        client.flush();
        h.startTls(LARGE_BUFFER_SIZE + 1);
        driveHandshake(h, client);
        client.pump();
        client.assertHealthy();
        assertEquals(TLS_13, client.engine.getSession().getProtocol());
        for (int i = 0; i < updates; i++) {
            client.startNewHandshake();
        }
        client.flush();
        // the server reads the key updates and has nothing of its own to send
        for (int polls = 0; h.isRecvDue(); polls++) {
            assertTrue(polls < 1_000);
            assertEquals("a KeyUpdate carries no plaintext", 0, h.recv(4096));
        }
        // what the socket reports as pending goes out the way the dispatcher flushes it
        for (int polls = 0; socket.wantsTlsWrite(); polls++) {
            assertTrue(polls < 1_000);
            assertEquals(0, h.tlsIO(Socket.WRITE_FLAG));
        }
        if (isShutdown) {
            assertEquals(0, h.shutdown(Net.SHUT_WR));
            assertFalse("a transport without faults takes everything shutdown() sends", socket.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
            assertTrue("shutdown() must send close_notify behind the key update answers it owes", client.isCloseNotifySeen);
            h.closeAndVerify();
        } else {
            h.closeAndVerify();
            client.pump();
            client.assertHealthy();
            assertTrue("close() must send close_notify behind the key update answers it owes", client.isCloseNotifySeen);
        }
    }

    private static void runHandshakeIteration(Harness h, int iteration, Rnd rnd) {
        if (rnd.nextInt(6) == 0) {
            final String protocol = rnd.nextBoolean() ? TLS_13 : TLS_12;
            runIteration(h, iteration, "session resumption over " + protocol, () -> runResumptionPair(h, rnd, protocol));
            return;
        }
        final ClientConfig config = ClientConfig.random(rnd);
        if (config.expectation == ClientConfig.MUST_SUCCEED) {
            final Scenario scenario = Scenario.handshake(rnd, config);
            runIteration(h, iteration, scenario, () -> runCleanConnection(h, scenario, null));
        } else {
            final HostileSession session = new HostileSession(h, rnd, HostileSession.KIND_CLIENT_CONFIG, config);
            runIteration(h, iteration, session, session::run);
            // a refused handshake must leave the pooled socket fit for the next client
            final Scenario scenario = Scenario.handshake(rnd, ClientConfig.defaults(rnd));
            runIteration(h, iteration, scenario, () -> runCleanConnection(h, scenario, null));
        }
    }

    /**
     * Runs one iteration and puts its parameters into the failure, so a red run says what it was doing.
     */
    private static void runIteration(Harness h, int iteration, Object scenario, TestUtils.LeakProneCode body) {
        try {
            body.run();
        } catch (Throwable th) {
            h.abort();
            throw new AssertionError("iteration " + iteration + " failed [" + scenario + "]: " + th, th);
        }
    }

    private static void runKeyUpdateFlood(Harness h, Rnd rnd, int updates) throws Exception {
        final JavaTlsServerSocket socket = h.socket;
        final Link link = h.open(new Rnd(rnd.nextLong(), rnd.nextLong()));
        final EngineClient client = new EngineClient(TlsFuzzSupport.newTrustAllContext(), link, new String[]{TLS_13}, null, 1);
        client.expectPlaintext(PROBE);
        client.pump();
        client.flush();
        h.startTls(LARGE_BUFFER_SIZE + 1);
        driveHandshake(h, client);
        client.pump();
        client.assertHealthy();
        assertEquals(TLS_13, client.engine.getSession().getProtocol());

        var isRefused = false;
        for (int sent = 0; sent < updates && !isRefused; ) {
            final int batch = Math.min(updates - sent, 1 + rnd.nextInt(64));
            for (int i = 0; i < batch; i++) {
                client.startNewHandshake();
            }
            sent += batch;
            client.flush();
            // the server only reads, as a line receiver or a server that waits for a request does
            for (int polls = 0; h.isRecvDue() || socket.wantsTlsWrite(); polls++) {
                assertTrue("the server does not get through the key updates", polls < 10_000_000);
                if (h.isRecvDue()) {
                    final int n = h.recv(4096);
                    if (n < 0) {
                        // a server may refuse the flood outright
                        isRefused = true;
                        break;
                    }
                    assertEquals("a KeyUpdate carries no plaintext", 0, n);
                } else {
                    // replies the socket reports are flushed the way the dispatcher flushes them
                    assertEquals(0, h.tlsIO(Socket.WRITE_FLAG));
                }
            }
            client.pump();
            client.assertHealthy();
        }
        if (!isRefused) {
            final long callsBefore = link.sendCalls;
            final long bytesBefore = link.toClient.totalWritten;
            final int n = h.send(PROBE, 0, 1);
            final long calls = link.sendCalls - callsBefore;
            final long bytes = link.toClient.totalWritten - bytesBefore;
            assertEquals(1, n);
            assertTrue(
                    "one send() after " + updates + " KeyUpdate requests wrote " + bytes + " bytes in " + calls
                            + " transport calls: the replies queued up inside the SSLEngine while wantsTlsWrite() stayed false",
                    bytes <= MAX_REPLY_BACKLOG_BYTES && calls <= MAX_REPLY_BACKLOG_CALLS
            );
            for (int polls = 0; socket.wantsTlsWrite(); polls++) {
                assertTrue(polls < 1_000);
                assertEquals(0, h.tlsIO(Socket.WRITE_FLAG));
            }
            client.pump();
            client.assertHealthy();
            assertEquals("the byte sent after the key updates must arrive", 1, client.received);
        }
        h.closeAndVerify();
    }

    /**
     * Two connections from one client context. The second one must resume the session of the first: the server
     * then sends no certificate, and in TLS 1.2 the session id stays the same.
     */
    private static void runResumptionPair(Harness h, Rnd rnd, String protocol) throws Exception {
        final SSLContext clientContext = TlsFuzzSupport.newTrustAllContext();
        // only a key in the client's session cache: no socket is involved
        final int peerPort = 1 + rnd.nextInt(60_000);
        final ClientConfig config = ClientConfig.of(protocol);
        final ConnectionInfo full = new ConnectionInfo();
        final ConnectionInfo resumed = new ConnectionInfo();
        runCleanConnection(h, Scenario.resumable(rnd, config, clientContext, peerPort), full);
        runCleanConnection(h, Scenario.resumable(rnd, config, clientContext, peerPort), resumed);
        assertEquals(protocol, full.protocol);
        assertEquals(protocol, resumed.protocol);
        assertEquals("a resumed session keeps its cipher suite", full.suite, resumed.suite);
        if (TLS_12.equals(protocol)) {
            assertArrayEquals("the second connection must resume the session of the first", full.sessionId, resumed.sessionId);
        }
        assertTrue(
                "a resumed handshake carries no certificate [full=" + full.handshakeBytesToClient
                        + ", resumed=" + resumed.handshakeBytesToClient + ", certificate=" + certificateDer.length + ']',
                resumed.handshakeBytesToClient + certificateDer.length < full.handshakeBytesToClient
        );
        h.coverage.hit(TLS_12.equals(protocol) ? Coverage.RESUMED_TLS12 : Coverage.RESUMED_TLS13);
    }

    private static byte[] swapRecords(byte[] wire, int first, int second, int end) {
        final byte[] swapped = wire.clone();
        final int secondLength = end - second;
        System.arraycopy(wire, second, swapped, first, secondLength);
        System.arraycopy(wire, first, swapped, first + secondLength, second - first);
        return swapped;
    }

    /**
     * Bytes in flight in one direction of the fake connection.
     */
    private static final class ByteQueue {
        long totalRead;
        long totalWritten;
        private byte[] buf = new byte[4096];
        private int r;
        private int w;

        void append(byte[] source, int off, int len) {
            ensureRoom(len);
            System.arraycopy(source, off, buf, w, len);
            w += len;
            totalWritten += len;
        }

        void appendFromNative(long address, int len) {
            ensureRoom(len);
            Unsafe.copyMemory(null, address, buf, Unsafe.BYTE_OFFSET + w, len);
            w += len;
            totalWritten += len;
        }

        int available() {
            return w - r;
        }

        /**
         * @return a copy of the last {@code len} bytes written; the caller makes sure nobody has read them yet
         */
        byte[] copyTail(int len) {
            return Arrays.copyOfRange(buf, w - len, w);
        }

        void moveTo(ByteQueue target, int len) {
            target.append(buf, r, len);
            r += len;
            totalRead += len;
        }

        void read(byte[] target, int off, int len) {
            System.arraycopy(buf, r, target, off, len);
            r += len;
            totalRead += len;
        }

        int readByte() {
            totalRead++;
            return buf[r++] & 0xff;
        }

        void readToNative(long address, int len) {
            Unsafe.copyMemory(buf, Unsafe.BYTE_OFFSET + r, null, address, len);
            r += len;
            totalRead += len;
        }

        byte[] takeAll() {
            final byte[] bytes = Arrays.copyOfRange(buf, r, w);
            totalRead += bytes.length;
            r = w;
            return bytes;
        }

        private void ensureRoom(int len) {
            if (r == w) {
                r = 0;
                w = 0;
            }
            if (w + len <= buf.length) {
                return;
            }
            final int live = w - r;
            if (live + len <= buf.length) {
                System.arraycopy(buf, r, buf, 0, live);
            } else {
                final byte[] bigger = new byte[Math.max(buf.length * 2, live + len)];
                System.arraycopy(buf, r, bigger, 0, live);
                buf = bigger;
            }
            r = 0;
            w = live;
        }
    }

    /**
     * What the client offers, and what the server's RSA certificate makes of it.
     */
    private static final class ClientConfig {
        // Static RSA key exchange: older JDKs enable it, newer ones do not, and either answer is in order.
        static final int MAY_SUCCEED = 1;
        static final int MUST_FAIL = 2;
        static final int MUST_SUCCEED = 0;
        final int expectation;
        final String[] protocols;
        // null stands for the engine's defaults
        final String[] suites;

        private ClientConfig(String[] protocols, String[] suites) {
            this.protocols = protocols;
            this.suites = suites;
            this.expectation = classify(protocols, suites);
        }

        static ClientConfig defaults(Rnd rnd) {
            return new ClientConfig(randomProtocols(rnd), null);
        }

        static ClientConfig of(String protocol) {
            return new ClientConfig(new String[]{protocol}, null);
        }

        static ClientConfig random(Rnd rnd) {
            final String[] protocols = randomProtocols(rnd);
            switch (rnd.nextInt(10)) {
                case 0:
                case 1:
                    return new ClientConfig(protocols, null);
                case 2:
                case 3:
                case 4: {
                    // one suite the certificate can serve, mostly under a protocol that carries it
                    final String suite = compatibleSuites[rnd.nextInt(compatibleSuites.length)];
                    final String[] fitting = rnd.nextInt(4) == 0 ? protocols : new String[]{isTls13Suite(suite) ? TLS_13 : TLS_12};
                    return new ClientConfig(fitting, new String[]{suite});
                }
                case 5:
                case 6:
                    // any one suite: most of them need a certificate the server does not have
                    return new ClientConfig(protocols, new String[]{defaultSuites[rnd.nextInt(defaultSuites.length)]});
                default: {
                    // a random non-empty subset, in random order: the server honors the client's order
                    final String[] shuffled = defaultSuites.clone();
                    for (int i = shuffled.length - 1; i > 0; i--) {
                        final int j = rnd.nextInt(i + 1);
                        final String swap = shuffled[i];
                        shuffled[i] = shuffled[j];
                        shuffled[j] = swap;
                    }
                    return new ClientConfig(protocols, Arrays.copyOf(shuffled, 1 + rnd.nextInt(shuffled.length)));
                }
            }
        }

        /**
         * @return a random configuration the server must accept
         */
        static ClientConfig randomAccepted(Rnd rnd) {
            for (int i = 0; i < 64; i++) {
                final ClientConfig config = random(rnd);
                if (config.expectation == MUST_SUCCEED) {
                    return config;
                }
            }
            return defaults(rnd);
        }

        @Override
        public String toString() {
            return "protocols=" + Arrays.toString(protocols)
                    + ", suites=" + (suites == null ? "default" : Arrays.toString(suites))
                    + ", expectation=" + (expectation == MUST_SUCCEED ? "accept" : expectation == MUST_FAIL ? "refuse" : "either");
        }

        private static int classify(String[] protocols, String[] suites) {
            final boolean isTls13Offered = contains(protocols, TLS_13);
            final boolean isTls12Offered = contains(protocols, TLS_12);
            var hasTls13Suite = false;
            var hasEphemeralRsaSuite = false;
            var hasStaticRsaSuite = false;
            for (String suite : suites != null ? suites : defaultSuites) {
                if (isTls13Suite(suite)) {
                    hasTls13Suite = true;
                } else if (isEphemeralRsaSuite(suite)) {
                    hasEphemeralRsaSuite = true;
                } else if (suite.startsWith("TLS_RSA_WITH_")) {
                    hasStaticRsaSuite = true;
                }
            }
            if ((isTls13Offered && hasTls13Suite) || (isTls12Offered && hasEphemeralRsaSuite)) {
                return MUST_SUCCEED;
            }
            return isTls12Offered && hasStaticRsaSuite ? MAY_SUCCEED : MUST_FAIL;
        }

        private static String[] randomProtocols(Rnd rnd) {
            return switch (rnd.nextInt(3)) {
                case 0 -> new String[]{TLS_13};
                case 1 -> new String[]{TLS_12};
                default -> new String[]{TLS_13, TLS_12};
            };
        }
    }

    /**
     * What a clean connection reports back to a test that compares connections.
     */
    private static final class ConnectionInfo {
        long handshakeBytesToClient;
        String protocol;
        byte[] sessionId;
        String suite;
    }

    /**
     * Counts what the generators produced and what the socket was seen doing.
     */
    private static final class Coverage {
        static final int ALERT_DELIVERED = 24;
        static final int CLIENT_CANNOT_START = 23;
        static final int CLOSE_NOTIFY_DELIVERED = 25;
        static final int CLOSE_NOTIFY_LOST = 26;
        static final int CONFIG_REJECTED = 22;
        // the three drivers follow each other in the order of Scenario.DRIVER_*
        static final int DRIVER_RECV = 13;
        static final int GROWTH_AFTER_HANDSHAKE = 8;
        static final int GROWTH_IN_HANDSHAKE = 7;
        static final int HANDSHAKE_OUTPUT_PENDING = 9;
        // one counter per hostile kind follows
        static final int KIND_FIRST = 34;
        static final int ONE_BYTE_CHUNKS = 12;
        static final int OUTCOME_RECV_FAILED = 30;
        static final int OUTCOME_SEND_FAILED = 31;
        static final int OUTCOME_START_THREW = 29;
        static final int OUTCOME_TLSIO_FAILED = 32;
        static final int PG_STYLE = 16;
        static final int PROTOCOL_TLS12 = 1;
        static final int PROTOCOL_TLS13 = 0;
        static final int QUEUED_ALL = 17;
        static final int QUEUED_NONE = 19;
        static final int QUEUED_PART = 18;
        static final int RECV_DIRECT = 6;
        static final int RECV_STAGED = 5;
        static final int RECV_WOULD_BLOCK = 10;
        static final int RESUMED_TLS12 = 20;
        static final int RESUMED_TLS13 = 21;
        static final int SEND_WOULD_BLOCK = 11;
        static final int SUITE_CBC = 4;
        static final int SUITE_CHACHA20 = 3;
        static final int SUITE_GCM = 2;
        static final int SURVIVED = 33;
        static final int ZERO_LENGTH_RECV = 27;
        static final int ZERO_LENGTH_SEND = 28;
        private static final int COUNT = KIND_FIRST + HostileSession.KIND_COUNT;
        private static final String[] NAMES = new String[COUNT];
        private final long[] counts = new long[COUNT];

        void hit(int what) {
            counts[what]++;
        }

        boolean isComplete() {
            return missing().isEmpty();
        }

        /**
         * @return the shapes this run must have produced and has not, or an empty string
         */
        String missing() {
            final StringBuilder missing = new StringBuilder();
            for (int i = 0; i < COUNT; i++) {
                if (counts[i] == 0 && isRequired(i)) {
                    if (missing.length() > 0) {
                        missing.append(", ");
                    }
                    missing.append(NAMES[i]);
                }
            }
            return missing.toString();
        }

        @Override
        public String toString() {
            final StringBuilder sink = new StringBuilder();
            for (int i = 0; i < COUNT; i++) {
                if (i > 0) {
                    sink.append(", ");
                }
                sink.append(NAMES[i]).append('=').append(counts[i]);
            }
            return sink.toString();
        }

        private static boolean hasCompatibleSuite(String marker) {
            for (String suite : compatibleSuites) {
                if (suite.contains(marker)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean isRequired(int what) {
            return switch (what) {
                // whether a refused handshake leaves an alert on the wire depends on which call met the failure,
                // and a transport error lands in send() or tlsIO() only when it hits a pending write
                case ALERT_DELIVERED, OUTCOME_SEND_FAILED, OUTCOME_TLSIO_FAILED -> false;
                // a JDK that does not enable a family cannot negotiate it
                case SUITE_GCM -> hasCompatibleSuite("_GCM_");
                case SUITE_CHACHA20 -> hasCompatibleSuite("CHACHA20");
                case SUITE_CBC -> hasCompatibleSuite("_CBC_");
                default -> true;
            };
        }

        static {
            NAMES[PROTOCOL_TLS13] = "TLSv1.3";
            NAMES[PROTOCOL_TLS12] = "TLSv1.2";
            NAMES[SUITE_GCM] = "GCM suite";
            NAMES[SUITE_CHACHA20] = "ChaCha20 suite";
            NAMES[SUITE_CBC] = "CBC suite";
            NAMES[RECV_STAGED] = "recv through the staging buffer";
            NAMES[RECV_DIRECT] = "recv straight into the caller's buffer";
            NAMES[GROWTH_IN_HANDSHAKE] = "buffer growth in the handshake";
            NAMES[GROWTH_AFTER_HANDSHAKE] = "buffer growth after the handshake";
            NAMES[HANDSHAKE_OUTPUT_PENDING] = "handshake output pending";
            NAMES[RECV_WOULD_BLOCK] = "recv would block";
            NAMES[SEND_WOULD_BLOCK] = "send would block";
            NAMES[ONE_BYTE_CHUNKS] = "one-byte chunks";
            NAMES[DRIVER_RECV] = "handshake driven by recv";
            NAMES[DRIVER_RECV + 1] = "handshake driven by send";
            NAMES[DRIVER_RECV + 2] = "handshake driven by recv and send";
            NAMES[PG_STYLE] = "upgrade from plaintext";
            NAMES[QUEUED_ALL] = "ClientHello queued before the start";
            NAMES[QUEUED_PART] = "ClientHello partly queued before the start";
            NAMES[QUEUED_NONE] = "start without client bytes";
            NAMES[RESUMED_TLS12] = "TLSv1.2 resumption";
            NAMES[RESUMED_TLS13] = "TLSv1.3 resumption";
            NAMES[CONFIG_REJECTED] = "ClientHello refused";
            NAMES[CLIENT_CANNOT_START] = "client with nothing to offer";
            NAMES[ALERT_DELIVERED] = "alert delivered";
            NAMES[CLOSE_NOTIFY_DELIVERED] = "close_notify delivered by shutdown";
            NAMES[CLOSE_NOTIFY_LOST] = "close_notify lost by shutdown";
            NAMES[ZERO_LENGTH_RECV] = "zero-length recv";
            NAMES[ZERO_LENGTH_SEND] = "zero-length send";
            NAMES[OUTCOME_START_THREW] = "startTlsSession threw";
            NAMES[OUTCOME_RECV_FAILED] = "recv failed";
            NAMES[OUTCOME_SEND_FAILED] = "send failed";
            NAMES[OUTCOME_TLSIO_FAILED] = "tlsIO failed";
            NAMES[SURVIVED] = "connection survived";
            for (int kind = 0; kind < HostileSession.KIND_COUNT; kind++) {
                NAMES[KIND_FIRST + kind] = "hostile " + HostileSession.KIND_NAMES[kind];
            }
        }
    }

    /**
     * A JDK TLS client stepped from the test thread. What it wraps waits in {@link #staged} until the test puts it
     * on the wire, so a test can hold bytes back, change them or drop them.
     */
    private static final class EngineClient {
        private static final ByteBuffer NOTHING = ByteBuffer.allocate(0);
        private static final int PUMP_GUARD = 100_000;
        final SSLEngine engine;
        SSLException failure;
        boolean isCloseNotifySeen;
        // a tolerant client records an engine failure instead of failing the test: hostile sessions break it on purpose
        boolean isFailureTolerated;
        int received;
        private final Link link;
        private final ByteBuffer netOut = ByteBuffer.allocate(64 * 1024);
        private final ByteQueue staged = new ByteQueue();
        private ByteBuffer appIn = ByteBuffer.allocate(64 * 1024);
        // the plaintext the server is going to send, in order
        private byte[] expected = EMPTY;
        private boolean isHandshakeDone;
        private boolean isWrapExhausted;
        private ByteBuffer netIn = ByteBuffer.allocate(64 * 1024);

        EngineClient(SSLContext context, Link link, String[] protocols, String[] suites, int peerPort) throws SSLException {
            this.link = link;
            // host and port only name the entry in the context's session cache
            engine = context.createSSLEngine("localhost", peerPort);
            engine.setUseClientMode(true);
            // the engine reads its configuration when the handshake begins
            engine.setEnabledProtocols(protocols);
            if (suites != null) {
                engine.setEnabledCipherSuites(suites);
            }
            engine.beginHandshake();
        }

        void assertHealthy() {
            if (failure != null) {
                throw new AssertionError("the client engine failed: " + failure, failure);
            }
        }

        void closeNotify() throws SSLException {
            engine.closeOutbound();
            wrapControl();
        }

        void expectPlaintext(byte[] expected) {
            this.expected = expected;
        }

        void flush() {
            flush(staged.available());
        }

        void flush(int len) {
            staged.moveTo(link.toServer, len);
        }

        boolean isHandshakeDone() {
            return isHandshakeDone;
        }

        /**
         * @return true when the engine has output to produce or a task to run without more bytes from the server
         */
        boolean isWorkPending() {
            if (failure != null || isWrapExhausted) {
                return false;
            }
            final SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
            return status == SSLEngineResult.HandshakeStatus.NEED_WRAP || status == SSLEngineResult.HandshakeStatus.NEED_TASK;
        }

        /**
         * Feeds the engine everything the server has written and does all the work that needs no further input.
         */
        void pump() {
            takeServerBytes();
            if (failure != null) {
                // a failed engine reads nothing more
                netIn.clear();
                return;
            }
            try {
                for (int guard = 0; ; guard++) {
                    if (guard > PUMP_GUARD) {
                        throw new AssertionError("the client engine spins [status=" + engine.getHandshakeStatus() + ']');
                    }
                    final SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
                    if (status == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                        Runnable task;
                        while ((task = engine.getDelegatedTask()) != null) {
                            task.run();
                        }
                    } else if (status == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                        if (wrapControl() == 0) {
                            isWrapExhausted = true;
                            break;
                        }
                    } else if (!unwrap()) {
                        break;
                    }
                }
                if (!isHandshakeDone && !isCloseNotifySeen
                        && engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                    isHandshakeDone = true;
                }
            } catch (SSLException e) {
                onFailure(e);
            }
        }

        int stagedSize() {
            return staged.available();
        }

        /**
         * Starts a new handshake on an established connection: a KeyUpdate in TLS 1.3, a renegotiation in TLS 1.2.
         */
        void startNewHandshake() throws SSLException {
            engine.beginHandshake();
            wrapControl();
        }

        /**
         * @return everything wrapped and not yet on the wire, which is no longer staged after the call
         */
        byte[] takeStaged() {
            return staged.takeAll();
        }

        /**
         * Wraps application data. One call produces one record.
         *
         * @return the plaintext bytes the engine took: none while it has other output to send first, or once it
         * has closed
         */
        int wrap(byte[] plain, int off, int len) throws SSLException {
            if (failure != null) {
                return 0;
            }
            try {
                netOut.clear();
                final SSLEngineResult result = engine.wrap(ByteBuffer.wrap(plain, off, len), netOut);
                netOut.flip();
                staged.append(netOut.array(), 0, netOut.remaining());
                return result.bytesConsumed();
            } catch (SSLException e) {
                if (!isFailureTolerated) {
                    throw e;
                }
                onFailure(e);
                return 0;
            }
        }

        private void onFailure(SSLException e) {
            failure = e;
            // the engine may have an alert for the server
            try {
                for (int i = 0; i < 4 && engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP; i++) {
                    if (wrapControl() == 0) {
                        break;
                    }
                }
            } catch (SSLException ignore) {
                // nothing more to say
            }
        }

        private void onPlaintext() {
            final int n = appIn.remaining();
            if (received + n > expected.length) {
                throw new AssertionError("the client received " + (received + n) + " plaintext bytes, the server was given " + expected.length);
            }
            final int mismatch = Arrays.mismatch(appIn.array(), appIn.position(), appIn.limit(), expected, received, received + n);
            if (mismatch != -1) {
                throw new AssertionError("the client received a wrong byte at stream offset " + (received + mismatch));
            }
            received += n;
        }

        private void takeServerBytes() {
            final int n = link.toClient.available();
            if (n == 0) {
                return;
            }
            isWrapExhausted = false;
            if (netIn.remaining() < n) {
                final ByteBuffer bigger = ByteBuffer.allocate(Math.max(netIn.capacity() * 2, netIn.position() + n));
                netIn.flip();
                bigger.put(netIn);
                netIn = bigger;
            }
            link.toClient.read(netIn.array(), netIn.position(), n);
            netIn.position(netIn.position() + n);
        }

        /**
         * Unwraps one record.
         *
         * @return false when the engine needs more bytes from the server to go on
         */
        private boolean unwrap() throws SSLException {
            netIn.flip();
            if (!netIn.hasRemaining()) {
                netIn.compact();
                return false;
            }
            appIn.clear();
            final SSLEngineResult result;
            try {
                result = engine.unwrap(netIn, appIn);
            } finally {
                netIn.compact();
            }
            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                isHandshakeDone = true;
            }
            appIn.flip();
            if (appIn.hasRemaining()) {
                onPlaintext();
            }
            switch (result.getStatus()) {
                case CLOSED:
                    // the server's close_notify; whatever follows it stays unread
                    isCloseNotifySeen = true;
                    return result.bytesConsumed() > 0;
                case BUFFER_UNDERFLOW:
                    return false;
                case BUFFER_OVERFLOW:
                    appIn = ByteBuffer.allocate(appIn.capacity() * 2);
                    return true;
                default:
                    return result.bytesConsumed() > 0 || result.bytesProduced() > 0 || isWorkPending();
            }
        }

        /**
         * Wraps one record of handshake, alert or KeyUpdate output.
         *
         * @return the bytes produced
         */
        private int wrapControl() throws SSLException {
            netOut.clear();
            final SSLEngineResult result = engine.wrap(NOTHING, netOut);
            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                isHandshakeDone = true;
            }
            netOut.flip();
            final int produced = netOut.remaining();
            staged.append(netOut.array(), 0, produced);
            return produced;
        }
    }

    /**
     * The server side of every connection of a test: one socket object over the fake transport, reused through
     * {@code of(fd) ... close()} the way a pooled connection context reuses its socket, and the native buffer its
     * calls work on.
     */
    private static final class Harness implements QuietCloseable {
        private static final int BUF_CAPACITY = 1 << 20;
        private static final long CANARY = 0x5ca1ab1e0ddba11L;
        // every byte the socket has buffered could be a record of its own, and each record may cost a transport call
        private static final long MAX_BUFFERED_BYTES = 1 << 20;
        final Coverage coverage;
        final JavaTlsServerSocket socket;
        final byte[] tmp = new byte[BUF_CAPACITY];
        long fd;
        // What the dispatcher learns when a handler registers for a read: plaintext is waiting inside the socket,
        // so the read is due without any event on the transport. The test samples it after recv() and send(),
        // never after tlsIO(), because the dispatcher does not look at it there either.
        boolean isReadRepublished;
        private final long buf;
        private final InMemoryNetworkFacade nf = new InMemoryNetworkFacade();
        private int bufferSize;
        private Link link;
        private long memAtHandshakeEnd;
        private long nextFd = FIRST_FAKE_FD;
        private long tlsMemBefore;

        Harness(Coverage coverage) {
            this.coverage = coverage;
            this.socket = new JavaTlsServerSocket(nf, LOG, serverContext);
            this.buf = Unsafe.malloc(BUF_CAPACITY + Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        /**
         * Gets rid of the connection of an iteration that has failed already.
         */
        void abort() {
            try {
                nf.beginOperation(Long.MAX_VALUE);
                socket.close();
            } catch (Throwable ignore) {
                // the iteration's own failure is the one to report
            }
            nf.forget();
        }

        @Override
        public void close() {
            abort();
            Unsafe.free(buf, BUF_CAPACITY + Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }

        /**
         * Closes the connection the way the context pool does and checks that nothing of it is left.
         */
        void closeAndVerify() {
            final Link closing = link;
            nf.beginOperation(spinLimit(0));
            socket.close();
            assertEquals("close() must close the fd exactly once", 1, closing.closeCalls);
            assertTrue(socket.isClosed());
            assertEquals(-1, socket.getFd());
            assertFalse(socket.isTlsSessionStarted());
            assertFalse(socket.isHandshakeInProgress());
            assertFalse(socket.wantsTlsWrite());
            assertFalse(socket.wantsTlsRead());
            assertFalse(socket.isMorePlaintextBuffered());
            assertEquals(
                    "close() must free every TLS buffer of the connection",
                    tlsMemBefore,
                    Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS)
            );
            // close() is idempotent
            socket.close();
            assertEquals(1, closing.closeCalls);
            assertTrue(socket.isClosed());
            if (closing.recvWouldBlocks > 0) {
                coverage.hit(Coverage.RECV_WOULD_BLOCK);
            }
            if (closing.sendWouldBlocks > 0) {
                coverage.hit(Coverage.SEND_WOULD_BLOCK);
            }
        }

        /**
         * @return true when a dispatcher would hand the connection to a reader: the link has bytes, an end of
         * stream or an error to report, or the socket said it holds plaintext
         */
        boolean isRecvDue() {
            return link.isReadable() || isReadRepublished;
        }

        /**
         * Takes the socket for a new connection, as the context pool does with a fresh fd.
         */
        Link open(Rnd transportRnd) {
            assertTrue("the previous connection is still open", socket.isClosed());
            fd = nextFd++;
            tlsMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            link = nf.open(fd, transportRnd);
            socket.of(fd);
            assertEquals(fd, socket.getFd());
            assertFalse(socket.isClosed());
            assertTrue(socket.supportsTls());
            assertFalse(socket.isTlsSessionStarted());
            assertFalse(socket.isHandshakeInProgress());
            isReadRepublished = false;
            memAtHandshakeEnd = -1;
            bufferSize = 0;
            return link;
        }

        /**
         * Calls recv() and leaves what it returned in {@link #tmp}.
         */
        int recv(int len) {
            Unsafe.putLong(buf + len, CANARY);
            nf.beginOperation(spinLimit(len));
            final int n = socket.recv(buf, len);
            assertEquals("recv() wrote past the caller's buffer", CANARY, Unsafe.getLong(buf + len));
            assertTrue("recv() returned more than the buffer holds [n=" + n + ", len=" + len + ']', n <= len);
            isReadRepublished = socket.isMorePlaintextBuffered();
            afterOperation();
            if (n > 0) {
                Unsafe.copyMemory(null, buf, tmp, Unsafe.BYTE_OFFSET, n);
            }
            return n;
        }

        int send(byte[] source, int off, int len) {
            Unsafe.copyMemory(source, Unsafe.BYTE_OFFSET + off, null, buf, len);
            nf.beginOperation(spinLimit(len));
            final int n = socket.send(buf, len);
            assertTrue("send() took more than it was given [n=" + n + ", len=" + len + ']', n <= len);
            isReadRepublished = socket.isMorePlaintextBuffered();
            afterOperation();
            return n;
        }

        int shutdown(int how) {
            nf.beginOperation(spinLimit(0));
            final int result = socket.shutdown(how);
            afterOperation();
            return result;
        }

        /**
         * Starts TLS with the socket's buffers at {@code bufferSize}. The size is a JVM-wide property that the
         * socket reads when the session starts, so it is set for this call only.
         */
        void startTls(int bufferSize) throws TlsSessionInitFailedException {
            this.bufferSize = bufferSize;
            final String previous = System.getProperty(BUFFER_SIZE_PROPERTY);
            System.setProperty(BUFFER_SIZE_PROPERTY, Integer.toString(bufferSize));
            try {
                nf.beginOperation(spinLimit(0));
                socket.startTlsSession(null);
            } finally {
                if (previous == null) {
                    System.clearProperty(BUFFER_SIZE_PROPERTY);
                } else {
                    System.setProperty(BUFFER_SIZE_PROPERTY, previous);
                }
            }
            assertTrue(socket.isTlsSessionStarted());
            assertTrue("a handshake cannot complete before the client has answered", socket.isHandshakeInProgress());
            assertFalse(socket.wantsTlsRead());
            afterOperation();
        }

        int tlsIO(int flags) {
            nf.beginOperation(spinLimit(0));
            final int n = socket.tlsIO(flags);
            afterOperation();
            return n;
        }

        private void afterOperation() {
            if (!socket.isTlsSessionStarted()) {
                return;
            }
            final long used = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMemBefore;
            if (socket.isHandshakeInProgress()) {
                // a session starts with an input and an output buffer of the configured size
                if (used > 2L * bufferSize) {
                    coverage.hit(Coverage.GROWTH_IN_HANDSHAKE);
                }
                if (socket.wantsTlsWrite()) {
                    coverage.hit(Coverage.HANDSHAKE_OUTPUT_PENDING);
                }
            } else if (memAtHandshakeEnd < 0) {
                memAtHandshakeEnd = used;
            } else if (used > memAtHandshakeEnd) {
                coverage.hit(Coverage.GROWTH_AFTER_HANDSHAKE);
                memAtHandshakeEnd = used;
            }
        }

        private long spinLimit(int len) {
            return 1_024L + len + link.toServer.available() + MAX_BUFFERED_BYTES;
        }
    }

    /**
     * One connection with a client that misbehaves in one chosen way. Whatever the client does, the server may
     * only end in one of the terminal outcomes, may only deliver a prefix of the plaintext the client wrapped,
     * and must come back clean after {@code close()}.
     */
    private static final class HostileSession {
        // not a fault: a client whose configuration the server may have to refuse
        static final int KIND_CLIENT_CONFIG = -1;
        static final int KIND_CLOSE_NOTIFY = 10;
        static final int KIND_COUNT = 13;
        static final int KIND_DATA_BIT_FLIP = 3;
        static final int KIND_DATA_INFLATED_LENGTH = 5;
        static final int KIND_DATA_PLAINTEXT_ALERT = 8;
        static final int KIND_DATA_REPLAY = 6;
        static final int KIND_DATA_SWAP = 7;
        static final int KIND_DATA_TRUNCATED = 4;
        static final int KIND_FLIGHT_FAULT = 2;
        static final int KIND_GARBAGE = 0;
        static final int KIND_KEY_UPDATE_BURST = 12;
        static final int KIND_MUTATED_HELLO = 1;
        static final int KIND_PEER_RESET = 9;
        static final int KIND_RENEGOTIATION = 11;
        private static final String[] KIND_NAMES = {
                "garbage first flight",
                "mutated ClientHello",
                "tampered second flight",
                "bit flip in a record",
                "truncated record",
                "inflated record length",
                "replayed record",
                "swapped records",
                "plaintext alert between records",
                "peer reset",
                "close_notify",
                "renegotiation",
                "KeyUpdate burst"
        };
        private static final int OUTCOME_NONE = 0;
        private static final String[] OUTCOME_NAMES = {"none", "recv failed", "send failed", "start threw", "tlsIO failed"};
        private static final int OUTCOME_RECV_FAILED = 1;
        private static final int OUTCOME_SEND_FAILED = 2;
        private static final int OUTCOME_START_THREW = 3;
        private static final int OUTCOME_TLSIO_FAILED = 4;
        private final int bufferSize;
        private final ClientConfig config;
        private final Coverage coverage;
        // what the server sends back, in order
        private final byte[] echo;
        private final Harness h;
        private final boolean isPgStyle;
        private final int kind;
        // what the client wraps, in order
        private final byte[] plain;
        private final int preQueued;
        private final int recvLenClass;
        private final Rnd rnd;
        private EngineClient client;
        // plaintext recv() has returned
        private int delivered;
        // the stream offset recv() must never get past: where the tampering starts
        private int deliveredLimit = Integer.MAX_VALUE;
        private int echoOffered;
        private int echoSent;
        private int failureCode;
        private boolean isAutoFlush = true;
        private boolean isCloseNotifyLast;
        private boolean isEofBeforeStart;
        private boolean isEofSent;
        private boolean isEofSentToForceOutcome;
        private boolean isStarted;
        // set when the script put bytes on the wire that leave the client and the server out of step
        private boolean isStreamPoisoned;
        private Link link;
        private int outcome = OUTCOME_NONE;
        private int queuedBeforeStart;
        private String startFailure;
        private long steps;
        private boolean wasHandshakingAtFailure;
        // plaintext the client engine has taken
        private int wrapped;

        HostileSession(Harness h, Rnd rnd, int kind, ClientConfig config) {
            this.h = h;
            this.rnd = rnd;
            this.kind = kind;
            this.coverage = h.coverage;
            this.config = config != null ? config : switch (kind) {
                case KIND_RENEGOTIATION -> ClientConfig.of(TLS_12);
                case KIND_KEY_UPDATE_BURST -> ClientConfig.of(TLS_13);
                default -> ClientConfig.defaults(rnd);
            };
            this.bufferSize = BUFFER_SIZES[rnd.nextInt(BUFFER_SIZES.length)];
            this.isPgStyle = rnd.nextInt(4) == 0;
            this.preQueued = rnd.nextInt(3);
            this.recvLenClass = rnd.nextInt(IO_LEN_CLASSES);
            this.plain = randomBytes(rnd, 96 * 1024);
            this.echo = randomBytes(rnd, 8 * 1024);
        }

        @Override
        public String toString() {
            return "hostile kind=" + (kind == KIND_CLIENT_CONFIG ? "client configuration" : KIND_NAMES[kind])
                    + ", " + config
                    + ", bufferSize=" + bufferSize
                    + ", pgStyle=" + isPgStyle
                    + ", preQueued=" + preQueued
                    + ", recvLenClass=" + recvLenClass
                    + ", outcome=" + OUTCOME_NAMES[outcome]
                    + ", code=" + failureCode
                    + ", startFailure=" + startFailure
                    + ", wrapped=" + wrapped
                    + ", delivered=" + delivered
                    + ", deliveredLimit=" + deliveredLimit
                    + ", eofSent=" + isEofSent
                    + (link != null ? ", " + link : "");
        }

        void run() throws Exception {
            if (kind != KIND_CLIENT_CONFIG) {
                coverage.hit(Coverage.KIND_FIRST + kind);
            }
            link = h.open(new Rnd(rnd.nextLong(), rnd.nextLong()));
            link.recvChunk = Scenario.chunk(rnd);
            link.sendChunk = Scenario.chunk(rnd);
            link.recvWouldBlockPct = Scenario.wouldBlockPct(rnd);
            link.sendWouldBlockPct = Scenario.wouldBlockPct(rnd);
            link.sendErrorCode = rnd.nextBoolean() ? Net.EPEERDISCONNECT : Net.EOTHERDISCONNECT;
            switch (kind) {
                case KIND_CLIENT_CONFIG -> scriptClientConfig();
                case KIND_GARBAGE -> scriptGarbage();
                case KIND_MUTATED_HELLO -> scriptMutatedHello();
                case KIND_FLIGHT_FAULT -> scriptFlightFault();
                case KIND_PEER_RESET -> scriptPeerReset();
                case KIND_CLOSE_NOTIFY -> scriptCloseNotify();
                default -> scriptDataFault();
            }
            finish();
            conclude();
        }

        /**
         * Puts the client's first bytes on the wire and starts TLS, in the order and with the split this session
         * drew: some of the bytes may be there before {@code startTlsSession()}, the rest arrives after it.
         */
        private void begin(byte[] firstFlight) {
            if (isPgStyle) {
                link.toServer.append(TlsFuzzSupport.PG_SSL_REQUEST, 0, TlsFuzzSupport.PG_SSL_REQUEST.length);
            }
            queuedBeforeStart = switch (preQueued) {
                case Scenario.QUEUED_ALL -> firstFlight.length;
                case Scenario.QUEUED_PART -> firstFlight.length > 1 ? 1 + rnd.nextInt(firstFlight.length - 1) : 0;
                default -> 0;
            };
            link.toServer.append(firstFlight, 0, queuedBeforeStart);
            if (isPgStyle) {
                pgUpgrade(h, link);
            }
            if (isEofBeforeStart) {
                link.closeFromPeer(rnd.nextBoolean() ? Net.EPEERDISCONNECT : Net.EOTHERDISCONNECT);
                isEofSent = true;
            }
            try {
                h.startTls(bufferSize);
                isStarted = true;
            } catch (TlsSessionInitFailedException e) {
                // the exception is a flyweight: read its message before anything else reuses it
                startFailure = e.getFlyweightMessage().toString();
                assertTrue("an error message must be printable ASCII: " + startFailure, TlsFuzzSupport.isPrintableAscii(startFailure));
                assertTrue("an error message must stay short: " + startFailure.length(), startFailure.length() <= MAX_ERROR_MESSAGE_LENGTH);
                assertFalse("a failed start must leave no TLS session behind", h.socket.isTlsSessionStarted());
                assertFalse(h.socket.isHandshakeInProgress());
                outcome = OUTCOME_START_THREW;
            }
            link.toServer.append(firstFlight, queuedBeforeStart, firstFlight.length - queuedBeforeStart);
        }

        private boolean clientStep() {
            if (client == null) {
                return false;
            }
            final boolean hadWork = link.toClient.available() > 0 || client.isWorkPending();
            client.pump();
            assertTrue("the client received plaintext the server never sent", client.received <= echoOffered);
            if (isAutoFlush && client.stagedSize() > 0) {
                client.flush();
                return true;
            }
            return hadWork;
        }

        private void conclude() {
            assertTrue("the session ended without a terminal outcome", outcome != OUTCOME_NONE);
            assertTrue("recv() delivered plaintext from behind the tampering", delivered <= deliveredLimit);
            final JavaTlsServerSocket socket = h.socket;
            if (outcome == OUTCOME_START_THREW) {
                coverage.hit(Coverage.OUTCOME_START_THREW);
            } else {
                coverage.hit(outcome == OUTCOME_RECV_FAILED ? Coverage.OUTCOME_RECV_FAILED
                        : outcome == OUTCOME_SEND_FAILED ? Coverage.OUTCOME_SEND_FAILED : Coverage.OUTCOME_TLSIO_FAILED);
                if (!isTransportFaulty()) {
                    assertEquals("a failure of the TLS layer is reported as -1", -1, failureCode);
                }
                if (wasHandshakingAtFailure && outcome != OUTCOME_TLSIO_FAILED) {
                    assertEquals("a failed handshake is reported as -1", -1, failureCode);
                    // a failed handshake stays failed, whatever the caller tries next
                    for (int i = 0; i < 2; i++) {
                        assertEquals(-1, h.recv(64));
                        assertEquals(-1, h.send(PROBE, 0, 1));
                    }
                    assertTrue(socket.isHandshakeInProgress());
                    assertTrue(socket.isTlsSessionStarted());
                }
            }
            if (isEofBeforeStart && queuedBeforeStart == 0) {
                // the first read of the handshake meets the end of the stream
                assertEquals(OUTCOME_START_THREW, outcome);
                assertEquals("socket read error", startFailure);
            }
            if (isCloseNotifyLast) {
                assertEquals("everything in front of the close_notify must arrive", wrapped, delivered);
            }
            if (kind == KIND_CLIENT_CONFIG && client != null && config.expectation == ClientConfig.MUST_FAIL) {
                concludeRefusedConfig();
            } else {
                h.closeAndVerify();
            }
            if (client != null) {
                client.pump();
                assertTrue("the client received plaintext the server never sent", client.received <= echoOffered);
            }
        }

        /**
         * The server has no cipher suite for this ClientHello. It must say so at once, and where the engine that
         * knows the reason is still there, {@code close()} sends its handshake_failure alert.
         */
        private void concludeRefusedConfig() {
            assertFalse("the server did not refuse the ClientHello until the client hung up", isEofSentToForceOutcome);
            assertTrue(
                    "a ClientHello without a usable cipher suite must fail the handshake",
                    outcome == OUTCOME_START_THREW || (outcome == OUTCOME_RECV_FAILED && wasHandshakingAtFailure)
            );
            coverage.hit(Coverage.CONFIG_REJECTED);
            if (outcome != OUTCOME_RECV_FAILED) {
                h.closeAndVerify();
                return;
            }
            link.clearFaults();
            final long mark = link.toClient.totalWritten;
            h.closeAndVerify();
            final long emitted = link.toClient.totalWritten - mark;
            if (bufferSize >= LARGE_BUFFER_SIZE) {
                assertEquals("close() must send the alert of a failed handshake", TlsFuzzSupport.ALERT_RECORD_LENGTH, emitted);
            } else {
                // a small output buffer that the handshake never grew has no room for the alert
                assertTrue("close() wrote " + emitted + " bytes after a failed handshake", emitted == 0 || emitted == TlsFuzzSupport.ALERT_RECORD_LENGTH);
            }
            if (emitted > 0) {
                final byte[] alert = link.toClient.copyTail(TlsFuzzSupport.ALERT_RECORD_LENGTH);
                assertEquals(TlsFuzzSupport.RECORD_TYPE_ALERT, alert[0]);
                assertEquals(0x03, alert[1]);
                assertEquals(0, alert[3]);
                assertEquals(2, alert[4]);
                assertEquals(TlsFuzzSupport.ALERT_LEVEL_FATAL, alert[5]);
                assertEquals(TlsFuzzSupport.ALERT_DESCRIPTION_HANDSHAKE_FAILURE, alert[6]);
                client.pump();
                assertNotNull("the client must learn of the refusal from the alert", client.failure);
                coverage.hit(Coverage.ALERT_DELIVERED);
            }
        }

        /**
         * Sends a little data over a connection that has come through the tampering unharmed: it must then work
         * like any other.
         */
        private void exchangeIfHealthy() throws SSLException {
            if (isStreamPoisoned || !isHealthy()) {
                return;
            }
            wrapRecords(1 + rnd.nextInt(4));
            client.flush();
            settle();
            if (outcome == OUTCOME_NONE) {
                assertEquals("a connection that came through the tampering must carry the whole stream", wrapped, delivered);
                coverage.hit(Coverage.SURVIVED);
            }
        }

        /**
         * Lets both sides run until nobody moves, then has the client hang up, which forces a terminal outcome.
         */
        private void finish() {
            if (outcome == OUTCOME_NONE) {
                settle();
            }
            if (outcome != OUTCOME_NONE) {
                return;
            }
            if (!isEofSent) {
                link.closeFromPeer(rnd.nextBoolean() ? Net.EPEERDISCONNECT : Net.EOTHERDISCONNECT);
                // a peer that is gone for good takes no more bytes either
                link.isSendRejected = rnd.nextBoolean();
                isEofSent = true;
                isEofSentToForceOutcome = true;
            }
            var lastSignature = -1L;
            var idle = 0;
            while (outcome == OUTCOME_NONE) {
                assertTrue("no terminal outcome after the peer closed", steps++ < MAX_SESSION_STEPS);
                assertTrue("recv() is not due although the peer has closed", h.isRecvDue());
                serverStep();
                final long signature = signature();
                if (signature != lastSignature) {
                    lastSignature = signature;
                    idle = 0;
                } else {
                    assertTrue("recv() keeps returning 0 after the peer closed", ++idle < MAX_IDLE_STEPS);
                }
            }
        }

        /**
         * Runs an untampered handshake to its end.
         */
        private void handshake() throws Exception {
            client = newClient();
            assertNotNull("the client could not start", client);
            begin(client.takeStaged());
            settle();
            assertEquals("an untampered handshake must not fail [startFailure=" + startFailure + ", code=" + failureCode + ']', OUTCOME_NONE, outcome);
            assertNull("the client failed an untampered handshake", client.failure);
            assertTrue(client.isHandshakeDone());
            assertFalse(h.socket.isHandshakeInProgress());
        }

        private boolean isHealthy() {
            return outcome == OUTCOME_NONE
                    && isStarted
                    && client != null
                    && client.failure == null
                    && client.isHandshakeDone()
                    && !h.socket.isHandshakeInProgress();
        }

        private boolean isTransportFaulty() {
            return isEofSent || link.recvLimit != Long.MAX_VALUE || link.sendLimit != Long.MAX_VALUE;
        }

        private void keyUpdateBurst() throws Exception {
            final int updates = 1 + rnd.nextInt(32);
            for (int i = 0; i < updates; i++) {
                client.startNewHandshake();
                if (rnd.nextBoolean()) {
                    wrapRecords(1);
                }
            }
            wrapRecords(1);
            client.flush();
            settle();
            // key updates are part of TLS 1.3: the stream survives them in both directions
            assertEquals("a KeyUpdate must not fail the connection [code=" + failureCode + ']', OUTCOME_NONE, outcome);
            assertNull("the client failed after its own key updates", client.failure);
            assertEquals(wrapped, delivered);
            sendEcho(1 + rnd.nextInt(2_000));
            settle();
            assertEquals(OUTCOME_NONE, outcome);
            assertNull(client.failure);
            assertEquals("the client must read what the server sent after the key updates", echoSent, client.received);
            coverage.hit(Coverage.SURVIVED);
        }

        /**
         * @return a client with its ClientHello staged, or null when the configuration leaves it nothing to offer
         */
        private EngineClient newClient() throws Exception {
            final EngineClient candidate;
            try {
                candidate = new EngineClient(TlsFuzzSupport.newTrustAllContext(), link, config.protocols, config.suites, 1);
            } catch (SSLException e) {
                return null;
            }
            candidate.isFailureTolerated = true;
            candidate.expectPlaintext(echo);
            candidate.pump();
            return candidate.failure == null && candidate.stagedSize() > 0 ? candidate : null;
        }

        private void renegotiate() throws Exception {
            deliveredLimit = wrapped;
            isStreamPoisoned = true;
            try {
                client.startNewHandshake();
            } catch (SSLException e) {
                // this client does not renegotiate: nothing hostile went out
                return;
            }
            if (client.stagedSize() == 0) {
                return;
            }
            // data behind the new ClientHello, which the server must not deliver
            wrapRecords(1 + rnd.nextInt(2));
            client.flush();
            isAutoFlush = false;
            settle();
            // this server does not renegotiate: it refuses at once, without waiting for the client to hang up
            assertEquals("a renegotiation must fail recv()", OUTCOME_RECV_FAILED, outcome);
            assertEquals(-1, failureCode);
            assertEquals("after a refused renegotiation send() fails too", -1, h.send(PROBE, 0, 1));
        }

        private void scriptClientConfig() throws Exception {
            client = newClient();
            if (client == null) {
                // nothing to offer: the client connects and leaves
                coverage.hit(Coverage.CLIENT_CANNOT_START);
                isEofBeforeStart = rnd.nextInt(4) == 0;
                begin(EMPTY);
                return;
            }
            begin(client.takeStaged());
            settle();
            exchangeIfHealthy();
        }

        private void scriptCloseNotify() throws Exception {
            switch (rnd.nextInt(4)) {
                case 0 -> {
                    // a plaintext close_notify where the ClientHello belongs
                    deliveredLimit = 0;
                    isStreamPoisoned = true;
                    begin(PLAINTEXT_CLOSE_NOTIFY.clone());
                }
                case 1 -> {
                    // a ClientHello with a plaintext close_notify right behind it
                    client = newClient();
                    assertNotNull("the client could not start", client);
                    deliveredLimit = 0;
                    isStreamPoisoned = true;
                    isAutoFlush = false;
                    begin(concat(client.takeStaged(), PLAINTEXT_CLOSE_NOTIFY));
                }
                default -> {
                    // a real close_notify behind some records, with or without junk after it
                    handshake();
                    wrapRecords(rnd.nextInt(4));
                    client.closeNotify();
                    client.flush();
                    isAutoFlush = false;
                    isStreamPoisoned = true;
                    if (rnd.nextBoolean()) {
                        final byte[] junk = randomBytes(rnd, 1 + rnd.nextInt(500));
                        link.toServer.append(junk, 0, junk.length);
                    }
                    isCloseNotifyLast = true;
                }
            }
        }

        private void scriptDataFault() throws Exception {
            handshake();
            if (rnd.nextBoolean()) {
                // some honest traffic first
                wrapRecords(1 + rnd.nextInt(3));
                client.flush();
                settle();
                assertEquals(OUTCOME_NONE, outcome);
                assertEquals(wrapped, delivered);
            }
            switch (kind) {
                case KIND_RENEGOTIATION -> renegotiate();
                case KIND_KEY_UPDATE_BURST -> keyUpdateBurst();
                default -> tamperWithRecords();
            }
        }

        private void scriptFlightFault() throws Exception {
            client = newClient();
            assertNotNull("the client could not start", client);
            begin(client.takeStaged());
            // the handshake runs until the client has its second flight ready; that flight is what gets hurt
            isAutoFlush = false;
            for (int rounds = 0; outcome == OUTCOME_NONE && client.stagedSize() == 0; rounds++) {
                assertTrue("the client never produced its second flight", rounds < 10_000_000);
                final boolean isClientBusy = clientStep();
                final boolean isServerBusy = serverStep();
                if (!isClientBusy && !isServerBusy) {
                    break;
                }
            }
            if (outcome == OUTCOME_NONE && client.stagedSize() > 0) {
                byte[] flight = client.takeStaged();
                switch (rnd.nextInt(5)) {
                    case 0 -> flipBit(flight, rnd.nextInt(flight.length));
                    case 1 -> {
                        // the rest of the flight never comes
                        flight = Arrays.copyOf(flight, rnd.nextInt(flight.length));
                        isStreamPoisoned = true;
                    }
                    case 2 -> {
                        flight = PLAINTEXT_CLOSE_NOTIFY.clone();
                        isStreamPoisoned = true;
                    }
                    case 3 -> {
                        flight = concat(flight, randomBytes(rnd, 1 + rnd.nextInt(500)));
                        isStreamPoisoned = true;
                    }
                    default -> {
                        flight = concat(randomBytes(rnd, 1 + rnd.nextInt(500)), flight);
                        isStreamPoisoned = true;
                    }
                }
                if (isStreamPoisoned) {
                    deliveredLimit = 0;
                }
                link.toServer.append(flight, 0, flight.length);
            }
            isAutoFlush = !isStreamPoisoned;
            settle();
            exchangeIfHealthy();
        }

        private void scriptGarbage() {
            deliveredLimit = 0;
            isStreamPoisoned = true;
            isEofBeforeStart = rnd.nextInt(4) == 0;
            begin(TlsFuzzSupport.hostileFirstFlight(rnd));
        }

        private void scriptMutatedHello() throws Exception {
            client = newClient();
            assertNotNull("the client could not start", client);
            byte[] hello = client.takeStaged();
            switch (rnd.nextInt(4)) {
                case 0 -> {
                    for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                        flipBit(hello, rnd.nextInt(hello.length));
                    }
                }
                case 1 -> {
                    hello = Arrays.copyOf(hello, 1 + rnd.nextInt(hello.length - 1));
                    isStreamPoisoned = true;
                }
                case 2 -> {
                    hello = concat(hello, randomBytes(rnd, 1 + rnd.nextInt(2_000)));
                    isStreamPoisoned = true;
                }
                default -> {
                    hello = concat(hello, hello);
                    isStreamPoisoned = true;
                }
            }
            if (isStreamPoisoned) {
                deliveredLimit = 0;
                isAutoFlush = false;
            }
            begin(hello);
            settle();
            exchangeIfHealthy();
        }

        private void scriptPeerReset() throws Exception {
            client = newClient();
            assertNotNull("the client could not start", client);
            begin(client.takeStaged());
            if (outcome != OUTCOME_NONE) {
                return;
            }
            // the peer vanishes after a byte budget, in one direction or in both
            final int direction = rnd.nextInt(3);
            if (direction != 1) {
                link.recvLimit = link.toServer.totalRead + rnd.nextInt(6_000);
            }
            if (direction != 0) {
                link.sendLimit = link.toClient.totalWritten + rnd.nextInt(6_000);
            }
            settle();
            if (isHealthy()) {
                wrapRecords(1 + rnd.nextInt(6));
                client.flush();
                settle();
                if (outcome == OUTCOME_NONE) {
                    // the server keeps sending until the budget ends
                    sendEcho(echo.length);
                }
            }
        }

        private void sendEcho(int total) {
            final int target = Math.min(echo.length, echoSent + total);
            for (int tries = 0; echoSent < target && outcome == OUTCOME_NONE; tries++) {
                assertTrue("send() makes no progress", tries < 10_000_000);
                final int len = Math.min(target - echoSent, 1 + rnd.nextInt(1_024));
                // the bytes of a call that fails may still have left before the failure
                // only grows: a short write may have encrypted bytes that a later, shorter call does not present
                echoOffered = Math.max(echoOffered, echoSent + len);
                final int n = h.send(echo, echoSent, len);
                if (n < 0) {
                    terminate(OUTCOME_SEND_FAILED, n);
                    return;
                }
                echoSent += n;
                echoOffered = Math.max(echoOffered, echoSent);
                serverStep();
                clientStep();
            }
        }

        /**
         * One step of the server under the dispatcher's rules.
         *
         * @return false when the server has nothing to do
         */
        private boolean serverStep() {
            if (outcome != OUTCOME_NONE || !isStarted) {
                return false;
            }
            final JavaTlsServerSocket socket = h.socket;
            if (h.isRecvDue()) {
                final int len = rnd.nextInt(32) == 0 ? 0 : Scenario.ioLen(rnd, recvLenClass);
                final int n = h.recv(len);
                if (n < 0) {
                    terminate(OUTCOME_RECV_FAILED, n);
                    return true;
                }
                if (socket.isHandshakeInProgress()) {
                    assertEquals("recv() must deliver nothing during the handshake", 0, n);
                }
                if (n > 0) {
                    assertTrue(
                            "recv() returned plaintext the client never wrapped [delivered=" + delivered + ", n=" + n + ", wrapped=" + wrapped + ']',
                            delivered + n <= wrapped
                    );
                    final int mismatch = Arrays.mismatch(h.tmp, 0, n, plain, delivered, delivered + n);
                    if (mismatch != -1) {
                        fail("recv() returned a byte the client did not send at stream offset " + (delivered + mismatch));
                    }
                    delivered += n;
                    assertTrue("recv() delivered plaintext from behind the tampering", delivered <= deliveredLimit);
                }
                return true;
            }
            if (socket.wantsTlsWrite() && !link.isSendBlocked) {
                final int n = h.tlsIO(Socket.WRITE_FLAG);
                if (n < 0) {
                    terminate(OUTCOME_TLSIO_FAILED, n);
                } else {
                    assertEquals(0, n);
                }
                return true;
            }
            return false;
        }

        /**
         * Runs both sides until nobody has anything to do or the server has reached a terminal outcome.
         */
        private void settle() {
            var lastSignature = -1L;
            var idle = 0;
            for (; ; ) {
                assertTrue("the session does not settle", steps++ < MAX_SESSION_STEPS);
                final boolean isClientBusy = clientStep();
                final boolean isServerBusy = serverStep();
                if (!isClientBusy && !isServerBusy) {
                    return;
                }
                final long signature = signature();
                if (signature != lastSignature) {
                    lastSignature = signature;
                    idle = 0;
                } else {
                    assertTrue("nothing has moved for " + MAX_IDLE_STEPS + " steps", ++idle < MAX_IDLE_STEPS);
                }
            }
        }

        private long signature() {
            return link.toServer.totalRead + link.toServer.totalWritten
                    + link.toClient.totalRead + link.toClient.totalWritten
                    + delivered + echoSent + outcome;
        }

        /**
         * Wraps a few records and breaks one of them, or the order they come in, before they go on the wire.
         */
        private void tamperWithRecords() throws Exception {
            final int wanted = 2 + rnd.nextInt(4);
            final int[] plainEnds = new int[wanted];
            final int plainStart = wrapped;
            var count = 0;
            while (count < wanted) {
                final int before = wrapped;
                wrapRecords(1);
                if (wrapped == before) {
                    break;
                }
                plainEnds[count++] = wrapped;
            }
            byte[] wire = client.takeStaged();
            final int[] starts = recordStarts(wire);
            isStreamPoisoned = true;
            isAutoFlush = false;
            if (starts == null || starts.length - 1 != count || count < 2) {
                // not the record layout this script counts on: break one byte and let the general rules judge
                if (wire.length > 0) {
                    flipBit(wire, rnd.nextInt(wire.length));
                }
                link.toServer.append(wire, 0, wire.length);
                return;
            }
            final int j = rnd.nextInt(count);
            final int recordStart = starts[j];
            final int recordEnd = starts[j + 1];
            final int plainBefore = j == 0 ? plainStart : plainEnds[j - 1];
            switch (kind) {
                case KIND_DATA_BIT_FLIP -> {
                    final int at = switch (rnd.nextInt(3)) {
                        case 0 -> recordStart + rnd.nextInt(TlsFuzzSupport.RECORD_HEADER_LENGTH);
                        case 1 -> recordEnd - 1 - rnd.nextInt(Math.min(16, recordEnd - recordStart));
                        default -> recordStart + rnd.nextInt(recordEnd - recordStart);
                    };
                    flipBit(wire, at);
                    // JSSE authenticates the type and the length of a record header, not the version in bytes
                    // 1 and 2: it only refuses a version below SSL 3.0. A flip there can leave the stream intact,
                    // so it sets no limit and the general rules judge the outcome.
                    final int headerOffset = at - recordStart;
                    if (headerOffset != 1 && headerOffset != 2) {
                        deliveredLimit = plainBefore;
                    }
                }
                case KIND_DATA_TRUNCATED -> {
                    // cut inside the record: its rest never comes
                    wire = Arrays.copyOf(wire, recordStart + 1 + rnd.nextInt(recordEnd - recordStart - 1));
                    deliveredLimit = plainBefore;
                }
                case KIND_DATA_INFLATED_LENGTH -> {
                    final int length = 16_385 + rnd.nextInt(65_535 - 16_385 + 1);
                    wire[recordStart + 3] = (byte) (length >> 8);
                    wire[recordStart + 4] = (byte) length;
                    if (rnd.nextBoolean()) {
                        // with enough bytes behind the header to fill the length it claims
                        wire = concat(wire, randomBytes(rnd, length));
                    }
                    deliveredLimit = plainBefore;
                }
                case KIND_DATA_REPLAY -> {
                    // the record once more, right behind itself: the first copy is honest
                    wire = concat(Arrays.copyOf(wire, recordEnd), Arrays.copyOfRange(wire, recordStart, wire.length));
                    deliveredLimit = plainEnds[j];
                }
                case KIND_DATA_SWAP -> {
                    final int first = Math.min(j, count - 2);
                    wire = swapRecords(wire, starts[first], starts[first + 1], starts[first + 2]);
                    deliveredLimit = first == 0 ? plainStart : plainEnds[first - 1];
                }
                default -> {
                    // a plaintext alert between two records of an encrypted stream
                    final byte[] alert = {
                            TlsFuzzSupport.RECORD_TYPE_ALERT, 0x03, 0x03, 0x00, 0x02,
                            (byte) (1 + rnd.nextInt(2)),
                            (byte) (rnd.nextBoolean() ? 0 : rnd.nextInt(121))
                    };
                    wire = concat(concat(Arrays.copyOf(wire, recordStart), alert), Arrays.copyOfRange(wire, recordStart, wire.length));
                }
            }
            link.toServer.append(wire, 0, wire.length);
        }

        private void terminate(int outcome, int code) {
            this.outcome = outcome;
            this.failureCode = code;
            this.wasHandshakingAtFailure = h.socket.isHandshakeInProgress();
            assertTrue("unexpected error code " + code, code == Net.EPEERDISCONNECT || code == Net.EOTHERDISCONNECT);
        }

        private void wrapRecords(int count) throws SSLException {
            for (int i = 0; i < count && wrapped < plain.length; i++) {
                final int len = Math.min(plain.length - wrapped, 1 + rnd.nextInt(rnd.nextBoolean() ? 64 : 16_384));
                final int consumed = client.wrap(plain, wrapped, len);
                if (consumed == 0) {
                    return;
                }
                wrapped += consumed;
            }
        }
    }

    /**
     * The transport under the socket. {@link JavaTlsServerSocket} does every raw read, write, shutdown and close
     * through the network facade of its plain delegate, so a facade keyed by a made-up fd is a complete
     * connection: no real socket and no port is involved.
     */
    private static final class InMemoryNetworkFacade extends NetworkFacadeImpl {
        private long callLimit = Long.MAX_VALUE;
        private long callsInOperation;
        private Link link;
        private long openFd = -1;
        private Rnd rnd;

        @Override
        public int close(long fd) {
            onClose(fd);
            return 0;
        }

        @Override
        public void close(long fd, Log logger) {
            onClose(fd);
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            final Link link = linkOf(fd);
            countCall();
            link.recvCalls++;
            if (bufferLen <= 0) {
                // recv(2) into an empty buffer returns 0, which Net.recv() reports as a disconnect
                return Net.EOTHERDISCONNECT;
            }
            final int readable = link.readableBytes();
            if (readable == 0) {
                // the peer has closed, or it has nothing more to say for now
                return link.isPeerClosed || link.toServer.totalRead >= link.recvLimit ? link.recvCloseCode : 0;
            }
            if (link.recvWouldBlockPct > 0 && rnd.nextInt(100) < link.recvWouldBlockPct) {
                link.recvWouldBlocks++;
                return 0;
            }
            int n = Math.min(bufferLen, readable);
            if (link.recvChunk != Integer.MAX_VALUE) {
                n = Math.min(n, 1 + rnd.nextInt(link.recvChunk));
            }
            link.toServer.readToNative(buffer, n);
            return n;
        }

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            final Link link = linkOf(fd);
            countCall();
            link.sendCalls++;
            if (link.isWriteShutdown || link.isSendRejected || link.toClient.totalWritten >= link.sendLimit) {
                return link.sendErrorCode;
            }
            if (bufferLen <= 0) {
                return 0;
            }
            if (link.isSendBlocked || (link.sendWouldBlockPct > 0 && rnd.nextInt(100) < link.sendWouldBlockPct)) {
                link.sendWouldBlocks++;
                return 0;
            }
            long n = Math.min(bufferLen, link.sendLimit - link.toClient.totalWritten);
            if (link.sendChunk != Integer.MAX_VALUE) {
                n = Math.min(n, 1 + rnd.nextInt(link.sendChunk));
            }
            link.toClient.appendFromNative(buffer, (int) n);
            return (int) n;
        }

        @Override
        public int shutdown(long fd, int how) {
            final Link link = linkOf(fd);
            countCall();
            if (how == Net.SHUT_WR) {
                link.shutdownWriteCalls++;
                link.isWriteShutdown = true;
            } else {
                // production code never shuts the read side down; the link keeps delivering and only counts the call
                link.shutdownReadCalls++;
            }
            return 0;
        }

        /**
         * Starts counting the transport calls of one socket call. A call that comes back more often than
         * {@code callLimit} is spinning.
         */
        void beginOperation(long callLimit) {
            this.callLimit = callLimit;
            this.callsInOperation = 0;
        }

        void forget() {
            openFd = -1;
        }

        Link open(long fd, Rnd rnd) {
            if (openFd != -1) {
                throw new AssertionError("fd " + openFd + " was never closed");
            }
            this.openFd = fd;
            this.rnd = rnd;
            this.link = new Link();
            return link;
        }

        private void countCall() {
            if (++callsInOperation > callLimit) {
                throw new TransportSpinError("one socket call made " + callsInOperation + " transport calls: it spins");
            }
        }

        private Link linkOf(long fd) {
            if (fd != openFd) {
                throw new AssertionError("transport call on a closed fd [fd=" + fd + ", open=" + openFd + ']');
            }
            return link;
        }

        private void onClose(long fd) {
            linkOf(fd).closeCalls++;
            openFd = -1;
        }
    }

    /**
     * One fake connection: a queue per direction and the faults the transport injects.
     */
    private static final class Link {
        final ByteQueue toClient = new ByteQueue();
        final ByteQueue toServer = new ByteQueue();
        int closeCalls;
        // the peer has closed: once the queue is empty a read reports recvCloseCode
        boolean isPeerClosed;
        // the peer's window is full: writes take nothing
        boolean isSendBlocked;
        // the peer is gone for good: writes fail
        boolean isSendRejected;
        boolean isWriteShutdown;
        long recvCalls;
        // a read takes between one and this many bytes
        int recvChunk = Integer.MAX_VALUE;
        int recvCloseCode = Net.EOTHERDISCONNECT;
        // the peer resets the connection after this many bytes: what it sent later never arrives
        long recvLimit = Long.MAX_VALUE;
        int recvWouldBlockPct;
        long recvWouldBlocks;
        long sendCalls;
        int sendChunk = Integer.MAX_VALUE;
        int sendErrorCode = Net.EOTHERDISCONNECT;
        // the peer stops taking bytes after this many
        long sendLimit = Long.MAX_VALUE;
        int sendWouldBlockPct;
        long sendWouldBlocks;
        int shutdownReadCalls;
        int shutdownWriteCalls;

        @Override
        public String toString() {
            return "recvChunk=" + recvChunk
                    + ", recvWouldBlockPct=" + recvWouldBlockPct
                    + ", sendChunk=" + sendChunk
                    + ", sendWouldBlockPct=" + sendWouldBlockPct
                    + ", recvLimit=" + recvLimit
                    + ", sendLimit=" + sendLimit
                    + ", toServer=" + toServer.totalRead + '/' + toServer.totalWritten
                    + ", toClient=" + toClient.totalRead + '/' + toClient.totalWritten;
        }

        /**
         * Leaves a transport that takes and gives everything at once. The limits and the peer's state stay.
         */
        void clearFaults() {
            recvChunk = Integer.MAX_VALUE;
            recvWouldBlockPct = 0;
            sendChunk = Integer.MAX_VALUE;
            sendWouldBlockPct = 0;
            isSendBlocked = false;
        }

        void closeFromPeer(int code) {
            isPeerClosed = true;
            recvCloseCode = code;
        }

        /**
         * @return true when a dispatcher would see a read event: bytes, an end of stream or an error
         */
        boolean isReadable() {
            return isPeerClosed || toServer.totalRead >= recvLimit || toServer.available() > 0;
        }

        int readableBytes() {
            return (int) Math.min(toServer.available(), recvLimit - toServer.totalRead);
        }
    }

    /**
     * A long random sequence of calls on one connection, checked against a model of two byte streams: what the
     * client engine took is what recv() must return, and what send() took is what the client must read.
     */
    private static final class OperationSequence {
        private final int bufferSize;
        private final byte[] down;
        private final Harness h;
        private final boolean isHandshakeFirst;
        // Only some sequences let the client ask for key updates. While the server owes an answer to one,
        // shutdown() and close() send that answer instead of close_notify, which
        // testCloseNotifyFollowsOwedKeyUpdateReply pins; such a sequence makes no claim about close_notify.
        private final boolean isKeyUpdating;
        private final int opCount;
        private final String protocol;
        private final int recvLenClass;
        private final Rnd rnd;
        private final int sendLenClass;
        // the operations that end a connection come up only from this step on, so most of a sequence is traffic
        private final int terminalFrom;
        private final byte[] up;
        private EngineClient client;
        private int clientWrapped;
        private boolean isClientCloseNotifySent;
        private boolean isClientGone;
        private boolean isCloseNotifyFlushed;
        // a call has failed: the caller of a real server disconnects here
        private boolean isDead;
        private boolean isReadDead;
        private boolean isWriteShutdown;
        private int keyUpdates;
        private Link link;
        private int op;
        // plaintext send() has taken
        private int serverAccepted;
        // plaintext send() was given, including that of a call that failed: some of it may have left before the failure
        private int serverOffered;
        private int serverReceived;
        private boolean wasHandshakeDone;

        OperationSequence(Harness h, Rnd rnd) {
            this.h = h;
            this.rnd = rnd;
            this.protocol = rnd.nextBoolean() ? TLS_13 : TLS_12;
            // Large buffers only: with a small one shutdown() can find no room for close_notify next to bytes
            // that are still queued, and the model below counts on close_notify being queued.
            this.bufferSize = LARGE_BUFFER_SIZE + rnd.nextInt(3);
            this.opCount = 200 + rnd.nextInt(1_801);
            this.terminalFrom = rnd.nextInt(opCount + opCount / 4);
            this.isHandshakeFirst = rnd.nextBoolean();
            this.isKeyUpdating = rnd.nextBoolean();
            this.recvLenClass = rnd.nextInt(IO_LEN_CLASSES);
            this.sendLenClass = rnd.nextInt(IO_LEN_CLASSES - 1);
            this.up = randomBytes(rnd, 1024 * 1024);
            this.down = randomBytes(rnd, 1024 * 1024);
        }

        @Override
        public String toString() {
            return "operation sequence protocol=" + protocol
                    + ", bufferSize=" + bufferSize
                    + ", ops=" + op + '/' + opCount
                    + ", terminalFrom=" + terminalFrom
                    + ", handshakeFirst=" + isHandshakeFirst
                    + ", keyUpdates=" + keyUpdates
                    + ", recvLenClass=" + recvLenClass
                    + ", sendLenClass=" + sendLenClass
                    + ", clientWrapped=" + clientWrapped
                    + ", serverReceived=" + serverReceived
                    + ", serverAccepted=" + serverAccepted
                    + ", clientReceived=" + (client != null ? client.received : 0)
                    + ", clientCloseNotifySent=" + isClientCloseNotifySent
                    + ", clientGone=" + isClientGone
                    + ", writeShutdown=" + isWriteShutdown
                    + ", dead=" + isDead
                    + (link != null ? ", " + link : "");
        }

        void run() throws Exception {
            link = h.open(new Rnd(rnd.nextLong(), rnd.nextLong()));
            link.sendErrorCode = rnd.nextBoolean() ? Net.EPEERDISCONNECT : Net.EOTHERDISCONNECT;
            randomizeChunks();
            client = new EngineClient(TlsFuzzSupport.newTrustAllContext(), link, new String[]{protocol}, null, 1);
            client.expectPlaintext(down);
            client.pump();
            client.assertHealthy();
            client.flush();
            h.startTls(bufferSize);
            if (isHandshakeFirst) {
                driveHandshake(h, client);
            }
            for (op = 0; op < opCount && !isDead; op++) {
                step();
                checkInvariants();
            }
            if (isReadDead) {
                // nothing follows the end of the stream: a caller that kept polling gets no bytes
                for (int i = 0; i < 3; i++) {
                    assertTrue("recv() returned data after it had reported the end of the stream", h.recv(64) <= 0);
                }
            }
            if (!isDead) {
                settle();
            }
            close();
        }

        private void checkInvariants() {
            final JavaTlsServerSocket socket = h.socket;
            assertFalse(socket.wantsTlsRead());
            assertTrue(socket.isTlsSessionStarted());
            assertFalse(socket.isClosed());
            assertEquals(h.fd, socket.getFd());
            if (wasHandshakeDone) {
                assertFalse("a completed handshake started over", socket.isHandshakeInProgress());
            }
            wasHandshakeDone = !socket.isHandshakeInProgress();
            assertTrue(serverReceived <= clientWrapped);
            assertTrue("the client received plaintext the server never sent", client.received <= serverOffered);
        }

        private void clientCloseNotify() throws SSLException {
            client.closeNotify();
            client.flush();
            isClientCloseNotifySent = true;
        }

        private void clientHangUp() {
            client.flush();
            link.closeFromPeer(rnd.nextBoolean() ? Net.EPEERDISCONNECT : Net.EOTHERDISCONNECT);
            link.isSendRejected = true;
            isClientGone = true;
        }

        private void clientKeyUpdate() throws SSLException {
            // An engine whose inbound side the server's close_notify has closed refuses to start a KeyUpdate:
            // beginHandshake() throws and breaks the client.
            if (!isKeyUpdating || isClientGone || isClientCloseNotifySent || client.isCloseNotifySeen
                    || keyUpdates >= 8 || !TLS_13.equals(protocol) || !client.isHandshakeDone()) {
                return;
            }
            client.startNewHandshake();
            client.flush();
            keyUpdates++;
        }

        private void clientPump() {
            if (isClientGone) {
                return;
            }
            client.pump();
            client.assertHealthy();
            client.flush();
            assertTrue("the client received plaintext the server never sent", client.received <= serverOffered);
        }

        private void clientWrap() throws SSLException {
            if (isClientGone || isClientCloseNotifySent || !client.isHandshakeDone() || clientWrapped == up.length) {
                return;
            }
            final int len = Math.min(up.length - clientWrapped, 1 + rnd.nextInt(rnd.nextInt(4) == 0 ? 16_384 : 512));
            clientWrapped += client.wrap(up, clientWrapped, len);
            client.flush();
        }

        /**
         * close() is the last operation of every sequence.
         */
        private void close() {
            final JavaTlsServerSocket socket = h.socket;
            final boolean wasHandshaking = socket.isHandshakeInProgress();
            final boolean isTransportClean = rnd.nextBoolean();
            if (isTransportClean) {
                link.clearFaults();
            }
            final boolean wasPending = socket.wantsTlsWrite();
            h.closeAndVerify();
            if (isClientGone) {
                return;
            }
            client.pump();
            if (!wasHandshaking && client.isHandshakeDone()) {
                client.assertHealthy();
            }
            assertTrue("the client received plaintext the server never sent", client.received <= serverOffered);
            if (isTransportClean && !wasPending && !wasHandshaking && !isDead && !isWriteShutdown && keyUpdates == 0 && client.isHandshakeDone()) {
                assertTrue("close() must deliver close_notify when the transport takes it", client.isCloseNotifySeen);
            }
        }

        private void randomizeChunks() {
            link.recvChunk = Scenario.chunk(rnd);
            link.sendChunk = Scenario.chunk(rnd);
            link.recvWouldBlockPct = Scenario.wouldBlockPct(rnd);
            link.sendWouldBlockPct = Scenario.wouldBlockPct(rnd);
        }

        private void serverQueries() {
            final JavaTlsServerSocket socket = h.socket;
            final long callsBefore = link.recvCalls + link.sendCalls;
            assertFalse(socket.wantsTlsRead());
            final boolean wantsWrite = socket.wantsTlsWrite();
            final boolean isMoreBuffered = socket.isMorePlaintextBuffered();
            assertTrue(socket.isTlsSessionStarted());
            assertTrue(socket.supportsTls());
            assertEquals("asking the socket about its state must not change it", wantsWrite, socket.wantsTlsWrite());
            assertEquals(isMoreBuffered, socket.isMorePlaintextBuffered());
            assertEquals("asking the socket about its state must not touch the transport", callsBefore, link.recvCalls + link.sendCalls);
        }

        private void serverRecv(boolean isLengthForced) {
            // recv() is in order at any time; what it may return depends on whether a dispatcher would have called it
            final boolean isDue = h.isRecvDue();
            final int len = !isLengthForced && rnd.nextInt(16) == 0 ? 0 : Scenario.ioLen(rnd, recvLenClass);
            final int n = h.recv(len);
            if (h.socket.isHandshakeInProgress()) {
                assertEquals("recv() must deliver nothing during the handshake", 0, n);
            }
            if (n > 0) {
                assertTrue(
                        "recv() returned plaintext that nothing had announced: the link had nothing to read and isMorePlaintextBuffered() was false",
                        isDue
                );
                assertTrue("recv() returned more plaintext than the client has sent", serverReceived + n <= clientWrapped);
                final int mismatch = Arrays.mismatch(h.tmp, 0, n, up, serverReceived, serverReceived + n);
                if (mismatch != -1) {
                    fail("recv() returned a wrong byte at stream offset " + (serverReceived + mismatch));
                }
                serverReceived += n;
            } else if (n < 0) {
                // A TLS 1.2 client answers the server's close_notify with its own, so a client that has seen the
                // server's shutdown counts as having closed too. A TLS 1.3 client keeps its write side open.
                final boolean isCloseNotifyDue = isClientCloseNotifySent || (client.isCloseNotifySeen && TLS_12.equals(protocol));
                assertTrue("recv() failed although the client is still there: " + n, isCloseNotifyDue || isClientGone);
                assertEquals("recv() reported the end of the stream before the plaintext in front of it", clientWrapped, serverReceived);
                if (!isClientGone) {
                    // the peer's close_notify
                    assertEquals(-1, n);
                } else if (!isCloseNotifyDue) {
                    // the end of the transport's stream passes through as the transport reports it
                    assertEquals(link.recvCloseCode, n);
                }
                isDead = true;
                isReadDead = true;
            }
        }

        // Everything send() reported has arrived. The client may hold up to one record more: plaintext that was
        // encrypted in a short write and went out with a later flush, before send() was called again to report it.
        private void assertReceivedMatchesAccepted(String message) {
            assertTrue(message + " [accepted=" + serverAccepted + ", received=" + client.received + ']',
                    client.received >= serverAccepted && client.received <= serverAccepted + MAX_RECORD_PLAINTEXT);
        }

        private void serverSend() {
            final JavaTlsServerSocket socket = h.socket;
            final boolean wasHandshaking = socket.isHandshakeInProgress();
            final int len = rnd.nextInt(16) == 0 ? 0 : Math.min(down.length - serverAccepted, Scenario.ioLen(rnd, sendLenClass));
            // only grows: a short write may have encrypted bytes that a later, shorter call does not present
            serverOffered = Math.max(serverOffered, serverAccepted + len);
            final int n = h.send(down, serverAccepted, len);
            if (socket.isHandshakeInProgress()) {
                assertEquals("send() must take nothing during the handshake", 0, n);
            }
            if (n < 0) {
                // the write side is shut, the peer is gone, or the peer's close_notify has closed the engine
                assertTrue("send() failed on a healthy connection: " + n, isWriteShutdown || isClientGone || isClientCloseNotifySent);
                isDead = true;
                return;
            }
            serverAccepted += n;
            serverOffered = Math.max(serverOffered, serverAccepted);
            if (len > 0 && !wasHandshaking) {
                assertFalse("send() took plaintext after shutdown(SHUT_WR)", isWriteShutdown);
                assertFalse("send() took plaintext although the peer is gone", isClientGone);
            }
        }

        private void serverShutdownRead() {
            final JavaTlsServerSocket socket = h.socket;
            final long written = link.toClient.totalWritten;
            final boolean wantsWrite = socket.wantsTlsWrite();
            final int calls = link.shutdownReadCalls;
            assertEquals(0, h.shutdown(0));
            assertEquals(calls + 1, link.shutdownReadCalls);
            assertEquals("shutting down the read side must leave the TLS session alone", written, link.toClient.totalWritten);
            assertEquals(wantsWrite, socket.wantsTlsWrite());
        }

        private void serverShutdownWrite() {
            assertEquals(0, h.shutdown(Net.SHUT_WR));
            assertEquals(1, link.shutdownWriteCalls);
            isWriteShutdown = true;
            // shutdown() tries once; what stayed behind, close_notify included, is lost
            isCloseNotifyFlushed = !h.socket.wantsTlsWrite();
        }

        private void serverTlsIO() {
            // every combination of the write flag (1) and the read flag (2)
            final int flags = rnd.nextInt(4);
            final int n = h.tlsIO(flags);
            if (n < 0) {
                assertTrue("tlsIO() failed without the write flag", (flags & Socket.WRITE_FLAG) != 0);
                assertTrue("tlsIO() failed on a healthy transport: " + n, isWriteShutdown || isClientGone);
                assertEquals("a transport error passes through tlsIO()", link.sendErrorCode, n);
                isDead = true;
            } else {
                assertEquals(0, n);
            }
        }

        /**
         * After the last operation both sides run until nothing moves, the way a dispatcher and a client would.
         */
        private void settle() {
            final JavaTlsServerSocket socket = h.socket;
            link.isSendBlocked = false;
            for (int steps = 0; ; steps++) {
                assertTrue("the connection does not settle", steps < 10_000_000);
                var isBusy = false;
                if (!isDead) {
                    if (h.isRecvDue()) {
                        serverRecv(true);
                        isBusy = true;
                    } else if (socket.wantsTlsWrite() && !isWriteShutdown) {
                        assertEquals(0, h.tlsIO(Socket.WRITE_FLAG));
                        isBusy = true;
                    }
                }
                if (!isClientGone && (link.toClient.available() > 0 || client.isWorkPending() || client.stagedSize() > 0)) {
                    clientPump();
                    isBusy = true;
                }
                if (!isBusy) {
                    break;
                }
            }
            if (isDead || isClientGone) {
                return;
            }
            // nothing moves and the transport is open: everything that was sent has arrived
            assertFalse("the handshake never completed", socket.isHandshakeInProgress());
            assertTrue(client.isHandshakeDone());
            assertEquals("the server must receive the whole client stream", clientWrapped, serverReceived);
            if (!isWriteShutdown) {
                assertFalse(socket.wantsTlsWrite());
                assertReceivedMatchesAccepted("the client must receive everything send() took");
            } else if (isCloseNotifyFlushed) {
                assertReceivedMatchesAccepted("the client must receive everything send() took before the shutdown");
                if (keyUpdates == 0) {
                    assertTrue("shutdown() left nothing behind, so the client must see close_notify", client.isCloseNotifySeen);
                }
            } else {
                assertTrue(client.received <= serverAccepted + MAX_RECORD_PLAINTEXT);
                assertFalse("the client saw a close_notify that shutdown() could not send", client.isCloseNotifySeen);
            }
        }

        private void step() throws Exception {
            final int dice = rnd.nextInt(100);
            // a connection ends only once both sides are through the handshake
            final boolean isTerminalAllowed = op >= terminalFrom && client.isHandshakeDone() && !h.socket.isHandshakeInProgress();
            if (dice < 26) {
                serverRecv(false);
            } else if (dice < 44) {
                serverSend();
            } else if (dice < 52) {
                serverTlsIO();
            } else if (dice < 57) {
                serverQueries();
            } else if (dice < 59) {
                serverShutdownRead();
            } else if (dice < 60) {
                if (isTerminalAllowed && !isWriteShutdown) {
                    serverShutdownWrite();
                } else {
                    serverRecv(false);
                }
            } else if (dice < 78) {
                clientWrap();
            } else if (dice < 92) {
                clientPump();
            } else if (dice < 94) {
                clientKeyUpdate();
            } else if (dice < 95) {
                if (isTerminalAllowed && !isClientCloseNotifySent && !isClientGone) {
                    clientCloseNotify();
                } else {
                    clientPump();
                }
            } else if (dice < 96) {
                if (isTerminalAllowed && !isClientGone) {
                    clientHangUp();
                } else {
                    clientPump();
                }
            } else if (dice < 98) {
                link.isSendBlocked = !link.isSendBlocked;
            } else {
                randomizeChunks();
            }
        }
    }

    /**
     * The parameters of one well-behaved connection.
     */
    private static final class Scenario {
        static final int DRIVER_MIXED = 2;
        static final int DRIVER_RECV = 0;
        static final int DRIVER_SEND = 1;
        static final int ENDING_CLOSE = 0;
        static final int ENDING_CLOSE_CLEAN = 1;
        static final int ENDING_SHUTDOWN = 2;
        static final int QUEUED_ALL = 2;
        static final int QUEUED_NONE = 0;
        static final int QUEUED_PART = 1;
        int bufferSize;
        // null stands for a fresh context, which makes the handshake a full one
        SSLContext clientContext;
        // how much plaintext the client puts into one record: 1 byte, up to 100 bytes, up to a full record
        int clientRecordClass;
        byte[] down;
        int ending;
        // which call moves the handshake: recv() as for a handler that reads first, send() as for one that
        // writes first, or both
        int handshakeDriver;
        // the server sends nothing of its own until the bytes of its handshake have been counted
        boolean isHandshakeMeasured;
        boolean isPgStyle;
        int peerPort = 1;
        // how much of the ClientHello is on the wire before startTlsSession()
        int preQueued;
        String[] protocols;
        int recvChunk;
        int recvLenClass;
        int recvWouldBlockPct;
        long scheduleSeed0;
        long scheduleSeed1;
        int sendChunk;
        int sendLenClass;
        int sendWouldBlockPct;
        String[] suites;
        long transportSeed0;
        long transportSeed1;
        byte[] up;

        /**
         * @return the most bytes one transport call moves: 1, a few, a few thousand, or all there is
         */
        static int chunk(Rnd rnd) {
            return switch (rnd.nextInt(4)) {
                case 0 -> 1;
                case 1 -> 1 + rnd.nextInt(64);
                case 2 -> 1 + rnd.nextInt(20_000);
                default -> Integer.MAX_VALUE;
            };
        }

        /**
         * Small payloads and every knob of the handshake.
         */
        static Scenario handshake(Rnd rnd, ClientConfig config) {
            final Scenario s = common(rnd, config);
            s.up = randomBytes(rnd, rnd.nextInt(3) == 0 ? 0 : 1 + rnd.nextInt(2_000));
            s.down = randomBytes(rnd, rnd.nextInt(3) == 0 ? 0 : 1 + rnd.nextInt(2_000));
            s.settleDriver();
            return s;
        }

        /**
         * @return the length of one recv() or send(): around the record size, around the staging buffer's
         * capacity, tiny, or as large as the caller's buffer gets
         */
        static int ioLen(Rnd rnd, int lenClass) {
            return switch (lenClass) {
                case 0 -> 1;
                case 1 -> 1 + rnd.nextInt(64);
                case 2 -> 16_383 + rnd.nextInt(3);
                case 3 -> 16_600 + rnd.nextInt(121);
                case 4 -> 1 + rnd.nextInt(65_536);
                default -> 1 << 20;
            };
        }

        /**
         * A handshake whose server bytes get counted, from a client context that remembers the session.
         */
        static Scenario resumable(Rnd rnd, ClientConfig config, SSLContext clientContext, int peerPort) {
            final Scenario s = handshake(rnd, config);
            s.clientContext = clientContext;
            s.peerPort = peerPort;
            s.isHandshakeMeasured = true;
            s.handshakeDriver = DRIVER_RECV;
            return s;
        }

        /**
         * Payloads around every record and buffer boundary.
         */
        static Scenario roundTrip(Rnd rnd, ClientConfig config, int maxPayload) {
            final Scenario s = common(rnd, config);
            s.up = randomBytes(rnd, payloadSize(rnd, s.bufferSize, s.upCap(maxPayload)));
            s.down = randomBytes(rnd, payloadSize(rnd, s.bufferSize, s.downCap(maxPayload)));
            s.settleDriver();
            return s;
        }

        static int wouldBlockPct(Rnd rnd) {
            return rnd.nextInt(3) == 0 ? 1 + rnd.nextInt(60) : 0;
        }

        @Override
        public String toString() {
            return "protocols=" + Arrays.toString(protocols)
                    + ", suites=" + (suites == null ? "default" : Arrays.toString(suites))
                    + ", bufferSize=" + bufferSize
                    + ", up=" + up.length
                    + ", down=" + down.length
                    + ", clientRecordClass=" + clientRecordClass
                    + ", recvLenClass=" + recvLenClass
                    + ", sendLenClass=" + sendLenClass
                    + ", recvChunk=" + recvChunk
                    + ", recvWouldBlockPct=" + recvWouldBlockPct
                    + ", sendChunk=" + sendChunk
                    + ", sendWouldBlockPct=" + sendWouldBlockPct
                    + ", pgStyle=" + isPgStyle
                    + ", preQueued=" + preQueued
                    + ", handshakeDriver=" + handshakeDriver
                    + ", handshakeMeasured=" + isHandshakeMeasured
                    + ", resumable=" + (clientContext != null)
                    + ", ending=" + ending;
        }

        int clientRecordLen(Rnd rnd) {
            return switch (clientRecordClass) {
                case 0 -> 1;
                case 1 -> 2 + rnd.nextInt(99);
                default -> 1 + rnd.nextInt(16_384);
            };
        }

        /**
         * @return an upper bound of the steps an honest run takes; idle detection catches a hang long before it
         */
        long stepBudget() {
            final long upWire = (long) up.length * (clientRecordClass == 0 ? 64 : clientRecordClass == 1 ? 8 : 2);
            final long downWire = (long) down.length * (sendLenClass == 0 ? 64 : sendLenClass == 1 ? 8 : 2);
            return 2_000_000L + 64L * (upWire + downWire);
        }

        private static Scenario common(Rnd rnd, ClientConfig config) {
            final Scenario s = new Scenario();
            s.protocols = config.protocols;
            s.suites = config.suites;
            s.bufferSize = BUFFER_SIZES[rnd.nextInt(BUFFER_SIZES.length)];
            s.clientRecordClass = rnd.nextInt(3);
            s.recvLenClass = rnd.nextInt(IO_LEN_CLASSES);
            s.sendLenClass = rnd.nextInt(IO_LEN_CLASSES);
            s.recvChunk = chunk(rnd);
            s.sendChunk = chunk(rnd);
            s.recvWouldBlockPct = wouldBlockPct(rnd);
            s.sendWouldBlockPct = wouldBlockPct(rnd);
            s.isPgStyle = rnd.nextInt(4) == 0;
            s.preQueued = rnd.nextInt(3);
            s.handshakeDriver = rnd.nextInt(3);
            s.ending = rnd.nextInt(3);
            s.scheduleSeed0 = rnd.nextLong();
            s.scheduleSeed1 = rnd.nextLong();
            s.transportSeed0 = rnd.nextLong();
            s.transportSeed1 = rnd.nextLong();
            return s;
        }

        private static int payloadSize(Rnd rnd, int bufferSize, int cap) {
            final int size = switch (rnd.nextInt(12)) {
                case 0 -> 0;
                case 1 -> 1;
                case 2 -> 2;
                case 3 -> 16_383 + rnd.nextInt(3);
                // around the largest record and the staging buffer's capacity
                case 4 -> 16_600 + rnd.nextInt(121);
                case 5 -> 32_768;
                case 6 -> 65_535 + rnd.nextInt(3);
                case 7 -> 262_143 + rnd.nextInt(3);
                case 8 -> Math.max(0, bufferSize - 1 + rnd.nextInt(3));
                case 9 -> 300 * 1024 + rnd.nextInt(Math.max(1, cap - 300 * 1024 + 1));
                default -> 1 + rnd.nextInt(4_096);
            };
            return Math.min(size, cap);
        }

        /**
         * A transport that moves one byte per call, or a sender that makes one record per byte, turns a megabyte
         * into millions of steps: such combinations get a smaller payload.
         */
        private int downCap(int maxPayload) {
            int cap = maxPayload;
            if (sendChunk == 1) {
                cap /= 16;
            } else if (sendChunk <= 64) {
                cap /= 4;
            }
            if (sendLenClass == 0) {
                cap /= 64;
            } else if (sendLenClass == 1) {
                cap /= 8;
            }
            return Math.max(cap, 256);
        }

        private void settleDriver() {
            // send() can move the handshake only when the server has something to send
            if (handshakeDriver == DRIVER_SEND && down.length == 0) {
                handshakeDriver = DRIVER_RECV;
            }
        }

        private int upCap(int maxPayload) {
            int cap = maxPayload;
            if (recvChunk == 1) {
                cap /= 16;
            } else if (recvChunk <= 64) {
                cap /= 4;
            }
            if (clientRecordClass == 0) {
                cap /= 64;
            } else if (clientRecordClass == 1) {
                cap /= 8;
            }
            if (recvLenClass == 0) {
                cap /= 4;
            }
            return Math.max(cap, 256);
        }
    }

    /**
     * What the fake transport throws when one socket call keeps coming back to it. An Error, so that the socket's
     * handlers for SSLException and RuntimeException let it through to the test.
     */
    private static final class TransportSpinError extends Error {
        TransportSpinError(String message) {
            super(message);
        }
    }
}
