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
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.Socket;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.JavaTlsServerSocket;
import io.questdb.tls.JavaTlsServerSocketFactory;
import io.questdb.tls.PemReader;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static org.junit.Assert.*;

public class JavaTlsServerSocketTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    // the authentication tag of every TLS 1.3 cipher suite
    private static final int AEAD_TAG_SIZE = 16;
    private static final String BUFFER_SIZE_PROPERTY = "questdb.experimental.tls.buffersize";
    // The answers to this many TLS 1.3 key updates, 27 bytes each, overflow the default 256 KiB output buffer.
    private static final int KEY_UPDATE_FLOOD = 10_000;
    // The socket ends a connection whose peer sends more than 32 records without data in a row.
    private static final int KEY_UPDATES_PER_DATA_RECORD = 16;
    private static final Log LOG = LogFactory.getLog(JavaTlsServerSocketTest.class);
    private static final int MEMORY_BUF_SIZE = 64 * 1024;
    // A made-up fd: only a MemoryTransport ever sees it.
    private static final long MEMORY_FD = 1_000_000;
    // A close_notify alert record before any cipher change: warning level, description 0 (RFC 5246, 7.2).
    private static final byte[] PLAINTEXT_CLOSE_NOTIFY = {0x15, 0x03, 0x03, 0x00, 0x02, 0x01, 0x00};
    private static final long TIMEOUT_NANOS = 20_000_000_000L;
    private static final String TLS_12 = "TLSv1.2";
    private static final String TLS_13 = "TLSv1.3";
    private static int boundPort;
    private static String certPath;
    private static String keyPath;
    private static boolean opensslAvailable;
    // A spinning socket would otherwise hang the build instead of failing it.
    @Rule
    public final Timeout timeout = Timeout.seconds(120);

    @BeforeClass
    public static void setUp() throws Exception {
        opensslAvailable = isOpensslAvailable();
        if (opensslAvailable) {
            final var certFile = new File(temp.getRoot(), "server.crt");
            final var keyFile = new File(temp.getRoot(), "server.key");
            final var proc = new ProcessBuilder(
                    "openssl", "req", "-x509", "-newkey", "rsa:2048",
                    "-keyout", keyFile.getAbsolutePath(),
                    "-out", certFile.getAbsolutePath(),
                    "-days", "1", "-nodes",
                    "-subj", "/CN=localhost"
            ).redirectErrorStream(true).start();
            if (proc.waitFor() == 0) {
                certPath = certFile.getAbsolutePath();
                keyPath = keyFile.getAbsolutePath();
            } else {
                opensslAvailable = false;
            }
        }
    }

    @Test
    public void testAlertInsteadOfClientHelloFailsHandshake() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientFd = connectRaw();
                try {
                    final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                    tlsSocket.startTlsSession(null);
                    // a plaintext close_notify alert record where the ClientHello belongs
                    sendRaw(clientFd, new byte[]{0x15, 0x03, 0x03, 0x00, 0x02, 0x01, 0x00});
                    final var buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        assertEquals(-1, recvUntilNonZero(tlsSocket, buf, 64));
                    } finally {
                        Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                    }
                    tlsSocket.close();
                } finally {
                    Net.close(clientFd);
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testAppendSanitizedEscapesEverythingButPrintableAscii() {
        final var sink = new StringSink();
        JavaTlsServerSocket.appendSanitized(sink, "ok name=a\r\nFAKE é\\x");
        TestUtils.assertEquals("ok name=a\\u000d\\u000aFAKE \\u00e9\\u005cx", sink);
        sink.clear();
        JavaTlsServerSocket.appendSanitized(sink, null);
        TestUtils.assertEquals("null", sink);
        sink.clear();
        JavaTlsServerSocket.appendSanitized(sink, "y".repeat(300));
        TestUtils.assertEquals("y".repeat(256) + "...", sink);
    }

    @Test
    public void testBufferedPlaintextIsSignalledUntilDrained() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, null, (client, server, buf) -> {
            final var plain = new byte[40_000];
            new SecureRandom().nextBytes(plain);
            client.writeRaw(client.encrypt(plain));
            // let every byte reach the server's kernel buffer, so one recv() pulls all of it into the socket
            Os.sleep(500);
            final var received = new ByteArrayOutputStream();
            var n = server.recv(buf, 1000);
            assertEquals(1000, n);
            appendBytes(received, buf, n);
            // A dispatcher calls recv() again without waiting for the socket only while this flag is set: if it
            // were cleared while data still sits in the socket's buffers, that data would be stranded.
            while (server.isMorePlaintextBuffered()) {
                n = server.recv(buf, 1000);
                assertTrue(n > 0);
                appendBytes(received, buf, n);
            }
            assertArrayEquals(plain, received.toByteArray());
            assertEquals(0, server.recv(buf, 1000));
        });
    }

    @Test
    public void testClientCancelDuringTls12HandshakeFailsHandshake() throws Exception {
        // A TLS 1.2 client that gives up mid-handshake sends user_canceled and close_notify. The server must fail
        // the handshake at once rather than wait for a Finished that never comes, and close() answers with a
        // close_notify of its own, in plaintext, as the handshake never got as far as changing ciphers.
        TestUtils.assertMemoryLeak(() -> {
            final var transport = new MemoryTransport();
            final var server = new JavaTlsServerSocket(transport, LOG, TlsFuzzSupport.newServerContext());
            final var client = new MemoryClient(TlsFuzzSupport.newTrustAllContext(), transport, TLS_12);
            final var tlsMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            final var buf = Unsafe.malloc(MEMORY_BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            final long sentBeforeClose;
            try {
                server.of(MEMORY_FD);
                server.startTlsSession(null);
                client.pump(); // the ClientHello
                assertEquals(0, server.recv(buf, MEMORY_BUF_SIZE));
                assertTrue("the server answers the ClientHello", transport.toClient.available() > 0);
                // the client cancels before it reads the server's flight
                client.cancel();
                sentBeforeClose = transport.toClient.totalWritten;
                assertEquals(-1, server.recv(buf, MEMORY_BUF_SIZE));
                // a failed handshake stays failed
                assertEquals(-1, server.recv(buf, MEMORY_BUF_SIZE));
                assertEquals(-1, server.send(buf, 1));
                assertEquals("the failed handshake sends nothing before close()", sentBeforeClose, transport.toClient.totalWritten);
            } finally {
                Unsafe.free(buf, MEMORY_BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
                server.close();
            }
            final var wire = transport.toClient.takeAll();
            assertArrayEquals(PLAINTEXT_CLOSE_NOTIFY, Arrays.copyOfRange(wire, (int) sentBeforeClose, wire.length));
            assertEquals("close() must close the fd exactly once", 1, transport.closeCalls);
            assertEquals("close() must free every TLS buffer", tlsMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS));
        });
    }

    @Test
    public void testCloseDoesNotSpinOnEngineThatWrapsNothingToCloseWith() throws Exception {
        // close() wraps until the engine's outbound side is done. An engine that is never done, and wraps nothing,
        // would keep close() spinning: close() stops, and still closes the fd and frees every buffer.
        final var engine = new ScriptedEngine();
        assertWithMemoryHandshake(engine.context(), -1, TLS_13, (client, server, transport, buf) -> {
            engine.isOutboundNeverDone = true;
            engine.wrapStep = (delegate, src, dst) -> new SSLEngineResult(
                    SSLEngineResult.Status.CLOSED,
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    0,
                    0
            );
            final var sentBefore = transport.toClient.totalWritten;
            server.close();
            assertTrue(server.isClosed());
            assertFalse(server.isTlsSessionStarted());
            assertEquals("nothing was wrapped, so nothing goes out", sentBefore, transport.toClient.totalWritten);
        });
    }

    @Test
    public void testCloseDuringHandshakeReleasesResources() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientFd = connectRaw();
                try {
                    final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                    tlsSocket.startTlsSession(null);
                    assertTrue(tlsSocket.isHandshakeInProgress());
                    final var start = System.nanoTime();
                    tlsSocket.close();
                    assertTrue("close() must not wait for the client", System.nanoTime() - start < TIMEOUT_NANOS);
                    assertFalse(tlsSocket.isTlsSessionStarted());
                    assertFalse(tlsSocket.isHandshakeInProgress());
                    // idempotent
                    tlsSocket.close();
                } finally {
                    Net.close(clientFd);
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testCloseNotifyBehindOwedAnswerGrowsBufferConfiguredSmall() throws Exception {
        // The output buffer holds the answer to a key update, and close_notify does not fit behind it in a buffer
        // configured to one packet. When the socket takes nothing at that moment, the buffer grows: close_notify
        // goes out behind the answer instead of being dropped.
        final var packetSize = packetBufferSize();
        assertWithMemoryHandshake(packetSize, TLS_13, (client, server, transport, buf) -> {
            final var wireBefore = client.wireSize();
            client.keyUpdate();
            assertEquals("a key update carries no data", 0, server.recv(buf, MEMORY_BUF_SIZE));
            assertTrue("the answer waits in the output buffer", server.wantsTlsWrite());
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            transport.blockedSends = 1;
            assertEquals(0, server.shutdown(Net.SHUT_WR));
            assertEquals("the output buffer doubles to make room for close_notify", packetSize, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMem);
            assertFalse("shutdown() sends the answer and close_notify once the socket takes them", server.wantsTlsWrite());
            assertEquals(1, transport.shutdownWriteCalls);
            client.pump();
            client.assertHealthy();
            assertEquals("the answer, then close_notify", 2, countRecords(client.wire(), wireBefore));
            assertTrue(client.isCloseNotifySeen);
        });
    }

    @Test
    public void testCloseNotifyDoesNotSpinOnEagain() throws Exception {
        final var isSocketFull = new AtomicBoolean();
        final var facade = new NetworkFacadeImpl() {
            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                return isSocketFull.get() ? 0 : super.sendRaw(fd, buffer, bufferLen);
            }
        };
        assertWithHandshake(facade, null, (client, server, buf) -> {
            // the socket takes nothing more: close() queues close_notify, tries once and gives up
            isSocketFull.set(true);
            final var start = System.nanoTime();
            server.close();
            assertTrue("close() must not wait for the socket", System.nanoTime() - start < 5_000_000_000L);
            assertFalse(server.isTlsSessionStarted());
        });
    }

    @Test
    public void testCloseUnderMemoryLimitStillFreesEverything() throws Exception {
        // A record the socket did not take leaves no room for close_notify in a buffer configured to one packet,
        // and the RSS limit forbids growing it: close() must still close the fd and free every buffer.
        assertWithMemoryHandshake(packetBufferSize(), TLS_13, (client, server, transport, buf) -> {
            transport.sendBudget = 0;
            putBytes(buf, pattern(100, 0));
            assertEquals(0, server.send(buf, 100));
            assertTrue(server.wantsTlsWrite());
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
            try {
                server.close();
            } finally {
                Unsafe.setRssMemLimit(0);
            }
            assertTrue(server.isClosed());
            assertFalse(server.isTlsSessionStarted());
            assertFalse(server.wantsTlsWrite());
            assertEquals(1, transport.closeCalls);
        });
    }

    @Test
    public void testCorruptRecordFailsRecv() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, null, (client, server, buf) -> {
            final var record = client.encrypt("tampered".getBytes(StandardCharsets.UTF_8));
            record[record.length - 1] ^= 1; // breaks the record's authentication tag
            client.writeRaw(record);
            assertEquals(-1, recvUntilNonZero(server, buf, 4096));
        });
    }

    @Test
    public void testEngineClosedOnHandshakeWrapSendsItsAlertAndFailsStart() throws Exception {
        // An engine that closes on a handshake wrap has wrapped a fatal alert: the socket sends the alert on a
        // best-effort basis and fails the handshake.
        final byte[] alert = {0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28}; // fatal handshake_failure
        final var engine = new ScriptedEngine();
        engine.wrapStep = (delegate, src, dst) -> {
            dst.put(alert);
            return new SSLEngineResult(SSLEngineResult.Status.CLOSED, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, alert.length);
        };
        assertWithMemoryConnection(engine.context(), TLS_13, (client, server, transport, buf) -> {
            // the ClientHello
            client.pump();
            try {
                server.startTlsSession(null);
                fail("the handshake must fail");
            } catch (TlsSessionInitFailedException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "TLS handshake failed: engine closed");
            }
            assertFalse(server.isTlsSessionStarted());
            client.pump();
            assertArrayEquals("the alert goes out", alert, client.wire());
            assertNotNull("the client reads the alert", client.failure);
            TestUtils.assertContains(client.failure.getMessage(), "handshake_failure");
        });
    }

    @Test
    public void testEngineResultsWithoutProgressFailRecv() throws Exception {
        // An engine that waits for handshake input after the handshake, or that neither consumes nor produces a
        // byte, would keep recv() spinning: recv() gives the connection up instead, as on a renegotiation attempt.
        final SSLEngineResult[] results = {
                new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_UNWRAP, 0, 0),
                new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN, 0, 0),
                new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, 0),
        };
        for (var result : results) {
            final var engine = new ScriptedEngine();
            assertWithMemoryHandshake(engine.context(), -1, TLS_13, (client, server, transport, buf) -> {
                engine.unwrapStep = (delegate, src, dst) -> result;
                client.write("data".getBytes(StandardCharsets.UTF_8));
                assertEquals(result.getHandshakeStatus().name(), -1, server.recv(buf, MEMORY_BUF_SIZE));
            });
        }
    }

    @Test
    public void testEngineThatNeverCompletesRecordStopsGrowingInputAtCap() throws Exception {
        // An engine that keeps asking for the rest of a record makes recv() grow the input buffer, which a buffer
        // full of one incomplete record needs. The growth stops at 1 MiB, far above any TLS record: there recv()
        // gives the connection up instead of buffering whatever the peer sends.
        final var initialSize = 64 * 1024;
        final var cap = 1024 * 1024;
        final var engine = new ScriptedEngine();
        assertWithMemoryHandshake(engine.context(), initialSize, TLS_13, (client, server, transport, buf) -> {
            engine.unwrapStep = (delegate, src, dst) -> new SSLEngineResult(
                    SSLEngineResult.Status.BUFFER_UNDERFLOW,
                    SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
                    0,
                    0
            );
            final var bytes = new byte[2 * cap];
            transport.toServer.append(bytes, 0, bytes.length);
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            assertEquals(-1, server.recv(buf, MEMORY_BUF_SIZE));
            assertEquals("the input buffer grows to the cap", cap - initialSize, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMem);
            assertEquals("recv() reads no more than the cap holds", bytes.length - cap, transport.toServer.available());
        });
    }

    @Test
    public void testEofInsideRecordAfterDataDeliversDataFirst() throws Exception {
        // One recv() reads a whole record, half of the next and the end of the stream: it returns the data of the
        // whole record, and the next call reports the disconnect.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            final var whole = "delivered".getBytes(StandardCharsets.UTF_8);
            client.write(whole);
            final var cut = client.encrypt("cut short".getBytes(StandardCharsets.UTF_8));
            transport.toServer.append(cut, 0, cut.length / 2);
            transport.isPeerClosed = true;
            assertEquals(whole.length, server.recv(buf, MEMORY_BUF_SIZE));
            assertNativeBytes(whole, buf);
            // the transport's own disconnect code passes through
            assertEquals(Net.EOTHERDISCONNECT, server.recv(buf, MEMORY_BUF_SIZE));
        });
    }

    @Test
    public void testEofInsideRecordFailsRecv() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, null, (client, server, buf) -> {
            final var record = client.encrypt("cut short".getBytes(StandardCharsets.UTF_8));
            client.writeRaw(Arrays.copyOf(record, record.length / 2));
            // half a record is never enough, however much of it has arrived
            assertEquals(0, server.recv(buf, 4096));
            client.closeChannel();
            // the socket's own disconnect code passes through, as for a plain socket
            assertTrue(recvUntilNonZero(server, buf, 4096) < 0);
        });
    }

    @Test
    public void testFactoryCreatesTlsSocket() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final Socket socket = new JavaTlsServerSocketFactory(sslContext).newInstance(NetworkFacadeImpl.INSTANCE, LOG);
            assertTrue(socket instanceof JavaTlsServerSocket);
            assertTrue(socket.supportsTls());
            assertFalse(socket.isTlsSessionStarted());
            socket.close();
        });
    }

    @Test
    public void testGarbageAfterStartFailsHandshakeOnRecv() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientFd = connectRaw();
                try {
                    final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                    tlsSocket.startTlsSession(null);
                    assertTrue(tlsSocket.isHandshakeInProgress());
                    sendRaw(clientFd, "GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                    final var buf = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
                    try {
                        assertEquals(-1, recvUntilNonZero(tlsSocket, buf, 256));
                        // a failed handshake stays failed
                        assertEquals(-1, tlsSocket.recv(buf, 256));
                        assertEquals(-1, tlsSocket.send(buf, 16));
                        // the failed engine must not be advanced by the dispatcher either
                        assertEquals(0, tlsSocket.tlsIO(Socket.WRITE_FLAG | Socket.READ_FLAG));
                    } finally {
                        Unsafe.free(buf, 256, MemoryTag.NATIVE_DEFAULT);
                    }
                    tlsSocket.close();
                } finally {
                    Net.close(clientFd);
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testGarbageBeforeStartFailsStartTlsSession() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                // the bytes must be on the server side before startTlsSession(); retry with a fresh connection
                // until they were, rather than trusting a fixed sleep
                var isFailureSeen = false;
                for (var attempt = 0; attempt < 20 && !isFailureSeen; attempt++) {
                    final var clientFd = connectRaw();
                    try {
                        final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                        try {
                            sendRaw(clientFd, "GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                            Os.sleep(10L * (attempt + 1));
                            try {
                                tlsSocket.startTlsSession(null);
                                continue;
                            } catch (TlsSessionInitFailedException e) {
                                isFailureSeen = true;
                                TestUtils.assertContains(e.getFlyweightMessage(), "TLS session creation failed [error=");
                            }
                            assertFalse(tlsSocket.isTlsSessionStarted());
                            assertFalse(tlsSocket.isHandshakeInProgress());
                            // A second attempt on the same socket reuses the buffers of the failed one; the client
                            // sends nothing more, so it waits for input.
                            tlsSocket.startTlsSession(null);
                            assertTrue(tlsSocket.isTlsSessionStarted());
                            assertTrue(tlsSocket.isHandshakeInProgress());
                        } finally {
                            tlsSocket.close();
                        }
                    } finally {
                        Net.close(clientFd);
                    }
                }
                assertTrue("the garbage never arrived before startTlsSession()", isFailureSeen);
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testHandshakeAndEcho() throws Exception {
        assertEcho(NetworkFacadeImpl.INSTANCE, "hello over TLS".getBytes(StandardCharsets.UTF_8), 4096, false);
    }

    @Test
    public void testHandshakeAndEchoLargePayloadThroughSmallRecvBuffer() throws Exception {
        final var payload = new byte[1024 * 1024 + 17];
        new SecureRandom().nextBytes(payload);
        // a 1 KiB destination is smaller than one TLS record, so recv() leaves plaintext buffered
        assertEcho(NetworkFacadeImpl.INSTANCE, payload, 1024, true);
    }

    @Test
    public void testHandshakeBytesPartiallySentAreFlushedByTlsIO() throws Exception {
        final var wasThrottled = new AtomicBoolean();
        final var throttlingFacade = new NetworkFacadeImpl() {
            private int calls;

            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                // every other call accepts nothing, the rest accept at most 64 bytes
                if ((calls++ & 1) == 0) {
                    wasThrottled.set(true);
                    return 0;
                }
                return super.sendRaw(fd, buffer, Math.min(bufferLen, 64));
            }
        };
        assertEcho(throttlingFacade, "throttled".getBytes(StandardCharsets.UTF_8), 4096, false);
        assertTrue(wasThrottled.get());
    }

    @Test
    public void testHandshakeCompletesInsideSend() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try (var client = new EngineClient(boundPort, null)) {
                final var server = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                final var buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    server.startTlsSession(null);
                    final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                    // drive the handshake through recv() until the client has written its Finished, and stop
                    // before recv() could read that Finished
                    for (; ; ) {
                        assertTrue("handshake timed out", System.nanoTime() < deadline);
                        client.stepHandshake();
                        if (client.isHandshakeDone()) {
                            break;
                        }
                        assertEquals(0, server.recv(buf, 64));
                        // the read flag alone never makes the dispatcher hook do anything
                        assertEquals(0, server.tlsIO(Socket.READ_FLAG));
                    }
                    assertTrue("the client's Finished is still unread", server.isHandshakeInProgress());
                    final var plain = "sent first".getBytes(StandardCharsets.UTF_8);
                    for (var i = 0; i < plain.length; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, plain[i]);
                    }
                    // send() reads the client's Finished, completes the handshake and sends the data; until the
                    // Finished arrives it returns 0
                    var n = 0;
                    while (n == 0) {
                        assertTrue("send() never completed the handshake", System.nanoTime() < deadline);
                        n = server.send(buf, plain.length);
                    }
                    assertEquals(plain.length, n);
                    assertFalse(server.isHandshakeInProgress());
                    assertArrayEquals(plain, client.readApplicationData(plain.length));
                } finally {
                    Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                    server.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testHandshakeDoesNotWaitForSilentClient() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientFd = connectRaw();
                final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                try {
                    final var start = System.nanoTime();
                    tlsSocket.startTlsSession(null);
                    assertTrue(
                            "startTlsSession() must return without client bytes",
                            System.nanoTime() - start < 1_000_000_000L
                    );
                    assertTrue(tlsSocket.isTlsSessionStarted());
                    assertTrue(tlsSocket.isHandshakeInProgress());
                    assertFalse(tlsSocket.wantsTlsRead());
                    assertFalse(tlsSocket.wantsTlsWrite());
                    assertFalse(tlsSocket.isMorePlaintextBuffered());
                    final var buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        assertEquals(0, tlsSocket.recv(buf, 64));
                        assertEquals(0, tlsSocket.send(buf, 64));
                        assertEquals(0, tlsSocket.tlsIO(Socket.WRITE_FLAG));
                        assertTrue(tlsSocket.isHandshakeInProgress());
                        Net.close(clientFd);
                        assertEquals(-1, recvUntilNonZero(tlsSocket, buf, 64));
                    } finally {
                        Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    tlsSocket.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testHandshakeEndSeenOnlyAtApplicationDataStillHandsOverData() throws Exception {
        // An engine that goes on asking for handshake input after the handshake has ended learns otherwise from the
        // next record: application data, which it cannot unwrap without room for plaintext. The socket takes that
        // for the end of the handshake, and the same recv() unwraps the record into the caller's buffer.
        final var engine = new ScriptedEngine();
        final var isEndHidden = new AtomicBoolean(true);
        engine.handshakeStatusMapper = status -> isEndHidden.get()
                && (status == SSLEngineResult.HandshakeStatus.FINISHED || status == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)
                ? SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                : status;
        engine.unwrapStep = (delegate, src, dst) -> {
            final var result = delegate.unwrap(src, dst);
            if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                isEndHidden.set(false);
            }
            return result;
        };
        assertWithMemoryConnection(engine.context(), TLS_13, (client, server, transport, buf) -> {
            server.startTlsSession(null);
            stepUntilClientFinished(client, server, buf);
            assertEquals(0, server.recv(buf, MEMORY_BUF_SIZE));
            assertTrue("the engine still asks for handshake input", server.isHandshakeInProgress());
            client.pump();
            client.assertHealthy();
            final var data = "first data".getBytes(StandardCharsets.UTF_8);
            client.write(data);
            assertEquals(data.length, server.recv(buf, MEMORY_BUF_SIZE));
            assertNativeBytes(data, buf);
            assertFalse(server.isHandshakeInProgress());
            assertFalse("the engine no longer hides the end of the handshake", isEndHidden.get());
        });
    }

    @Test
    public void testHandshakeEndSeenOnlyInEngineStateStillHandsOverBufferedData() throws Exception {
        // As in testTicketPartlySentStillHandsOverBufferedData, but no wrap result reports FINISHED: that the engine
        // no longer handshakes after it wrapped its last handshake output must complete the handshake just the same,
        // with that output partly unsent, so that recv() returns the data behind the client's Finished.
        final var engine = new ScriptedEngine();
        engine.handshakeStatusMapper = status -> status == SSLEngineResult.HandshakeStatus.FINISHED
                ? SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                : status;
        assertWithMemoryConnection(engine.context(), TLS_13, (client, server, transport, buf) -> {
            server.startTlsSession(null);
            stepUntilClientFinished(client, server, buf);
            final var data = "behind the Finished".getBytes(StandardCharsets.UTF_8);
            client.write(data);
            transport.sendBudget = 7;
            assertEquals(data.length, server.recv(buf, MEMORY_BUF_SIZE));
            assertNativeBytes(data, buf);
            assertFalse(server.isHandshakeInProgress());
            assertTrue("the rest of the last handshake output waits in the output buffer", server.wantsTlsWrite());
            transport.sendBudget = Long.MAX_VALUE;
            assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
            assertFalse(server.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
        });
    }

    @Test
    public void testHandshakeFailureClearsSslEngine() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                var isFailureSeen = false;
                for (var attempt = 0; attempt < 20 && !isFailureSeen; attempt++) {
                    final var clientFd = connectRaw();
                    // Close the client before the handshake starts, so the first read sees the disconnect
                    Net.close(clientFd);
                    final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                    try {
                        assertFalse("should not be started before handshake", tlsSocket.isTlsSessionStarted());
                        Os.sleep(10L * (attempt + 1));
                        try {
                            tlsSocket.startTlsSession(null);
                            continue; // the disconnect had not arrived yet
                        } catch (TlsSessionInitFailedException e) {
                            isFailureSeen = true;
                            TestUtils.assertContains(e.getFlyweightMessage(), "socket read error");
                        }
                        assertFalse(
                                "sslEngine must be null after failed handshake to prevent poisoned socket reuse",
                                tlsSocket.isTlsSessionStarted()
                        );
                    } finally {
                        tlsSocket.close();
                    }
                }
                assertTrue("the disconnect never arrived before startTlsSession()", isFailureSeen);
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testHandshakeWriteFailureInRecvFailsSocket() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var facade = new NetworkFacadeImpl() {
                @Override
                public int sendRaw(long fd, long buffer, int bufferLen) {
                    return -1;
                }
            };
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try (var client = new EngineClient(boundPort, null)) {
                final var tlsSocket = acceptTls(serverFd, facade, sslContext);
                final var buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    tlsSocket.startTlsSession(null);
                    client.stepHandshake(); // ClientHello
                    // the server's first handshake record cannot be sent
                    assertEquals(-1, recvUntilNonZero(tlsSocket, buf, 64));
                } finally {
                    Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                    tlsSocket.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testHandshakeWriteFailureInTlsIOFailsSocket() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sendCalls = new AtomicInteger();
            final var facade = new NetworkFacadeImpl() {
                @Override
                public int sendRaw(long fd, long buffer, int bufferLen) {
                    return switch (sendCalls.incrementAndGet()) {
                        case 1 -> super.sendRaw(fd, buffer, Math.min(bufferLen, 10)); // leaves bytes pending
                        case 2 -> super.sendRaw(fd, buffer, bufferLen); // flushes them
                        default -> -1; // the next handshake record hits a socket error
                    };
                }
            };
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try (var client = new EngineClient(boundPort, null)) {
                final var tlsSocket = acceptTls(serverFd, facade, sslContext);
                final var buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    tlsSocket.startTlsSession(null);
                    client.stepHandshake(); // ClientHello
                    // recv() processes the ClientHello once it arrives and leaves the first record partly unsent
                    final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                    while (!tlsSocket.wantsTlsWrite()) {
                        assertTrue("the server never produced its first handshake record", System.nanoTime() < deadline);
                        assertEquals(0, tlsSocket.recv(buf, 64));
                    }
                    assertEquals(-1, tlsSocket.tlsIO(Socket.WRITE_FLAG));
                    // a failure in tlsIO() fails the socket for good
                    assertEquals(-1, tlsSocket.recv(buf, 64));
                } finally {
                    Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                    tlsSocket.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testIllegalServerNameIsEscapedInError() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                // a ClientHello whose server name carries CR/LF: written raw, the error would forge a log line
                final var clientHello = clientHelloWithServerName("a\r\nFAKE.".getBytes(StandardCharsets.US_ASCII));
                var isFailureSeen = false;
                for (var attempt = 0; attempt < 20 && !isFailureSeen; attempt++) {
                    final var clientFd = connectRaw();
                    try {
                        final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                        try {
                            sendRaw(clientFd, clientHello);
                            Os.sleep(10L * (attempt + 1));
                            try {
                                tlsSocket.startTlsSession(null);
                                continue; // the ClientHello had not arrived yet
                            } catch (TlsSessionInitFailedException e) {
                                isFailureSeen = true;
                                final var message = e.getFlyweightMessage().toString();
                                for (var i = 0; i < message.length(); i++) {
                                    final var c = message.charAt(i);
                                    assertTrue("non-printable character in: " + message, c >= 0x20 && c <= 0x7e);
                                }
                                if (message.contains("FAKE")) {
                                    TestUtils.assertContains(message, "a\\u000d\\u000aFAKE");
                                }
                            }
                        } finally {
                            tlsSocket.close();
                        }
                    } finally {
                        Net.close(clientFd);
                    }
                }
                assertTrue("the ClientHello never arrived before startTlsSession()", isFailureSeen);
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testKeyUpdateAfterTls13HandshakeKeepsWorking() throws Exception {
        // In TLS 1.3 a client-side beginHandshake() after the handshake sends a KeyUpdate, which the engine handles
        // without a delegated task: the connection must keep working in both directions.
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, "TLSv1.3", (client, server, buf) -> {
            client.startNewHandshake();
            final var plain = "after key update".getBytes(StandardCharsets.UTF_8);
            client.writeRaw(client.encrypt(plain));
            assertEquals(plain.length, recvUntilNonZero(server, buf, 4096));
            assertEquals(plain.length, server.send(buf, plain.length));
            assertArrayEquals(plain, client.readApplicationData(plain.length));
        });
    }

    @Test
    public void testKeyUpdateAnswerGrowsEmptyOutputBufferOnEngineRequest() throws Exception {
        // The JDK engine wraps the answer to a key update into any output buffer that holds one packet, and the
        // handshake grows the buffer to that size. An engine that asks for more room on an empty buffer gets it:
        // recv() doubles the buffer and wraps the answer again.
        final var engine = new ScriptedEngine();
        final var packetSize = packetBufferSize();
        assertWithMemoryHandshake(engine.context(), packetSize, TLS_13, (client, server, transport, buf) -> {
            engine.wrapStep = (delegate, src, dst) -> {
                engine.wrapStep = null;
                return new SSLEngineResult(SSLEngineResult.Status.BUFFER_OVERFLOW, delegate.getHandshakeStatus(), 0, 0);
            };
            final var wireBefore = client.wireSize();
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            client.keyUpdate();
            assertEquals("a key update carries no data", 0, server.recv(buf, MEMORY_BUF_SIZE));
            assertEquals("the output buffer doubles", packetSize, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMem);
            assertTrue("the answer waits in the output buffer", server.wantsTlsWrite());
            assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
            assertFalse(server.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
            assertEquals("one answer", 1, countRecords(client.wire(), wireBefore));
        });
    }

    @Test
    public void testKeyUpdateAnswerNotProducedDoesNotSpinRecv() throws Exception {
        // An engine that owes the answer to a key update, yet wraps nothing when asked for it, would keep recv()
        // asking. recv() goes on instead, and the answer goes out once the engine wraps it, behind the next record.
        final var engine = new ScriptedEngine();
        assertWithMemoryHandshake(engine.context(), -1, TLS_13, (client, server, transport, buf) -> {
            engine.wrapStep = (delegate, src, dst) -> {
                engine.wrapStep = null;
                return new SSLEngineResult(SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NEED_WRAP, 0, 0);
            };
            final var wireBefore = client.wireSize();
            client.keyUpdate();
            assertEquals("a key update carries no data", 0, server.recv(buf, MEMORY_BUF_SIZE));
            assertFalse("the engine wrapped no answer", server.wantsTlsWrite());
            final var data = "next".getBytes(StandardCharsets.UTF_8);
            client.write(data);
            assertEquals(data.length, server.recv(buf, MEMORY_BUF_SIZE));
            assertNativeBytes(data, buf);
            assertTrue("the answer waits in the output buffer", server.wantsTlsWrite());
            assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
            client.pump();
            client.assertHealthy();
            assertEquals("one answer", 1, countRecords(client.wire(), wireBefore));
        });
    }

    @Test
    public void testKeyUpdateAnswersGrowBufferConfiguredSmall() throws Exception {
        // In a buffer configured to one packet the second answer to a key update no longer fits behind the first.
        // With the socket taking nothing, the buffer grows rather than giving the connection up, and the answers go
        // out once the socket drains.
        final var packetSize = packetBufferSize();
        final var updates = 10;
        assertWithMemoryHandshake(packetSize, TLS_13, (client, server, transport, buf) -> {
            transport.sendBudget = 0;
            final var wireBefore = client.wireSize();
            final var sentBefore = transport.toClient.totalWritten;
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            final var data = client.floodKeyUpdates(updates);
            assertEquals(data.length, server.recv(buf, MEMORY_BUF_SIZE));
            assertNativeBytes(data, buf);
            assertEquals("the output buffer doubles to keep the answers", packetSize, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMem);
            assertTrue(server.wantsTlsWrite());
            assertEquals("a socket that takes nothing gets nothing", sentBefore, transport.toClient.totalWritten);
            transport.sendBudget = Long.MAX_VALUE;
            assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
            assertFalse(server.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
            assertEquals("one answer per key update", updates, countRecords(client.wire(), wireBefore));
            // each answer changes the key the client reads with: data sent next decrypts only behind all of them
            final var next = "next".getBytes(StandardCharsets.UTF_8);
            putBytes(buf, next);
            assertEquals(next.length, server.send(buf, next.length));
            client.pump();
            client.assertHealthy();
            assertArrayEquals(next, client.plaintext());
        });
    }

    @Test
    public void testKeyUpdateFloodAnswersGoOutBeforeCloseNotify() throws Exception {
        // recv() gives up a key update flood that fills the default output buffer while the peer reads nothing (see
        // testKeyUpdateFloodAtDefaultBufferSizeFailsRecv). When the peer reads again by the time the server closes,
        // close() sends the answers, the one it still owes included, and close_notify last.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            transport.sendBudget = 0;
            final var wireBefore = client.wireSize();
            client.floodKeyUpdates(KEY_UPDATE_FLOOD);
            assertEquals(-1, server.recv(buf, MEMORY_BUF_SIZE));
            transport.sendBudget = Long.MAX_VALUE;
            server.close();
            client.pump();
            // Each answer changes the key the client reads with, so close_notify decrypts only behind every
            // answer the server produced.
            client.assertHealthy();
            assertTrue("close_notify must follow the answers", client.isCloseNotifySeen);
            final var answers = countRecords(client.wire(), wireBefore) - 1;
            assertTrue("answers: " + answers, answers > 0 && answers < KEY_UPDATE_FLOOD);
        });
    }

    @Test
    public void testKeyUpdateFloodAtDefaultBufferSizeFailsRecv() throws Exception {
        // A peer that asks for key updates and reads none of the answers fills the output buffer with them. At the
        // default size the buffer does not grow: recv() gives the connection up, and close() frees it without the
        // answer and the close_notify it still owes.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            transport.sendBudget = 0;
            final var sentBefore = transport.toClient.totalWritten;
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            client.floodKeyUpdates(KEY_UPDATE_FLOOD);
            assertEquals(-1, server.recv(buf, MEMORY_BUF_SIZE));
            assertEquals("the output buffer must not grow past the default size", tlsMem, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS));
            assertEquals("a socket that takes nothing gets nothing", sentBefore, transport.toClient.totalWritten);
            assertTrue(server.wantsTlsWrite());
            // the socket still takes nothing when the harness closes the connection
        });
    }

    @Test
    public void testKeyUpdateFloodIsAnsweredWhilePeerReads() throws Exception {
        // A peer that reads its answers may ask for key updates as often as it likes: when the answers fill the
        // output buffer, recv() writes them to the socket itself instead of giving the connection up.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            final var wireBefore = client.wireSize();
            final var sentBefore = transport.toClient.totalWritten;
            final var data = client.floodKeyUpdates(KEY_UPDATE_FLOOD);
            assertEquals(data.length, server.recv(buf, MEMORY_BUF_SIZE));
            assertNativeBytes(data, buf);
            assertTrue("recv() writes out the answers that fill the output buffer", transport.toClient.totalWritten > sentBefore);
            while (server.wantsTlsWrite()) {
                assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
            }
            client.pump();
            client.assertHealthy();
            assertEquals("one answer per key update", KEY_UPDATE_FLOOD, countRecords(client.wire(), wireBefore));
            final var after = "after".getBytes(StandardCharsets.UTF_8);
            putBytes(buf, after);
            assertEquals(after.length, server.send(buf, after.length));
            client.pump();
            client.assertHealthy();
            assertArrayEquals(after, client.plaintext());
        });
    }

    @Test
    public void testKeyUpdateOwedBySendGoesOutAheadOfData() throws Exception {
        // A TLS 1.3 engine that owes the peer a key update wraps it ahead of the data send() gives it, and consumes
        // none of the data on that wrap. send() goes on to the data. When the socket takes only part of the key
        // update, send() reports nothing sent, since none of the data is encrypted yet.
        final var engine = new ScriptedEngine();
        assertWithMemoryHandshake(engine.context(), -1, TLS_13, (client, server, transport, buf) -> {
            final var wireBefore = client.wireSize();
            // on an established TLS 1.3 session the engine queues a key update
            engine.delegate.beginHandshake();
            final var data = pattern(100, 0);
            putBytes(buf, data);
            transport.sendBudget = 5;
            assertEquals("the socket took part of the key update and none of the data", 0, server.send(buf, data.length));
            assertTrue(server.wantsTlsWrite());
            transport.sendBudget = Long.MAX_VALUE;
            assertEquals(data.length, server.send(buf, data.length));
            assertFalse(server.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
            assertArrayEquals(data, client.plaintext());
            assertEquals("the key update, then the data", 2, countRecords(client.wire(), wireBefore));
        });
    }

    @Test
    public void testOversizedRecordGrowsPlaintextBufferBeforeFailing() throws Exception {
        // A record as long as the session's packet size carries more ciphertext than TLS 1.3 allows. SSLEngine
        // estimates its plaintext from its length before it authenticates it, finds the estimate larger than the
        // plaintext buffer and asks for a larger one: recv() grows the buffer instead of spinning, and fails when
        // the record does not authenticate. The bytes behind the record fill the input buffer, which recv() must
        // not then read into.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            final var session = client.engine.getSession();
            final var stagingCapacity = session.getApplicationBufferSize();
            final var length = session.getPacketBufferSize() - 5;
            assertTrue(
                    "the plaintext SSLEngine estimates for the record must exceed the plaintext buffer",
                    length - AEAD_TAG_SIZE > stagingCapacity
            );
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            final var record = oversizedRecord(length);
            transport.toServer.append(record, 0, record.length);
            final var filler = new byte[256 * 1024];
            transport.toServer.append(filler, 0, filler.length);
            // a caller buffer smaller than a record gets the record through the plaintext buffer
            assertEquals(-1, server.recv(buf, 4096));
            assertEquals("the plaintext buffer doubles", stagingCapacity, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMem);
            assertEquals("recv() must not read into a full input buffer", 0, transport.zeroLengthReads);
        });
    }

    @Test
    public void testPartialClientHelloWaitsForMore() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientFd = connectRaw();
                final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                try {
                    // the first 5 bytes of a TLS record header announcing a 512 byte ClientHello
                    sendRaw(clientFd, new byte[]{0x16, 0x03, 0x01, 0x02, 0x00});
                    tlsSocket.startTlsSession(null);
                    assertTrue(tlsSocket.isHandshakeInProgress());
                    final var buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                    try {
                        // however much of the partial record has arrived, recv() waits for the rest
                        assertEquals(0, tlsSocket.recv(buf, 64));
                        sendRaw(clientFd, new byte[]{0x01, 0x00});
                        assertEquals(0, tlsSocket.recv(buf, 64));
                        assertTrue(tlsSocket.isHandshakeInProgress());
                        Net.close(clientFd);
                        assertEquals(-1, recvUntilNonZero(tlsSocket, buf, 64));
                    } finally {
                        Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    tlsSocket.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testPartialRecordWaitsForRest() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, null, (client, server, buf) -> {
            final var plain = "split across two writes".getBytes(StandardCharsets.UTF_8);
            final var record = client.encrypt(plain);
            client.writeRaw(Arrays.copyOf(record, 7));
            assertEquals(0, server.recv(buf, 4096));
            assertFalse(server.isMorePlaintextBuffered());
            client.writeRaw(Arrays.copyOfRange(record, 7, record.length));
            final var n = recvUntilNonZero(server, buf, 4096);
            assertEquals(plain.length, n);
            for (var i = 0; i < n; i++) {
                assertEquals(plain[i], Unsafe.getUnsafe().getByte(buf + i));
            }
        });
    }

    @Test
    public void testPlaintextRecvBeforeHandshake() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientFd = connectRaw();
                try {
                    final var tlsSocket = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                    assertTrue("supportsTls should be true", tlsSocket.supportsTls());
                    assertFalse("session not started yet", tlsSocket.isTlsSessionStarted());
                    // Send plaintext from client before TLS handshake (this is what PGWire does with SSLRequest)
                    final var msg = new byte[]{0x00, 0x00, 0x00, 0x08, 0x04, (byte) 0xd2, 0x16, 0x2f};
                    sendRaw(clientFd, msg);
                    final var recvBuf = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
                    try {
                        var received = 0;
                        final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                        while (received < msg.length) {
                            assertTrue("timed out waiting for the plaintext", System.nanoTime() < deadline);
                            final var n = tlsSocket.recv(recvBuf + received, 256 - received);
                            assertTrue(n >= 0);
                            received += n;
                        }
                        assertEquals(msg.length, received);
                        for (var i = 0; i < msg.length; i++) {
                            assertEquals(msg[i], Unsafe.getUnsafe().getByte(recvBuf + i));
                        }
                        // plaintext send works too, and the dispatcher hooks are inert
                        assertEquals(1, tlsSocket.send(recvBuf, 1));
                        assertEquals(0, tlsSocket.tlsIO(Socket.WRITE_FLAG | Socket.READ_FLAG));
                        assertFalse(tlsSocket.wantsTlsWrite());
                        assertEquals(0, tlsSocket.shutdown(Net.SHUT_WR));
                    } finally {
                        Unsafe.free(recvBuf, 256, MemoryTag.NATIVE_DEFAULT);
                    }
                    tlsSocket.close();
                } finally {
                    Net.close(clientFd);
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testRenegotiationAfterTls12HandshakeFailsInsteadOfSpinning() throws Exception {
        // A ClientHello after the handshake leaves the engine waiting for a delegated task; recv() and send() used
        // to spin forever on such a connection. Anyone who can complete a handshake could trigger it.
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, "TLSv1.2", (client, server, buf) -> {
            client.startNewHandshake();
            client.writeRaw(client.encrypt("after renegotiation".getBytes(StandardCharsets.UTF_8)));
            assertEquals(-1, recvUntilNonZero(server, buf, 4096));
            assertEquals(-1, server.send(buf, 16));
        });
    }

    @Test
    public void testResumedTls12HandshakeCompletesOnClientFinished() throws Exception {
        // In an abbreviated (resumed) TLS 1.2 handshake the client's Finished is the last message: the server
        // completes the handshake when it unwraps it, with nothing left to send.
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var clientContext = trustAllContext();
            final var serverFd = listen();
            byte[] firstSessionId = null;
            try {
                for (var connection = 0; connection < 2; connection++) {
                    try (var client = new EngineClient(boundPort, "TLSv1.2", clientContext)) {
                        final var server = acceptTls(serverFd, NetworkFacadeImpl.INSTANCE, sslContext);
                        final var buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                        try {
                            server.startTlsSession(null);
                            final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                            while (!client.isHandshakeDone() || server.isHandshakeInProgress()) {
                                assertTrue("handshake timed out", System.nanoTime() < deadline);
                                client.stepHandshake();
                                assertEquals(0, server.recv(buf, 4096));
                                if (server.wantsTlsWrite()) {
                                    assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
                                }
                            }
                            if (connection == 0) {
                                firstSessionId = client.sessionId();
                            } else {
                                assertArrayEquals("the second connection must resume the session", firstSessionId, client.sessionId());
                            }
                            final var plain = ("connection " + connection).getBytes(StandardCharsets.UTF_8);
                            client.writeRaw(client.encrypt(plain));
                            assertEquals(plain.length, recvUntilNonZero(server, buf, 4096));
                        } finally {
                            Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
                            server.close();
                        }
                    }
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testSendAfterFailedRecordSendsAlert() throws Exception {
        // A record that fails authentication leaves the engine closed with a fatal alert to send. send() wraps the
        // alert instead of the caller's data and reports the closed engine, and close() delivers the alert, so the
        // client learns why the connection ended.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            final var record = client.encrypt("tampered".getBytes(StandardCharsets.UTF_8));
            record[record.length - 1] ^= 1; // breaks the record's authentication tag
            transport.toServer.append(record, 0, record.length);
            assertEquals(-1, server.recv(buf, MEMORY_BUF_SIZE));
            putBytes(buf, pattern(16, 0));
            assertEquals(-1, server.send(buf, 16));
            server.close();
            client.pump();
            assertNotNull("the client must learn why the connection ended", client.failure);
            TestUtils.assertContains(client.failure.getMessage(), "Received fatal alert: bad_record_mac");
            assertEquals("the caller's data never reaches the client", 0, client.plaintext().length);
        });
    }

    @Test
    public void testSendAfterPeerCloseNotifyFails() throws Exception {
        // TLS 1.2 closes the outbound side when the peer's close_notify arrives
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, "TLSv1.2", (client, server, buf) -> {
            client.writeRaw(client.closeNotify());
            assertEquals(-1, recvUntilNonZero(server, buf, 4096));
            assertEquals(-1, server.send(buf, 16));
        });
    }

    @Test
    public void testSendAfterShortWriteMustPresentTheSameBytes() throws Exception {
        // After a short write the record is encrypted and partly on the wire: the caller must present the same
        // bytes again. Different bytes would land behind what the peer already has, so send() fails instead.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            final var first = pattern(100, 0);
            putBytes(buf, first);
            transport.sendBudget = 10;
            assertEquals("a record the socket took in part reports no plaintext", 0, server.send(buf, first.length));
            assertTrue(server.wantsTlsWrite());
            transport.sendBudget = Long.MAX_VALUE;
            final var other = first.clone();
            other[other.length - 1] ^= 1;
            putBytes(buf, other);
            assertEquals(-1, server.send(buf, other.length));
            client.pump();
            client.assertHealthy();
            assertArrayEquals("the peer gets the bytes that were encrypted", first, client.plaintext());
        });
    }

    @Test
    public void testSendAfterShortWriteWithTheSameBytesSendsThemOnce() throws Exception {
        // The caller presents the bytes of a short write again, alone or followed by new ones: send() reports them
        // once the socket has their whole record, and encrypts only the new ones.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            final var wireBefore = client.wireSize();
            final var first = pattern(100, 0);
            final var second = pattern(50, 100);
            final var third = pattern(30, 150);
            putBytes(buf, first);
            transport.sendBudget = 10;
            assertEquals(0, server.send(buf, first.length));
            transport.sendBudget = Long.MAX_VALUE;
            final var firstAndSecond = concat(first, second);
            putBytes(buf, firstAndSecond);
            assertEquals(firstAndSecond.length, server.send(buf, firstAndSecond.length));
            putBytes(buf, third);
            transport.sendBudget = 1;
            assertEquals(0, server.send(buf, third.length));
            transport.sendBudget = Long.MAX_VALUE;
            assertEquals(third.length, server.send(buf, third.length));
            assertFalse(server.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
            assertArrayEquals(concat(firstAndSecond, third), client.plaintext());
            assertEquals("one record per send() that encrypted", 3, countRecords(client.wire(), wireBefore));
        });
    }

    @Test
    public void testSendGrowsOutputBufferOnEngineRequest() throws Exception {
        // The handshake grows the output buffer to the packet size the JDK engine wraps every record into. An engine
        // whose records outgrow it after the handshake asks for room when send() wraps data: send() doubles the
        // buffer and wraps again.
        final var engine = new ScriptedEngine();
        final var packetSize = packetBufferSize();
        assertWithMemoryHandshake(engine.context(), packetSize, TLS_13, (client, server, transport, buf) -> {
            engine.wrapStep = (delegate, src, dst) -> {
                engine.wrapStep = null;
                return new SSLEngineResult(SSLEngineResult.Status.BUFFER_OVERFLOW, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, 0);
            };
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            final var data = pattern(100, 0);
            putBytes(buf, data);
            assertEquals(data.length, server.send(buf, data.length));
            assertEquals("the output buffer doubles", packetSize, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS) - tlsMem);
            assertFalse(server.wantsTlsWrite());
            client.pump();
            client.assertHealthy();
            assertArrayEquals(data, client.plaintext());
        });
    }

    @Test
    public void testSendSocketErrorAfterHandshake() throws Exception {
        final var isSendFailing = new AtomicBoolean();
        final var facade = new NetworkFacadeImpl() {
            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                return isSendFailing.get() ? -1 : super.sendRaw(fd, buffer, bufferLen);
            }
        };
        assertWithHandshake(facade, null, (client, server, buf) -> {
            isSendFailing.set(true);
            Unsafe.getUnsafe().setMemory(buf, 16, (byte) 'x');
            assertEquals(-1, server.send(buf, 16));
        });
    }

    @Test
    public void testSendUnderMemoryLimitFailsWhenShortWriteCannotBeKept() throws Exception {
        // After a short write send() keeps a copy of the plaintext it encrypted. When the RSS limit forbids the
        // copy, send() fails the connection instead of throwing at the caller.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            putBytes(buf, pattern(100, 0));
            transport.sendBudget = 10;
            final int n;
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
            try {
                n = server.send(buf, 100);
            } finally {
                Unsafe.setRssMemLimit(0);
            }
            assertEquals(-1, n);
        });
    }

    @Test
    public void testShutdownSendsQueuedBytesAndCloseNotifyBeforeFin() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, null, (client, server, buf) -> {
            final var plain = "last words".getBytes(StandardCharsets.UTF_8);
            for (var i = 0; i < plain.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, plain[i]);
            }
            assertEquals(plain.length, server.send(buf, plain.length));
            assertEquals(0, server.shutdown(Net.SHUT_WR));
            // the client reads the data, then a close_notify, then EOF
            assertArrayEquals(plain, client.readApplicationData(plain.length));
            assertTrue("expected the server's close_notify", client.readUntilClosed());
            // after the shutdown nothing more can be sent
            assertEquals(-1, server.send(buf, plain.length));
        });
    }

    @Test
    public void testShutdownUnderMemoryLimitStillShutsDown() throws Exception {
        // A record the socket did not take leaves no room for close_notify in a buffer configured to one packet,
        // and the RSS limit forbids growing it: shutdown() still shuts the socket down.
        assertWithMemoryHandshake(packetBufferSize(), TLS_13, (client, server, transport, buf) -> {
            transport.sendBudget = 0;
            putBytes(buf, pattern(100, 0));
            assertEquals(0, server.send(buf, 100));
            final var tlsMem = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            final int result;
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
            try {
                result = server.shutdown(Net.SHUT_WR);
            } finally {
                Unsafe.setRssMemLimit(0);
            }
            assertEquals(0, result);
            assertEquals("shutdown() must shut the socket down without close_notify", 1, transport.shutdownWriteCalls);
            assertEquals("no buffer grew", tlsMem, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS));
            assertTrue("the record the socket did not take stays queued", server.wantsTlsWrite());
        });
    }

    @Test
    public void testShutdownWithFailingTransportStillShutsDown() throws Exception {
        // The socket fails to write close_notify: shutdown() still shuts it down, and a later send() reports the
        // transport's own error.
        assertWithMemoryHandshake(-1, TLS_13, (client, server, transport, buf) -> {
            transport.sendErrorCode = Net.EOTHERDISCONNECT;
            assertEquals(0, server.shutdown(Net.SHUT_WR));
            assertEquals(1, transport.shutdownWriteCalls);
            assertTrue("close_notify stays queued", server.wantsTlsWrite());
            putBytes(buf, pattern(1, 0));
            assertEquals(Net.EOTHERDISCONNECT, server.send(buf, 1));
        });
    }

    @Test
    public void testShutdownWithFullSocketStillShutsDown() throws Exception {
        final var isSocketFull = new AtomicBoolean();
        final var facade = new NetworkFacadeImpl() {
            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                return isSocketFull.get() ? 0 : super.sendRaw(fd, buffer, bufferLen);
            }
        };
        assertWithHandshake(facade, null, (client, server, buf) -> {
            // shutting down the read side leaves the TLS session alone
            assertEquals(0, server.shutdown(0));
            final var plain = "still open".getBytes(StandardCharsets.UTF_8);
            for (var i = 0; i < plain.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, plain[i]);
            }
            assertEquals(plain.length, server.send(buf, plain.length));
            assertArrayEquals(plain, client.readApplicationData(plain.length));
            // with the socket full, close_notify stays queued; shutdown() does not wait for it
            isSocketFull.set(true);
            assertEquals(0, server.shutdown(Net.SHUT_WR));
            assertTrue(server.wantsTlsWrite());
        });
    }

    @Test
    public void testSmallBuffersGrowDuringHandshake() throws Exception {
        // 2 KiB holds the ClientHello but not the server's certificate flight, so the handshake wrap overflows
        // and the output buffer grows
        System.setProperty(BUFFER_SIZE_PROPERTY, "2048");
        try {
            assertEcho(NetworkFacadeImpl.INSTANCE, "small buffers".getBytes(StandardCharsets.UTF_8), 4096, false);
        } finally {
            System.clearProperty(BUFFER_SIZE_PROPERTY);
        }
    }

    @Test
    public void testTicketPartlySentStillHandsOverBufferedData() throws Exception {
        // The client sends data right behind its Finished. If the server's last handshake output (the TLS 1.3
        // session ticket) leaves bytes unsent, the handshake must still complete inside recv(), so the data
        // already pulled from the socket is returned: a dispatcher would not call recv() again, because the
        // socket has nothing left to read.
        final var isThrottling = new AtomicBoolean();
        final var wasThrottled = new AtomicBoolean();
        final var facade = new NetworkFacadeImpl() {
            @Override
            public int sendRaw(long fd, long buffer, int bufferLen) {
                if (isThrottling.get()) {
                    wasThrottled.set(true);
                    return super.sendRaw(fd, buffer, Math.min(bufferLen, 7));
                }
                return super.sendRaw(fd, buffer, bufferLen);
            }
        };
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try (var client = new EngineClient(boundPort, "TLSv1.3")) {
                final var server = acceptTls(serverFd, facade, sslContext);
                final var buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                try {
                    server.startTlsSession(null);
                    final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                    for (; ; ) {
                        assertTrue("handshake timed out", System.nanoTime() < deadline);
                        client.stepHandshake();
                        if (client.isHandshakeDone()) {
                            break;
                        }
                        assertEquals(0, server.recv(buf, 4096));
                        if (server.wantsTlsWrite()) {
                            assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
                        }
                    }
                    final var plain = "behind the Finished".getBytes(StandardCharsets.UTF_8);
                    client.writeRaw(client.encrypt(plain));
                    isThrottling.set(true);
                    var n = 0;
                    while (n == 0) {
                        assertTrue("the data never arrived", System.nanoTime() < deadline);
                        n = server.recv(buf, 4096);
                        // with the ticket partly unsent, recv() must not stop at the handshake
                        assertFalse(
                                "recv() returned with the handshake waiting only for output",
                                n == 0 && server.isHandshakeInProgress() && server.wantsTlsWrite()
                        );
                    }
                    assertEquals(plain.length, n);
                    assertTrue("the session ticket was never throttled", wasThrottled.get());
                    isThrottling.set(false);
                    while (server.wantsTlsWrite()) {
                        assertTrue("the ticket was never flushed", System.nanoTime() < deadline);
                        assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
                    }
                } finally {
                    Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
                    server.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testTinyBuffersGrowForHandshakeAndRecords() throws Exception {
        // 64 bytes fits neither the ClientHello, the server flight nor a full record: every buffer grows on demand
        System.setProperty(BUFFER_SIZE_PROPERTY, "64");
        try {
            final var payload = new byte[300_000];
            new SecureRandom().nextBytes(payload);
            // a 1000 byte destination keeps the grown input buffer full of whole records between calls
            assertEcho(NetworkFacadeImpl.INSTANCE, payload, 1000, true);
        } finally {
            System.clearProperty(BUFFER_SIZE_PROPERTY);
        }
    }

    @Test
    public void testTls12HandshakeAndEcho() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, "TLSv1.2", (client, server, buf) -> {
            final var plain = "tls 1.2".getBytes(StandardCharsets.UTF_8);
            client.writeRaw(client.encrypt(plain));
            assertEquals(plain.length, recvUntilNonZero(server, buf, 4096));
            assertEquals(plain.length, server.send(buf, plain.length));
            assertArrayEquals(plain, client.readApplicationData(plain.length));
        });
    }

    private static JavaTlsServerSocket acceptTls(long serverFd, NetworkFacade nf, SSLContext sslContext) {
        final var acceptedFd = Net.accept(serverFd);
        assertTrue(acceptedFd > 0);
        Net.configureNonBlocking(acceptedFd);
        final var tlsSocket = new JavaTlsServerSocket(nf, LOG, sslContext);
        tlsSocket.of(acceptedFd);
        return tlsSocket;
    }

    private static void appendBytes(ByteArrayOutputStream out, long buf, int n) {
        for (var i = 0; i < n; i++) {
            out.write(Unsafe.getUnsafe().getByte(buf + i));
        }
    }

    private static void assertEcho(NetworkFacade nf, byte[] payload, int recvBufferSize, boolean expectBufferedPlaintext) throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try {
                final var clientError = new AtomicReference<Throwable>();
                final var echoed = new AtomicReference<byte[]>();
                final var port = boundPort;
                final var client = new Thread(() -> {
                    try (var socket = (SSLSocket) trustAllContext().getSocketFactory().createSocket("127.0.0.1", port)) {
                        socket.setSoTimeout((int) (TIMEOUT_NANOS / 1_000_000));
                        socket.startHandshake();
                        final OutputStream out = socket.getOutputStream();
                        out.write(payload);
                        out.flush();
                        final InputStream in = socket.getInputStream();
                        echoed.set(in.readNBytes(payload.length));
                    } catch (Throwable th) {
                        clientError.set(th);
                    }
                });
                client.start();
                final var tlsSocket = acceptTls(serverFd, nf, sslContext);
                final var buf = Unsafe.malloc(recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    tlsSocket.startTlsSession(null);
                    final var received = new byte[payload.length];
                    var receivedLen = 0;
                    var sawBufferedPlaintext = false;
                    final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                    while (receivedLen < payload.length) {
                        assertTrue("timed out waiting for the payload", System.nanoTime() < deadline);
                        if (tlsSocket.wantsTlsWrite()) {
                            assertEquals(0, tlsSocket.tlsIO(Socket.WRITE_FLAG));
                        }
                        final var n = tlsSocket.recv(buf, Math.min(recvBufferSize, payload.length - receivedLen));
                        assertTrue("recv failed: " + n + ", client error: " + clientError.get(), n >= 0);
                        for (var i = 0; i < n; i++) {
                            received[receivedLen + i] = Unsafe.getUnsafe().getByte(buf + i);
                        }
                        receivedLen += n;
                        sawBufferedPlaintext |= tlsSocket.isMorePlaintextBuffered();
                        if (n == 0 && !tlsSocket.isMorePlaintextBuffered()) {
                            Os.pause();
                        }
                    }
                    assertFalse(tlsSocket.isHandshakeInProgress());
                    assertTrue(tlsSocket.isTlsSessionStarted());
                    assertArrayEquals(payload, received);
                    if (expectBufferedPlaintext) {
                        assertTrue("expected recv() to leave plaintext buffered", sawBufferedPlaintext);
                    }
                    // echo back
                    var sentLen = 0;
                    while (sentLen < payload.length || tlsSocket.wantsTlsWrite()) {
                        assertTrue("timed out sending the echo", System.nanoTime() < deadline);
                        if (sentLen < payload.length) {
                            final var chunk = Math.min(recvBufferSize, payload.length - sentLen);
                            for (var i = 0; i < chunk; i++) {
                                Unsafe.getUnsafe().putByte(buf + i, payload[sentLen + i]);
                            }
                            final var n = tlsSocket.send(buf, chunk);
                            assertTrue(n >= 0);
                            sentLen += n;
                        } else {
                            assertEquals(0, tlsSocket.tlsIO(Socket.WRITE_FLAG));
                        }
                    }
                    // the JDK client waits for our close_notify when it closes, so close before joining it
                    tlsSocket.close();
                    client.join(TIMEOUT_NANOS / 1_000_000);
                    assertFalse("client did not finish", client.isAlive());
                    assertNull("client failed", clientError.get());
                    assertArrayEquals(payload, echoed.get());
                } finally {
                    Unsafe.free(buf, recvBufferSize, MemoryTag.NATIVE_DEFAULT);
                    tlsSocket.close();
                    client.join(TIMEOUT_NANOS / 1_000_000);
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    private static void assertNativeBytes(byte[] expected, long address) {
        for (var i = 0; i < expected.length; i++) {
            assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(address + i));
        }
    }

    private static void assertWithHandshake(NetworkFacade nf, String clientProtocol, HandshakeScenario scenario) throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var serverFd = listen();
            try (var client = new EngineClient(boundPort, clientProtocol)) {
                final var server = acceptTls(serverFd, nf, sslContext);
                final var buf = Unsafe.malloc(4096, MemoryTag.NATIVE_DEFAULT);
                try {
                    server.startTlsSession(null);
                    final var deadline = System.nanoTime() + TIMEOUT_NANOS;
                    while (!client.isHandshakeDone() || server.isHandshakeInProgress()) {
                        assertTrue("handshake timed out", System.nanoTime() < deadline);
                        client.stepHandshake();
                        assertEquals("no application data during the handshake", 0, server.recv(buf, 4096));
                        if (server.wantsTlsWrite()) {
                            assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
                        }
                    }
                    scenario.run(client, server, buf);
                } finally {
                    Unsafe.free(buf, 4096, MemoryTag.NATIVE_DEFAULT);
                    server.close();
                }
            } finally {
                Net.close(serverFd);
            }
        });
    }

    /**
     * Runs {@code scenario} on a connection that lives in the test: the socket reads and writes through a
     * {@link MemoryTransport}, and a JDK {@link SSLEngine} client sits on its other end, so every byte that moves is
     * the test's decision and nothing depends on timing. The TLS session has not started when the scenario starts.
     * The connection is closed after it, and close() must close the fd once and free every TLS buffer.
     */
    private static void assertWithMemoryConnection(SSLContext serverContext, String clientProtocol, MemoryScenario scenario) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var transport = new MemoryTransport();
            final var server = new JavaTlsServerSocket(transport, LOG, serverContext);
            final var client = new MemoryClient(TlsFuzzSupport.newTrustAllContext(), transport, clientProtocol);
            final var tlsMemBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS);
            final var buf = Unsafe.malloc(MEMORY_BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                server.of(MEMORY_FD);
                scenario.run(client, server, transport, buf);
            } finally {
                Unsafe.free(buf, MEMORY_BUF_SIZE, MemoryTag.NATIVE_DEFAULT);
                server.close();
            }
            assertEquals("close() must close the fd exactly once", 1, transport.closeCalls);
            assertEquals("close() must free every TLS buffer", tlsMemBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TLS_RSS));
        });
    }

    private static void assertWithMemoryHandshake(int bufferSize, String clientProtocol, MemoryScenario scenario) throws Exception {
        assertWithMemoryHandshake(TlsFuzzSupport.newServerContext(), bufferSize, clientProtocol, scenario);
    }

    /**
     * Runs {@code scenario} as {@link #assertWithMemoryConnection} does, with the handshake complete when the
     * scenario starts.
     *
     * @param bufferSize the size the socket starts its buffers at, or -1 for the default
     */
    private static void assertWithMemoryHandshake(SSLContext serverContext, int bufferSize, String clientProtocol, MemoryScenario scenario) throws Exception {
        assertWithMemoryConnection(serverContext, clientProtocol, (client, server, transport, buf) -> {
            startTls(server, bufferSize);
            for (var steps = 0; !client.isHandshakeDone() || server.isHandshakeInProgress(); steps++) {
                assertTrue("the handshake did not complete", steps < 1_000);
                client.pump();
                client.assertHealthy();
                assertEquals("no application data during the handshake", 0, server.recv(buf, MEMORY_BUF_SIZE));
                if (server.wantsTlsWrite()) {
                    assertEquals(0, server.tlsIO(Socket.WRITE_FLAG));
                }
            }
            // the client takes what the server sends when its side of the handshake ends: the TLS 1.3 session ticket
            client.pump();
            client.assertHealthy();
            scenario.run(client, server, transport, buf);
        });
    }

    /**
     * Builds a ClientHello record with only a server_name extension. SunJSSE rejects the name if it is not a
     * valid host name and puts it, verbatim, into the error message.
     */
    private static byte[] clientHelloWithServerName(byte[] serverName) {
        final var body = new ByteArrayOutputStream();
        body.write(0x03);
        body.write(0x03); // TLS 1.2
        body.writeBytes(new byte[32]); // random
        body.write(0); // empty session id
        body.writeBytes(new byte[]{0x00, 0x04, 0x13, 0x01, 0x00, 0x2f}); // TLS_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_CBC_SHA
        body.writeBytes(new byte[]{0x01, 0x00}); // null compression
        final var nameLength = serverName.length;
        final var listLength = nameLength + 3;
        final var extensionLength = listLength + 2;
        final var extensionsLength = extensionLength + 4;
        body.write(extensionsLength >> 8);
        body.write(extensionsLength);
        body.writeBytes(new byte[]{0x00, 0x00}); // server_name
        body.write(extensionLength >> 8);
        body.write(extensionLength);
        body.write(listLength >> 8);
        body.write(listLength);
        body.write(0); // host_name
        body.write(nameLength >> 8);
        body.write(nameLength);
        body.writeBytes(serverName);
        final var handshake = body.toByteArray();
        final var record = new ByteArrayOutputStream();
        record.writeBytes(new byte[]{0x16, 0x03, 0x01});
        final var handshakeLength = handshake.length + 4;
        record.write(handshakeLength >> 8);
        record.write(handshakeLength);
        record.write(0x01); // client_hello
        record.write(handshake.length >> 16);
        record.write(handshake.length >> 8);
        record.write(handshake.length);
        record.writeBytes(handshake);
        return record.toByteArray();
    }

    private static byte[] concat(byte[] head, byte[] tail) {
        final var joined = Arrays.copyOf(head, head.length + tail.length);
        System.arraycopy(tail, 0, joined, head.length, tail.length);
        return joined;
    }

    private static long connectRaw() {
        final var clientFd = Net.socketTcp(true);
        assertTrue(clientFd > 0);
        final var addr = Net.sockaddr("127.0.0.1", boundPort);
        try {
            assertEquals(0, Net.connect(clientFd, addr));
        } finally {
            Net.freeSockAddr(addr);
        }
        return clientFd;
    }

    /**
     * @return the number of TLS records in {@code wire} from {@code offset} on, which must be a run of whole records
     */
    private static int countRecords(byte[] wire, int offset) {
        var count = 0;
        for (var at = offset; at < wire.length; count++) {
            assertTrue("a record cut short on the wire", at + 5 <= wire.length);
            at += 5 + (((wire[at + 3] & 0xff) << 8) | (wire[at + 4] & 0xff));
            assertTrue("a record cut short on the wire", at <= wire.length);
        }
        return count;
    }

    private static boolean isOpensslAvailable() {
        try {
            final var proc = new ProcessBuilder("openssl", "version")
                    .redirectErrorStream(true).start();
            return proc.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Opens a listening socket on a random free loopback port and remembers it in {@link #boundPort}.
     */
    private static long listen() {
        final var serverFd = Net.socketTcp(true);
        assertTrue(serverFd > 0);
        Net.setReuseAddress(serverFd);
        for (var attempt = 0; attempt < 100; attempt++) {
            final int port;
            try (var probe = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
                port = probe.getLocalPort();
            } catch (java.io.IOException e) {
                continue;
            }
            // bind the loopback address itself: a wildcard bind would succeed next to another process's
            // loopback listener, and the client below would then reach that process instead
            if (Net.bindTcp(serverFd, "127.0.0.1", port)) {
                Net.listen(serverFd, 4);
                boundPort = port;
                return serverFd;
            }
        }
        Net.close(serverFd);
        throw new AssertionError("could not find a free port");
    }

    /**
     * @return an application data record with {@code length} bytes of noise as its ciphertext, which fails
     * authentication
     */
    private static byte[] oversizedRecord(int length) {
        final var record = new byte[5 + length];
        record[0] = 0x17; // application_data
        record[1] = 0x03;
        record[2] = 0x03; // the version every TLS 1.2 and 1.3 record carries
        record[3] = (byte) (length >> 8);
        record[4] = (byte) length;
        for (var i = 5; i < record.length; i++) {
            record[i] = (byte) (i * 31);
        }
        return record;
    }

    /**
     * @return the room SSLEngine wants free in the output buffer before it wraps a record, for a session that has
     * received nothing larger than TLS allows
     */
    private static int packetBufferSize() throws Exception {
        return TlsFuzzSupport.newTrustAllContext().createSSLEngine().getSession().getPacketBufferSize();
    }

    private static byte[] pattern(int length, int seed) {
        final var bytes = new byte[length];
        for (var i = 0; i < length; i++) {
            bytes[i] = (byte) ('a' + (seed + i) % 26);
        }
        return bytes;
    }

    private static void putBytes(long address, byte[] bytes) {
        for (var i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
        }
    }

    private static int recvUntilNonZero(JavaTlsServerSocket socket, long buf, int len) {
        final var deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (System.nanoTime() < deadline) {
            final var n = socket.recv(buf, len);
            if (n != 0) {
                return n;
            }
            Os.sleep(5);
        }
        throw new AssertionError("recv() kept returning 0");
    }

    private static void sendRaw(long fd, byte[] bytes) {
        final var buf = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (var i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, bytes[i]);
            }
            assertEquals(bytes.length, Net.send(fd, buf, bytes.length));
        } finally {
            Unsafe.free(buf, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Starts TLS with the socket's buffers at {@code bufferSize}, or at the default size when it is -1. The size is
     * a JVM-wide property the socket reads when the session starts, so it is set for this call only.
     */
    private static void startTls(JavaTlsServerSocket server, int bufferSize) throws TlsSessionInitFailedException {
        if (bufferSize < 0) {
            server.startTlsSession(null);
            return;
        }
        System.setProperty(BUFFER_SIZE_PROPERTY, Integer.toString(bufferSize));
        try {
            server.startTlsSession(null);
        } finally {
            System.clearProperty(BUFFER_SIZE_PROPERTY);
        }
    }

    /**
     * Steps the handshake until the client has sent its Finished, which the server has not read yet.
     */
    private static void stepUntilClientFinished(MemoryClient client, JavaTlsServerSocket server, long buf) {
        for (var steps = 0; ; steps++) {
            assertTrue("the client's side of the handshake did not complete", steps < 1_000);
            client.pump();
            client.assertHealthy();
            if (client.isHandshakeDone()) {
                return;
            }
            assertEquals("no application data during the handshake", 0, server.recv(buf, MEMORY_BUF_SIZE));
        }
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

    @FunctionalInterface
    private interface EngineStep {
        SSLEngineResult apply(SSLEngine engine, ByteBuffer src, ByteBuffer dst) throws SSLException;
    }

    @FunctionalInterface
    private interface HandshakeScenario {
        void run(EngineClient client, JavaTlsServerSocket server, long buf) throws Exception;
    }

    @FunctionalInterface
    private interface MemoryScenario {
        void run(MemoryClient client, JavaTlsServerSocket server, MemoryTransport transport, long buf) throws Exception;
    }

    /**
     * Bytes in flight in one direction of a {@link MemoryTransport}.
     */
    private static final class ByteQueue {
        long totalWritten;
        private byte[] bytes = new byte[4096];
        private int readPos;
        private int writePos;

        void append(byte[] source, int offset, int length) {
            ensureRoom(length);
            System.arraycopy(source, offset, bytes, writePos, length);
            writePos += length;
            totalWritten += length;
        }

        void appendFromNative(long address, int length) {
            ensureRoom(length);
            Unsafe.copyMemory(null, address, bytes, Unsafe.BYTE_OFFSET + writePos, length);
            writePos += length;
            totalWritten += length;
        }

        int available() {
            return writePos - readPos;
        }

        void readToNative(long address, int length) {
            Unsafe.copyMemory(bytes, Unsafe.BYTE_OFFSET + readPos, null, address, length);
            readPos += length;
        }

        byte[] takeAll() {
            final var taken = Arrays.copyOfRange(bytes, readPos, writePos);
            readPos = writePos;
            return taken;
        }

        private void ensureRoom(int length) {
            if (writePos + length > bytes.length) {
                final var live = writePos - readPos;
                final var grown = new byte[Math.max(2 * bytes.length, live + length)];
                System.arraycopy(bytes, readPos, grown, 0, live);
                bytes = grown;
                readPos = 0;
                writePos = live;
            }
        }
    }

    /**
     * A TLS client on a non-blocking channel, driven step by step from the test thread, so a test can interleave it
     * with the server socket and send arbitrary, partial or corrupt records.
     */
    private static final class EngineClient implements AutoCloseable {
        private final SocketChannel channel;
        private final SSLEngine engine;
        private final ByteBuffer netIn = ByteBuffer.allocate(256 * 1024);

        EngineClient(int port, String protocol) throws Exception {
            this(port, protocol, trustAllContext());
        }

        EngineClient(int port, String protocol, SSLContext context) throws Exception {
            channel = SocketChannel.open(new InetSocketAddress("127.0.0.1", port));
            channel.configureBlocking(false);
            engine = context.createSSLEngine("localhost", port);
            engine.setUseClientMode(true);
            if (protocol != null) {
                engine.setEnabledProtocols(new String[]{protocol});
            }
            engine.beginHandshake();
        }

        @Override
        public void close() throws Exception {
            channel.close();
        }

        byte[] closeNotify() throws Exception {
            engine.closeOutbound();
            final var out = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
            engine.wrap(ByteBuffer.allocate(0), out);
            out.flip();
            final var bytes = new byte[out.remaining()];
            out.get(bytes);
            return bytes;
        }

        void closeChannel() throws Exception {
            channel.close();
        }

        byte[] sessionId() {
            return engine.getSession().getId();
        }

        byte[] encrypt(byte[] plain) throws Exception {
            final var src = ByteBuffer.wrap(plain);
            final var out = ByteBuffer.allocate(plain.length + 4 * engine.getSession().getPacketBufferSize());
            while (src.hasRemaining()) {
                assertEquals(SSLEngineResult.Status.OK, engine.wrap(src, out).getStatus());
            }
            out.flip();
            final var bytes = new byte[out.remaining()];
            out.get(bytes);
            return bytes;
        }

        boolean isHandshakeDone() {
            final var status = engine.getHandshakeStatus();
            return status == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING || status == SSLEngineResult.HandshakeStatus.FINISHED;
        }

        byte[] readApplicationData(int expected) throws Exception {
            final var app = ByteBuffer.allocate(Math.max(expected, engine.getSession().getApplicationBufferSize()) * 2);
            final var deadline = System.nanoTime() + TIMEOUT_NANOS;
            while (app.position() < expected) {
                assertTrue("timed out reading application data", System.nanoTime() < deadline);
                // unwrap whatever is already buffered (a session ticket may precede the data) before reading more
                netIn.flip();
                final var result = netIn.hasRemaining() ? engine.unwrap(netIn, app) : null;
                netIn.compact();
                if (result != null && result.getStatus() == SSLEngineResult.Status.OK && result.bytesConsumed() > 0) {
                    continue;
                }
                if (channel.read(netIn) < 0) {
                    throw new AssertionError("server closed the connection");
                }
            }
            app.flip();
            final var bytes = new byte[app.remaining()];
            app.get(bytes);
            return bytes;
        }

        /**
         * Reads until the server's close_notify and the end of the stream.
         *
         * @return true when a close_notify arrived before the end of the stream
         */
        boolean readUntilClosed() throws Exception {
            final var app = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
            final var deadline = System.nanoTime() + TIMEOUT_NANOS;
            var isCloseNotifySeen = false;
            for (; ; ) {
                assertTrue("timed out waiting for the server to close", System.nanoTime() < deadline);
                final var n = channel.read(netIn);
                netIn.flip();
                while (netIn.hasRemaining()) {
                    final var result = engine.unwrap(netIn, app);
                    app.clear();
                    if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                        isCloseNotifySeen = true;
                    }
                    if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW || result.bytesConsumed() == 0) {
                        break;
                    }
                }
                netIn.compact();
                if (n < 0) {
                    return isCloseNotifySeen;
                }
                if (n == 0) {
                    Os.sleep(5);
                }
            }
        }

        /**
         * Starts a new handshake on an established connection: a renegotiation in TLS 1.2, a KeyUpdate in TLS 1.3.
         */
        void startNewHandshake() throws Exception {
            engine.beginHandshake();
            final var out = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
            engine.wrap(ByteBuffer.allocate(0), out);
            out.flip();
            while (out.hasRemaining()) {
                channel.write(out);
            }
        }

        void stepHandshake() throws Exception {
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK -> {
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        task.run();
                    }
                }
                case NEED_WRAP -> {
                    final var out = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
                    engine.wrap(ByteBuffer.allocate(0), out);
                    out.flip();
                    while (out.hasRemaining()) {
                        channel.write(out);
                    }
                }
                case NEED_UNWRAP, NEED_UNWRAP_AGAIN -> {
                    if (channel.read(netIn) < 0) {
                        throw new AssertionError("server closed the connection during the handshake");
                    }
                    netIn.flip();
                    engine.unwrap(netIn, ByteBuffer.allocate(engine.getSession().getApplicationBufferSize()));
                    netIn.compact();
                }
                default -> {
                }
            }
        }

        void writeRaw(byte[] bytes) throws Exception {
            final var buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        }
    }

    /**
     * A JDK TLS client on the far end of a {@link MemoryTransport}, stepped from the test thread. It keeps every
     * byte the server sent, so a test can count the records on the wire, and every byte of plaintext it decrypted.
     */
    private static final class MemoryClient {
        private static final ByteBuffer NOTHING = ByteBuffer.allocate(0);
        final SSLEngine engine;
        SSLException failure;
        boolean isCloseNotifySeen;
        private final ByteBuffer netOut = ByteBuffer.allocate(64 * 1024);
        private final ByteArrayOutputStream plaintext = new ByteArrayOutputStream();
        private final MemoryTransport transport;
        private final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        private ByteBuffer appIn = ByteBuffer.allocate(64 * 1024);
        private boolean isHandshakeDone;
        private ByteBuffer netIn = ByteBuffer.allocate(64 * 1024);

        MemoryClient(SSLContext context, MemoryTransport transport, String protocol) throws SSLException {
            this.transport = transport;
            // host and port only name the entry in the context's session cache
            engine = context.createSSLEngine("localhost", 1);
            engine.setUseClientMode(true);
            engine.setEnabledProtocols(new String[]{protocol});
            engine.beginHandshake();
        }

        void assertHealthy() {
            if (failure != null) {
                throw new AssertionError("the client engine failed: " + failure, failure);
            }
        }

        /**
         * Gives the handshake up: closing the outbound side of an unfinished handshake sends user_canceled and
         * close_notify.
         */
        void cancel() throws SSLException {
            engine.closeOutbound();
            //noinspection StatementWithEmptyBody
            while (wrapControl() > 0) {
                // until the engine has wrapped everything it closes with
            }
        }

        byte[] encrypt(byte[] plain) throws SSLException {
            final var out = new ByteArrayOutputStream();
            final var src = ByteBuffer.wrap(plain);
            while (src.hasRemaining()) {
                netOut.clear();
                assertEquals(SSLEngineResult.Status.OK, engine.wrap(src, netOut).getStatus());
                netOut.flip();
                out.write(netOut.array(), 0, netOut.limit());
            }
            return out.toByteArray();
        }

        /**
         * Asks for {@code updates} key updates, each of which obliges the server to answer, with a data record of
         * one byte behind every {@value #KEY_UPDATES_PER_DATA_RECORD} of them and behind the last.
         *
         * @return the data sent along
         */
        byte[] floodKeyUpdates(int updates) throws SSLException {
            final var data = new ByteArrayOutputStream();
            for (var i = 1; i <= updates; i++) {
                keyUpdate();
                if (i % KEY_UPDATES_PER_DATA_RECORD == 0 || i == updates) {
                    final var b = (byte) ('a' + data.size() % 26);
                    write(new byte[]{b});
                    data.write(b);
                }
            }
            return data.toByteArray();
        }

        boolean isHandshakeDone() {
            return isHandshakeDone;
        }

        /**
         * Asks for a TLS 1.3 key update, which obliges the server to answer with one of its own.
         */
        void keyUpdate() throws SSLException {
            engine.beginHandshake();
            assertTrue("a key update takes a record", wrapControl() > 0);
        }

        byte[] plaintext() {
            return plaintext.toByteArray();
        }

        /**
         * Feeds the engine everything the server has written and does all the work that needs no more bytes from
         * the server.
         */
        void pump() {
            final var bytes = transport.toClient.takeAll();
            wire.writeBytes(bytes);
            if (netIn.remaining() < bytes.length) {
                final var bigger = ByteBuffer.allocate(Math.max(2 * netIn.capacity(), netIn.position() + bytes.length));
                netIn.flip();
                bigger.put(netIn);
                netIn = bigger;
            }
            netIn.put(bytes);
            if (failure != null) {
                // a failed engine reads nothing more
                return;
            }
            try {
                for (var steps = 0; ; steps++) {
                    assertTrue("the client engine spins", steps < 10_000_000);
                    switch (engine.getHandshakeStatus()) {
                        case NEED_TASK -> {
                            Runnable task;
                            while ((task = engine.getDelegatedTask()) != null) {
                                task.run();
                            }
                        }
                        case NEED_WRAP -> {
                            if (wrapControl() == 0) {
                                return;
                            }
                        }
                        default -> {
                            if (!unwrapRecord()) {
                                return;
                            }
                        }
                    }
                }
            } catch (SSLException e) {
                failure = e;
            }
        }

        byte[] wire() {
            return wire.toByteArray();
        }

        int wireSize() {
            return wire.size();
        }

        void write(byte[] plain) throws SSLException {
            final var bytes = encrypt(plain);
            transport.toServer.append(bytes, 0, bytes.length);
        }

        /**
         * @return false when the engine needs more bytes from the server to go on
         */
        private boolean unwrapRecord() throws SSLException {
            netIn.flip();
            try {
                if (!netIn.hasRemaining()) {
                    return false;
                }
                appIn.clear();
                final var result = engine.unwrap(netIn, appIn);
                if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                    isHandshakeDone = true;
                }
                plaintext.write(appIn.array(), 0, appIn.position());
                return switch (result.getStatus()) {
                    case BUFFER_OVERFLOW -> {
                        appIn = ByteBuffer.allocate(2 * appIn.capacity());
                        yield true;
                    }
                    case BUFFER_UNDERFLOW -> false;
                    case CLOSED -> {
                        // the server's close_notify; whatever follows it stays unread
                        isCloseNotifySeen |= result.bytesConsumed() > 0;
                        yield false;
                    }
                    default -> result.bytesConsumed() > 0
                            || engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK
                            || engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP;
                };
            } finally {
                netIn.compact();
            }
        }

        /**
         * Wraps one record of handshake, alert or key update output and puts it on the wire.
         *
         * @return the bytes wrapped
         */
        private int wrapControl() throws SSLException {
            netOut.clear();
            final var result = engine.wrap(NOTHING, netOut);
            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED) {
                isHandshakeDone = true;
            }
            netOut.flip();
            transport.toServer.append(netOut.array(), 0, netOut.limit());
            return netOut.limit();
        }
    }

    /**
     * A transport that lives in the test. The plain socket under the TLS socket reaches the network only through
     * these methods, keyed by a made-up fd, so its bytes go to two queues, and the test decides how many bytes each
     * write takes and when the peer hangs up.
     */
    private static final class MemoryTransport extends NetworkFacadeImpl {
        final ByteQueue toClient = new ByteQueue();
        final ByteQueue toServer = new ByteQueue();
        // the next writes take nothing, as when the peer's window is full
        int blockedSends;
        int closeCalls;
        // once the bytes for the server run out, reads report the end of the stream
        boolean isPeerClosed;
        // what writes return instead of taking bytes, such as a disconnect code; 0 lets them through
        int sendErrorCode;
        // how many bytes the writes take in all before they take nothing
        long sendBudget = Long.MAX_VALUE;
        int shutdownWriteCalls;
        // reads that asked for no bytes: recv(2) returns 0 for them, which the socket would take for a disconnect
        int zeroLengthReads;

        @Override
        public int close(long fd) {
            closeCalls++;
            return 0;
        }

        @Override
        public void close(long fd, Log logger) {
            closeCalls++;
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            if (bufferLen <= 0) {
                zeroLengthReads++;
                return Net.EOTHERDISCONNECT;
            }
            final var available = toServer.available();
            if (available == 0) {
                return isPeerClosed ? Net.EOTHERDISCONNECT : 0;
            }
            final var n = Math.min(bufferLen, available);
            toServer.readToNative(buffer, n);
            return n;
        }

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            if (sendErrorCode != 0) {
                return sendErrorCode;
            }
            if (shutdownWriteCalls > 0) {
                // as on a kernel socket, nothing goes out after shutdown(SHUT_WR)
                return Net.EPEERDISCONNECT;
            }
            if (blockedSends > 0) {
                blockedSends--;
                return 0;
            }
            final var n = (int) Math.min(bufferLen, sendBudget);
            sendBudget -= n;
            toClient.appendFromNative(buffer, n);
            return n;
        }

        @Override
        public int shutdown(long fd, int how) {
            if (how == Net.SHUT_WR) {
                shutdownWriteCalls++;
            }
            return 0;
        }
    }

    /**
     * A server engine that passes every call to a JDK engine, except where a test scripts it: a step decides the
     * result of a wrap or unwrap, and a mapper changes the handshake status the engine reports. The JDK engine never
     * gives the socket such results; the steps stand in for an engine that does.
     */
    private static final class ScriptedEngine extends SSLEngine {
        final SSLEngine delegate;
        // maps the handshake status the engine reports, in results and from getHandshakeStatus(); null keeps it
        UnaryOperator<SSLEngineResult.HandshakeStatus> handshakeStatusMapper;
        // makes isOutboundDone() report false, whatever the engine's state
        boolean isOutboundNeverDone;
        // decides the results of unwrap(src, dst) in place of the engine; null passes the calls on
        EngineStep unwrapStep;
        // decides the results of wrap(src, dst) in place of the engine; null passes the calls on
        EngineStep wrapStep;

        ScriptedEngine() throws Exception {
            delegate = TlsFuzzSupport.newServerContext().createSSLEngine();
        }

        @Override
        public void beginHandshake() throws SSLException {
            delegate.beginHandshake();
        }

        @Override
        public void closeInbound() throws SSLException {
            delegate.closeInbound();
        }

        @Override
        public void closeOutbound() {
            delegate.closeOutbound();
        }

        @Override
        public Runnable getDelegatedTask() {
            return delegate.getDelegatedTask();
        }

        @Override
        public boolean getEnableSessionCreation() {
            return delegate.getEnableSessionCreation();
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return delegate.getEnabledCipherSuites();
        }

        @Override
        public String[] getEnabledProtocols() {
            return delegate.getEnabledProtocols();
        }

        @Override
        public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
            return map(delegate.getHandshakeStatus());
        }

        @Override
        public boolean getNeedClientAuth() {
            return delegate.getNeedClientAuth();
        }

        @Override
        public SSLSession getSession() {
            return delegate.getSession();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public String[] getSupportedProtocols() {
            return delegate.getSupportedProtocols();
        }

        @Override
        public boolean getUseClientMode() {
            return delegate.getUseClientMode();
        }

        @Override
        public boolean getWantClientAuth() {
            return delegate.getWantClientAuth();
        }

        @Override
        public boolean isInboundDone() {
            return delegate.isInboundDone();
        }

        @Override
        public boolean isOutboundDone() {
            return !isOutboundNeverDone && delegate.isOutboundDone();
        }

        @Override
        public void setEnableSessionCreation(boolean flag) {
            delegate.setEnableSessionCreation(flag);
        }

        @Override
        public void setEnabledCipherSuites(String[] suites) {
            delegate.setEnabledCipherSuites(suites);
        }

        @Override
        public void setEnabledProtocols(String[] protocols) {
            delegate.setEnabledProtocols(protocols);
        }

        @Override
        public void setNeedClientAuth(boolean need) {
            delegate.setNeedClientAuth(need);
        }

        @Override
        public void setUseClientMode(boolean mode) {
            delegate.setUseClientMode(mode);
        }

        @Override
        public void setWantClientAuth(boolean want) {
            delegate.setWantClientAuth(want);
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
            return map(unwrapStep != null ? unwrapStep.apply(delegate, src, dst) : delegate.unwrap(src, dst));
        }

        @Override
        public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length) throws SSLException {
            return delegate.unwrap(src, dsts, offset, length);
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) throws SSLException {
            return map(wrapStep != null ? wrapStep.apply(delegate, src, dst) : delegate.wrap(src, dst));
        }

        @Override
        public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst) throws SSLException {
            return delegate.wrap(srcs, offset, length, dst);
        }

        /**
         * @return a context that hands out this engine: the socket asks for one engine per TLS session
         */
        SSLContext context() {
            final var engine = this;
            return new SSLContext(new SSLContextSpi() {
                @Override
                protected SSLEngine engineCreateSSLEngine() {
                    return engine;
                }

                @Override
                protected SSLEngine engineCreateSSLEngine(String host, int port) {
                    return engine;
                }

                @Override
                protected SSLSessionContext engineGetClientSessionContext() {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected SSLSessionContext engineGetServerSessionContext() {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected SSLServerSocketFactory engineGetServerSocketFactory() {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected SSLSocketFactory engineGetSocketFactory() {
                    throw new UnsupportedOperationException();
                }

                @Override
                protected void engineInit(KeyManager[] keyManagers, TrustManager[] trustManagers, SecureRandom random) {
                }
            }, null, "TLS") {
            };
        }

        private SSLEngineResult.HandshakeStatus map(SSLEngineResult.HandshakeStatus status) {
            return handshakeStatusMapper != null ? handshakeStatusMapper.apply(status) : status;
        }

        private SSLEngineResult map(SSLEngineResult result) {
            return handshakeStatusMapper != null
                    ? new SSLEngineResult(result.getStatus(), map(result.getHandshakeStatus()), result.bytesConsumed(), result.bytesProduced())
                    : result;
        }
    }
}
