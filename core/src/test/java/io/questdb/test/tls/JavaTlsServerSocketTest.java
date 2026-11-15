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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLSocket;
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

import static org.junit.Assert.*;

public class JavaTlsServerSocketTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final String BUFFER_SIZE_PROPERTY = "questdb.experimental.tls.buffersize";
    private static final Log LOG = LogFactory.getLog(JavaTlsServerSocketTest.class);
    private static final long TIMEOUT_NANOS = 20_000_000_000L;
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
    public void testCorruptRecordFailsRecv() throws Exception {
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, null, (client, server, buf) -> {
            final var record = client.encrypt("tampered".getBytes(StandardCharsets.UTF_8));
            record[record.length - 1] ^= 1; // breaks the record's authentication tag
            client.writeRaw(record);
            assertEquals(-1, recvUntilNonZero(server, buf, 4096));
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
    public void testSendAfterPeerCloseNotifyFails() throws Exception {
        // TLS 1.2 closes the outbound side when the peer's close_notify arrives
        assertWithHandshake(NetworkFacadeImpl.INSTANCE, "TLSv1.2", (client, server, buf) -> {
            client.writeRaw(client.closeNotify());
            assertEquals(-1, recvUntilNonZero(server, buf, 4096));
            assertEquals(-1, server.send(buf, 16));
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
    private interface HandshakeScenario {
        void run(EngineClient client, JavaTlsServerSocket server, long buf) throws Exception;
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
}
