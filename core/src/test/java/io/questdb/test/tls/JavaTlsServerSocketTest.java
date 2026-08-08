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
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.JavaTlsServerSocket;
import io.questdb.tls.JavaTlsServerSocketFactory;
import io.questdb.tls.PemReader;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class JavaTlsServerSocketTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final Log LOG = LogFactory.getLog(JavaTlsServerSocketTest.class);
    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(29_870);
    private static String certPath;
    private static String keyPath;
    private static boolean opensslAvailable;

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
    public void testCloseNotifyDoesNotSpinOnEagain() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var port = PORT_COUNTER.getAndIncrement();
            final var serverFd = Net.socketTcp(true);
            assertTrue(serverFd > 0);
            try {
                assertTrue(Net.bindTcp(serverFd, 0, port));
                Net.listen(serverFd, 1);
                final var clientFd = Net.socketTcp(true);
                assertTrue(clientFd > 0);
                try {
                    final var addr = Net.sockaddr("127.0.0.1", port);
                    assertEquals(0, Net.connect(clientFd, addr));
                    Net.freeSockAddr(addr);
                    final var acceptedFd = Net.accept(serverFd);
                    assertTrue(acceptedFd > 0);
                    final var tlsSocket = new JavaTlsServerSocket(
                            NetworkFacadeImpl.INSTANCE, LOG, sslContext
                    );
                    tlsSocket.of(acceptedFd);
                    // Close immediately without TLS handshake.
                    // close_notify wrap produces data but the client hasn't started TLS
                    // so send may return 0. close() must not spin.
                    final var start = System.currentTimeMillis();
                    tlsSocket.close();
                    final var elapsed = System.currentTimeMillis() - start;
                    assertTrue("close() took too long: " + elapsed + "ms", elapsed < 5000);
                } finally {
                    Net.close(clientFd);
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
            final var port = PORT_COUNTER.getAndIncrement();
            final var serverFd = Net.socketTcp(true);
            assertTrue(serverFd > 0);
            try {
                assertTrue(Net.bindTcp(serverFd, 0, port));
                Net.listen(serverFd, 1);
                final var clientFd = Net.socketTcp(true);
                assertTrue(clientFd > 0);
                final var addr = Net.sockaddr("127.0.0.1", port);
                    assertEquals(0, Net.connect(clientFd, addr));
                    Net.freeSockAddr(addr);
                // Close client immediately to make handshake fail
                Net.close(clientFd);
                final var acceptedFd = Net.accept(serverFd);
                assertTrue(acceptedFd > 0);
                final var tlsSocket = new JavaTlsServerSocket(
                        NetworkFacadeImpl.INSTANCE, LOG, sslContext
                );
                tlsSocket.of(acceptedFd);
                assertFalse("should not be started before handshake", tlsSocket.isTlsSessionStarted());
                try {
                    tlsSocket.startTlsSession(null);
                    fail("expected TlsSessionInitFailedException");
                } catch (TlsSessionInitFailedException e) {
                    // expected — client disconnected
                }
                assertFalse(
                        "sslEngine must be null after failed handshake to prevent poisoned socket reuse",
                        tlsSocket.isTlsSessionStarted()
                );
                tlsSocket.close();
            } finally {
                Net.close(serverFd);
            }
        });
    }

    @Test
    public void testHandshakeTimesOutOnNoClientData() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var port = PORT_COUNTER.getAndIncrement();
            final var serverFd = Net.socketTcp(true);
            assertTrue(serverFd > 0);
            try {
                assertTrue(Net.bindTcp(serverFd, 0, port));
                Net.listen(serverFd, 1);
                // Connect a client that never sends TLS ClientHello
                final var clientFd = Net.socketTcp(true);
                assertTrue(clientFd > 0);
                try {
                    Net.configureNonBlocking(clientFd);
                    final var addr = Net.sockaddr("127.0.0.1", port);
                    Net.connect(clientFd, addr);
                    Net.freeSockAddr(addr);
                    final var acceptedFd = Net.accept(serverFd);
                    assertTrue(acceptedFd > 0);
                    Net.configureNonBlocking(acceptedFd);
                    final var tlsSocket = new JavaTlsServerSocket(
                            NetworkFacadeImpl.INSTANCE, LOG, sslContext
                    );
                    tlsSocket.of(acceptedFd);
                    try {
                        tlsSocket.startTlsSession(null);
                        fail("expected TlsSessionInitFailedException");
                    } catch (TlsSessionInitFailedException e) {
                        assertTrue(
                                "expected 'not making progress' message, got: " + e.getMessage(),
                                e.getMessage().contains("not making progress")
                                        || e.getMessage().contains("socket read error")
                        );
                    }
                    assertFalse("sslEngine must be cleared after failure", tlsSocket.isTlsSessionStarted());
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
    public void testHandshakeTimesOutOnPartialClientHello() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var port = PORT_COUNTER.getAndIncrement();
            final var serverFd = Net.socketTcp(true);
            assertTrue(serverFd > 0);
            try {
                assertTrue(Net.bindTcp(serverFd, 0, port));
                Net.listen(serverFd, 1);
                final var clientFd = Net.socketTcp(true);
                assertTrue(clientFd > 0);
                try {
                    final var addr = Net.sockaddr("127.0.0.1", port);
                    Net.connect(clientFd, addr);
                    Net.freeSockAddr(addr);
                    final var acceptedFd = Net.accept(serverFd);
                    assertTrue(acceptedFd > 0);
                    Net.configureNonBlocking(acceptedFd);
                    // Send a partial TLS ClientHello — just the record header (5 bytes)
                    // and a few bytes of the handshake, then stop.
                    // TLS record: ContentType=22(handshake), Version=0x0301, Length=0x00FF
                    final var partial = new byte[]{0x16, 0x03, 0x01, 0x00, (byte) 0xFF, 0x01, 0x00};
                    final var sendBuf = io.questdb.std.Unsafe.malloc(partial.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (var i = 0; i < partial.length; i++) {
                            io.questdb.std.Unsafe.getUnsafe().putByte(sendBuf + i, partial[i]);
                        }
                        Net.send(clientFd, sendBuf, partial.length);
                    } finally {
                        io.questdb.std.Unsafe.free(sendBuf, partial.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    }
                    Thread.sleep(50); // let partial data arrive
                    final var tlsSocket = new JavaTlsServerSocket(
                            NetworkFacadeImpl.INSTANCE, LOG, sslContext
                    );
                    tlsSocket.of(acceptedFd);
                    // The server reads the partial data into the buffer (limit > 0),
                    // then readFromSocket returns 0 on subsequent reads.
                    // Without the fix, this would spin forever because limit > 0
                    // bypassed the zero-progress check.
                    try {
                        tlsSocket.startTlsSession(null);
                        fail("expected TlsSessionInitFailedException");
                    } catch (TlsSessionInitFailedException e) {
                        assertTrue(
                                "expected 'not making progress' message, got: " + e.getMessage(),
                                e.getMessage().contains("not making progress")
                        );
                    }
                    assertFalse("sslEngine must be cleared after failure", tlsSocket.isTlsSessionStarted());
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
    public void testPlaintextRecvBeforeHandshake() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var sslContext = PemReader.createServerSslContext(certPath, keyPath);
            final var port = PORT_COUNTER.getAndIncrement();
            final var serverFd = Net.socketTcp(true);
            assertTrue(serverFd > 0);
            try {
                assertTrue(Net.bindTcp(serverFd, 0, port));
                Net.listen(serverFd, 1);
                final var clientFd = Net.socketTcp(true);
                assertTrue(clientFd > 0);
                try {
                    final var addr = Net.sockaddr("127.0.0.1", port);
                    assertEquals(0, Net.connect(clientFd, addr));
                    Net.freeSockAddr(addr);
                    final var acceptedFd = Net.accept(serverFd);
                    assertTrue(acceptedFd > 0);
                    final var tlsSocket = new JavaTlsServerSocket(
                            NetworkFacadeImpl.INSTANCE, LOG, sslContext
                    );
                    tlsSocket.of(acceptedFd);
                    assertTrue("supportsTls should be true", tlsSocket.supportsTls());
                    assertFalse("session not started yet", tlsSocket.isTlsSessionStarted());
                    // Send plaintext from client before TLS handshake
                    // (this is what PGWire does with SSLRequest)
                    final var msg = new byte[]{0x00, 0x00, 0x00, 0x08, 0x04, (byte) 0xd2, 0x16, 0x2f};
                    final var sendBuf = io.questdb.std.Unsafe.malloc(msg.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    try {
                        for (var i = 0; i < msg.length; i++) {
                            io.questdb.std.Unsafe.getUnsafe().putByte(sendBuf + i, msg[i]);
                        }
                        final var sent = Net.send(clientFd, sendBuf, msg.length);
                        assertEquals(msg.length, sent);
                    } finally {
                        io.questdb.std.Unsafe.free(sendBuf, msg.length, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    }
                    // Server should be able to recv plaintext before handshake
                    Thread.sleep(50); // let data arrive
                    final var recvBuf = io.questdb.std.Unsafe.malloc(256, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
                    try {
                        final var received = tlsSocket.recv(recvBuf, 256);
                        assertEquals("should receive plaintext bytes before handshake", msg.length, received);
                    } finally {
                        io.questdb.std.Unsafe.free(recvBuf, 256, io.questdb.std.MemoryTag.NATIVE_DEFAULT);
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

    private static boolean isOpensslAvailable() {
        try {
            final var proc = new ProcessBuilder("openssl", "version")
                    .redirectErrorStream(true).start();
            return proc.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
