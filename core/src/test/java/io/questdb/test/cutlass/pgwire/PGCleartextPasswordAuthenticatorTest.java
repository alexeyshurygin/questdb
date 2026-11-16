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

package io.questdb.test.cutlass.pgwire;

import io.questdb.BuildInformationHolder;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cutlass.auth.SocketAuthenticator;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGCleartextPasswordAuthenticator;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.PlainSocket;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * Drives the PGWire login over a socket that takes only part of what the authenticator writes, the way a slow
 * client's window or a TLS socket with a record in flight does. The login response must reach the client once,
 * however many calls it takes to write it.
 */
public class PGCleartextPasswordAuthenticatorTest extends AbstractCairoTest {
    private static final int BUFFER_SIZE = 4096;
    // The messages a client reads from the start of the login to ReadyForQuery, in the PostgreSQL protocol's terms:
    // a request for the password, then AuthenticationOk, the parameters, the cancel key and ReadyForQuery.
    private static final String EXPECTED_LOGIN = """
            AuthenticationCleartextPassword
            AuthenticationOk
            ParameterStatus TimeZone=GMT
            ParameterStatus application_name=QuestDB
            ParameterStatus server_version=11.3
            ParameterStatus integer_datetimes=on
            ParameterStatus client_encoding=UTF8
            ParameterStatus questdb_version=9.9.9
            BackendKeyData
            ReadyForQuery I
            """;
    // A made-up fd: only the MemoryTransport below ever sees it.
    private static final long FD = 1_000_000;
    private static final Log LOG = LogFactory.getLog(PGCleartextPasswordAuthenticatorTest.class);
    private static final String PASSWORD = "quest";
    private static final String USER = "admin";

    @Test
    public void testLoginOkAfterShortWriteSendsResponseOnce() throws Exception {
        // The socket takes the first bytes of the login response and nothing more. The PG context then calls
        // handleIO() when the socket is writable and, once that returns OK, loginOK() a second time, which must
        // not build the response again: the client would read two of them.
        assertMemoryLeak(() -> {
            final var transport = new MemoryTransport();
            assertLogin(transport, authenticator -> {
                transport.sendBudget = 7;
                assertEquals(SocketAuthenticator.NEEDS_WRITE, authenticator.loginOK());
                assertFalse(authenticator.isAuthenticated());
                transport.sendBudget = Long.MAX_VALUE;
                assertEquals(SocketAuthenticator.OK, authenticator.handleIO());
                assertTrue(authenticator.isAuthenticated());
                assertEquals(SocketAuthenticator.OK, authenticator.loginOK());
                assertTrue(authenticator.isAuthenticated());
            });
            TestUtils.assertEquals(EXPECTED_LOGIN, describe(transport.sent.toByteArray()));
        });
    }

    @Test
    public void testLoginOkCalledWhileResponseIsPendingContinuesWrite() throws Exception {
        // Every write takes a few bytes, and loginOK() is called until the response is out: each call continues
        // the write of the one response.
        assertMemoryLeak(() -> {
            final var transport = new MemoryTransport();
            assertLogin(transport, authenticator -> {
                transport.maxBytesPerSend = 7;
                var calls = 1;
                int result;
                while ((result = authenticator.loginOK()) == SocketAuthenticator.NEEDS_WRITE) {
                    assertFalse(authenticator.isAuthenticated());
                    assertTrue("the login response never completed", ++calls < 1_000);
                }
                assertEquals(SocketAuthenticator.OK, result);
                assertTrue("the response must have taken several writes, calls: " + calls, calls > 2);
                assertTrue(authenticator.isAuthenticated());
                assertEquals("a call after the login only reports it", SocketAuthenticator.OK, authenticator.loginOK());
            });
            TestUtils.assertEquals(EXPECTED_LOGIN, describe(transport.sent.toByteArray()));
        });
    }

    /**
     * Logs in over {@code transport} up to the point where the password has been verified, then runs
     * {@code scenario}, which writes the login response.
     */
    private static void assertLogin(MemoryTransport transport, LoginScenario scenario) throws Exception {
        transport.toServer.writeBytes(startupMessage());
        transport.toServer.writeBytes(passwordMessage());
        final var pgConfiguration = new DefaultPGConfiguration();
        final var socket = new PlainSocket(transport, LOG);
        final var recvBuf = Unsafe.malloc(BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        final var sendBuf = Unsafe.malloc(BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try (var registry = new DefaultPGCircuitBreakerRegistry(pgConfiguration, configuration)) {
            final var authenticator = new PGCleartextPasswordAuthenticator(
                    pgConfiguration,
                    new BuildInformationHolder("9.9.9", "commit", "jdk", "QuestDB"),
                    new NetworkSqlExecutionCircuitBreaker(engine, new DefaultSqlExecutionCircuitBreakerConfiguration()),
                    registry,
                    timeout -> {
                    },
                    PGCleartextPasswordAuthenticatorTest::verifyPassword,
                    false
            );
            try {
                socket.of(FD);
                authenticator.init(socket, recvBuf, recvBuf + BUFFER_SIZE, sendBuf, sendBuf + BUFFER_SIZE);
                assertEquals("the password is verified", SocketAuthenticator.OK, authenticator.handleIO());
                assertEquals(SecurityContext.AUTH_TYPE_CREDENTIALS, authenticator.getAuthType());
                TestUtils.assertEquals(USER, authenticator.getPrincipal());
                scenario.run(authenticator);
            } finally {
                authenticator.close();
                socket.close();
            }
        } finally {
            Unsafe.free(recvBuf, BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(sendBuf, BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Lists the backend messages in {@code wire}, which must be a run of whole messages: a type byte, then a
     * big-endian length that counts itself and the body.
     */
    private static String describe(byte[] wire) {
        final var sink = new StringBuilder();
        var at = 0;
        while (at < wire.length) {
            assertTrue("a message cut short", at + 5 <= wire.length);
            final var type = (char) wire[at];
            final var bodyLo = at + 5;
            final var bodyHi = at + 1 + readInt(wire, at + 1);
            assertTrue("a message cut short", bodyLo <= bodyHi && bodyHi <= wire.length);
            switch (type) {
                case 'R' -> {
                    assertEquals("an authentication request carries a code", 4, bodyHi - bodyLo);
                    final var code = readInt(wire, bodyLo);
                    sink.append(code == 0 ? "AuthenticationOk" : code == 3 ? "AuthenticationCleartextPassword" : "Authentication " + code);
                }
                case 'S' -> {
                    final var nameHi = indexOfZero(wire, bodyLo, bodyHi);
                    final var valueHi = indexOfZero(wire, nameHi + 1, bodyHi);
                    assertEquals("ParameterStatus holds a name and a value", bodyHi - 1, valueHi);
                    sink.append("ParameterStatus ")
                            .append(new String(wire, bodyLo, nameHi - bodyLo, StandardCharsets.UTF_8))
                            .append('=')
                            .append(new String(wire, nameHi + 1, valueHi - nameHi - 1, StandardCharsets.UTF_8));
                }
                case 'K' -> {
                    assertEquals("BackendKeyData holds the process id and the secret", 8, bodyHi - bodyLo);
                    sink.append("BackendKeyData");
                }
                case 'Z' -> {
                    assertEquals("ReadyForQuery holds the transaction status", 1, bodyHi - bodyLo);
                    sink.append("ReadyForQuery ").append((char) wire[bodyLo]);
                }
                default -> sink.append(type).append(' ').append(bodyHi - bodyLo).append(" bytes");
            }
            sink.append('\n');
            at = bodyHi;
        }
        return sink.toString();
    }

    private static int indexOfZero(byte[] bytes, int lo, int hi) {
        for (var i = lo; i < hi; i++) {
            if (bytes[i] == 0) {
                return i;
            }
        }
        throw new AssertionError("a string without its terminating zero");
    }

    // PasswordMessage: 'p', the length, the password and its terminating zero
    private static byte[] passwordMessage() {
        final var password = (PASSWORD + '\0').getBytes(StandardCharsets.UTF_8);
        final var message = new ByteArrayOutputStream();
        message.write('p');
        writeInt(message, 4 + password.length);
        message.writeBytes(password);
        return message.toByteArray();
    }

    private static int readInt(byte[] bytes, int at) {
        return ((bytes[at] & 0xff) << 24) | ((bytes[at + 1] & 0xff) << 16) | ((bytes[at + 2] & 0xff) << 8) | (bytes[at + 3] & 0xff);
    }

    // StartupMessage: the length, protocol 3.0 and the parameters, each a name and a value, ending with a zero
    private static byte[] startupMessage() {
        final var parameters = ("user\0" + USER + "\0database\0qdb\0\0").getBytes(StandardCharsets.UTF_8);
        final var message = new ByteArrayOutputStream();
        writeInt(message, 8 + parameters.length);
        writeInt(message, 196_608);
        message.writeBytes(parameters);
        return message.toByteArray();
    }

    private static byte verifyPassword(CharSequence username, long passwordPtr, int passwordLen) {
        final var expected = PASSWORD.getBytes(StandardCharsets.UTF_8);
        if (!USER.contentEquals(username) || passwordLen != expected.length) {
            return SecurityContext.AUTH_TYPE_NONE;
        }
        for (var i = 0; i < passwordLen; i++) {
            if (Unsafe.getUnsafe().getByte(passwordPtr + i) != expected[i]) {
                return SecurityContext.AUTH_TYPE_NONE;
            }
        }
        return SecurityContext.AUTH_TYPE_CREDENTIALS;
    }

    private static void writeInt(ByteArrayOutputStream out, int value) {
        out.write(value >>> 24);
        out.write(value >>> 16);
        out.write(value >>> 8);
        out.write(value);
    }

    @FunctionalInterface
    private interface LoginScenario {
        void run(PGCleartextPasswordAuthenticator authenticator) throws Exception;
    }

    /**
     * A transport that lives in the test: the plain socket reaches the network only through these methods, keyed
     * by a made-up fd, so the test decides how many bytes each write takes.
     */
    private static final class MemoryTransport extends NetworkFacadeImpl {
        final ByteArrayOutputStream sent = new ByteArrayOutputStream();
        final ByteArrayOutputStream toServer = new ByteArrayOutputStream();
        // the most bytes one write takes
        int maxBytesPerSend = Integer.MAX_VALUE;
        // how many bytes the writes take in all before they take nothing
        long sendBudget = Long.MAX_VALUE;
        private int readPos;

        @Override
        public int close(long fd) {
            return 0;
        }

        @Override
        public void close(long fd, Log logger) {
        }

        @Override
        public int recvRaw(long fd, long buffer, int bufferLen) {
            final var bytes = toServer.toByteArray();
            final var n = Math.min(bufferLen, bytes.length - readPos);
            for (var i = 0; i < n; i++) {
                Unsafe.getUnsafe().putByte(buffer + i, bytes[readPos + i]);
            }
            readPos += n;
            return n;
        }

        @Override
        public int sendRaw(long fd, long buffer, int bufferLen) {
            final var n = (int) Math.min(Math.min(bufferLen, maxBytesPerSend), sendBudget);
            sendBudget -= n;
            for (var i = 0; i < n; i++) {
                sent.write(Unsafe.getUnsafe().getByte(buffer + i));
            }
            return n;
        }
    }
}
