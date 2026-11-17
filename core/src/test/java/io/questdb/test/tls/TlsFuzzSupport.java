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

import io.questdb.std.Rnd;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.Key;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Enumeration;

/**
 * Fixtures the TLS fuzz tests share: the server certificate, a client context that trusts it, the iteration
 * budget and the bytes a hostile client sends where a ClientHello belongs.
 * <p>
 * The certificate comes from the keystore the TLS proxy of the client-side tests uses, so the fuzz tests need no
 * openssl process and run wherever the fuzz jobs run.
 */
final class TlsFuzzSupport {
    static final int ALERT_DESCRIPTION_HANDSHAKE_FAILURE = 40;
    static final int ALERT_LEVEL_FATAL = 2;
    static final int ALERT_RECORD_LENGTH = 7;
    static final String ITERATIONS_PROPERTY = "questdb.fuzz.tls.iterations";
    // PGWire's SSLRequest: length 8, code 80877103
    static final byte[] PG_SSL_REQUEST = {0, 0, 0, 8, 0x04, (byte) 0xd2, 0x16, 0x2f};
    static final int RECORD_HEADER_LENGTH = 5;
    static final int RECORD_TYPE_ALERT = 0x15;
    private static final String KEYSTORE = "/keystore/server.keystore";
    private static final char[] KEYSTORE_PASSWORD = "questdb".toCharArray();

    private TlsFuzzSupport() {
    }

    /**
     * Builds a ClientHello record with only a server_name extension. SunJSSE rejects a name that is not a valid
     * host name.
     */
    static byte[] clientHelloWithServerName(byte[] serverName) {
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

    /**
     * Fills {@code bytes} with every byte value, unlike {@link Rnd#nextBytes(byte[])}, which draws letters only.
     */
    static void fill(Rnd rnd, byte[] bytes) {
        var i = 0;
        for (final var n = bytes.length - 7; i < n; i += 8) {
            final var value = rnd.nextLong();
            for (var k = 0; k < 8; k++) {
                bytes[i + k] = (byte) (value >>> (8 * k));
            }
        }
        for (var value = rnd.nextLong(); i < bytes.length; i++, value >>>= 8) {
            bytes[i] = (byte) value;
        }
    }

    /**
     * What a client that does not speak TLS, or speaks it badly, sends first: random bytes, the opening of
     * another protocol, or a ClientHello the server must reject.
     */
    static byte[] hostileFirstFlight(Rnd rnd) {
        switch (rnd.nextInt(9)) {
            case 0: {
                // random bytes of any length up to 64 KiB
                final var bytes = new byte[randomLength(rnd)];
                fill(rnd, bytes);
                return bytes;
            }
            case 1: {
                // random bytes that start like a TLS record or an SSLv2 hello
                final var bytes = new byte[randomLength(rnd)];
                fill(rnd, bytes);
                bytes[0] = rnd.nextInt(5) == 0 ? (byte) 0x80 : (byte) (0x14 + rnd.nextInt(4));
                if (bytes.length > 2 && rnd.nextBoolean()) {
                    bytes[1] = 0x03;
                    bytes[2] = (byte) rnd.nextInt(5);
                }
                return bytes;
            }
            case 2:
                return "GET /exec?query=SELECT%201 HTTP/1.1\r\nHost: localhost\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
            case 3:
                return PG_SSL_REQUEST.clone();
            case 4: {
                // PGWire StartupMessage, protocol 3.0
                final var parameters = "user\0admin\0database\0qdb\0\0".getBytes(StandardCharsets.US_ASCII);
                final var length = 8 + parameters.length;
                final var message = new byte[length];
                message[2] = (byte) (length >> 8);
                message[3] = (byte) length;
                message[5] = 0x03;
                System.arraycopy(parameters, 0, message, 8, parameters.length);
                return message;
            }
            case 5:
                return "cpu,host=a usage=1i 1000000000\n".getBytes(StandardCharsets.US_ASCII);
            case 6:
                return ("GET /write/v4 HTTP/1.1\r\n"
                        + "Host: localhost\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Sec-WebSocket-Version: 13\r\n"
                        + "Sec-WebSocket-Key: x3JJHMbDL1EzLkh9GBhXDw==\r\n"
                        + "\r\n").getBytes(StandardCharsets.US_ASCII);
            case 7:
                // a server name with CR and LF in it
                return clientHelloWithServerName("a\r\nFAKE.".getBytes(StandardCharsets.US_ASCII));
            default:
                // a single byte: the start of a record header and nothing else
                return new byte[]{(byte) (0x14 + rnd.nextInt(4))};
        }
    }

    static boolean isPrintableAscii(CharSequence text) {
        for (int i = 0, n = text.length(); i < n; i++) {
            final var c = text.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return the iteration budget: {@code defaultIterations} in CI, or what {@value #ITERATIONS_PROPERTY} says
     */
    static int iterations(int defaultIterations) {
        return Math.max(1, Integer.getInteger(ITERATIONS_PROPERTY, defaultIterations));
    }

    static SSLContext newServerContext() throws Exception {
        final var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(loadKeyStore(), KEYSTORE_PASSWORD);
        final var context = SSLContext.getInstance("TLS");
        context.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
        return context;
    }

    static SSLContext newTrustAllContext() throws Exception {
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
     * @return the server certificate in DER form, as the server sends it in a full handshake
     */
    static byte[] serverCertificateDer() throws Exception {
        final var keyStore = loadKeyStore();
        return keyStore.getCertificate(keyAlias(keyStore)).getEncoded();
    }

    /**
     * Writes the certificate chain and the PKCS8 private key of the test keystore as PEM files, the form the
     * server configuration takes.
     */
    static void writePemFiles(File certFile, File keyFile) throws Exception {
        final var keyStore = loadKeyStore();
        final var alias = keyAlias(keyStore);
        final var encoder = Base64.getMimeEncoder(64, new byte[]{'\n'});
        final var certPem = new StringBuilder();
        for (Certificate certificate : keyStore.getCertificateChain(alias)) {
            certPem.append("-----BEGIN CERTIFICATE-----\n")
                    .append(encoder.encodeToString(certificate.getEncoded()))
                    .append("\n-----END CERTIFICATE-----\n");
        }
        Files.writeString(certFile.toPath(), certPem, StandardCharsets.US_ASCII);
        final Key key = keyStore.getKey(alias, KEYSTORE_PASSWORD);
        if (!"PKCS#8".equals(key.getFormat())) {
            throw new AssertionError("unexpected private key encoding: " + key.getFormat());
        }
        final var keyPem = "-----BEGIN PRIVATE KEY-----\n"
                + encoder.encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        Files.writeString(keyFile.toPath(), keyPem, StandardCharsets.US_ASCII);
    }

    private static String keyAlias(KeyStore keyStore) throws Exception {
        final Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            final var alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return alias;
            }
        }
        throw new AssertionError("no private key in " + KEYSTORE);
    }

    private static KeyStore loadKeyStore() throws Exception {
        final var keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = TlsFuzzSupport.class.getResourceAsStream(KEYSTORE)) {
            if (in == null) {
                throw new AssertionError("test resource is missing: " + KEYSTORE);
            }
            keyStore.load(in, KEYSTORE_PASSWORD);
        }
        return keyStore;
    }

    private static int randomLength(Rnd rnd) {
        return switch (rnd.nextInt(4)) {
            case 0 -> 1 + rnd.nextInt(8);
            case 1 -> 1 + rnd.nextInt(600);
            case 2 -> 1 + rnd.nextInt(20_000);
            default -> 1 + rnd.nextInt(64 * 1024);
        };
    }
}
