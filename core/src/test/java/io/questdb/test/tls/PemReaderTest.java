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

import io.questdb.tls.PemReader;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;

import static org.junit.Assert.*;

public class PemReaderTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static boolean opensslAvailable() {
        try {
            final var process = new ProcessBuilder("openssl", "version")
                    .redirectErrorStream(true)
                    .start();
            final var exitCode = process.waitFor();
            return exitCode == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private static void generateSelfSignedCert(File certFile, File keyFile) throws Exception {
        final var process = new ProcessBuilder(
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", keyFile.getAbsolutePath(),
                "-out", certFile.getAbsolutePath(),
                "-days", "1",
                "-nodes",
                "-subj", "/CN=localhost"
        ).redirectErrorStream(true).start();
        final var exitCode = process.waitFor();
        if (exitCode != 0) {
            final var output = new String(process.getInputStream().readAllBytes());
            fail("openssl failed: " + output);
        }
        // Convert to PKCS8 format
        final var pkcs8Key = new File(keyFile.getParent(), "server-pkcs8.key");
        final var convertProcess = new ProcessBuilder(
                "openssl", "pkcs8", "-topk8", "-nocrypt",
                "-in", keyFile.getAbsolutePath(),
                "-out", pkcs8Key.getAbsolutePath()
        ).redirectErrorStream(true).start();
        final var convertExitCode = convertProcess.waitFor();
        if (convertExitCode != 0) {
            final var output = new String(convertProcess.getInputStream().readAllBytes());
            fail("openssl pkcs8 conversion failed: " + output);
        }
        // Replace the original key with the PKCS8 version
        Files.copy(pkcs8Key.toPath(), keyFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    public void testInvalidPemContent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var certFile = temp.newFile("invalid.crt");
            final var keyFile = temp.newFile("invalid.key");
            Files.writeString(certFile.toPath(), "not a real certificate");
            Files.writeString(keyFile.toPath(), "not a real private key");
            try {
                PemReader.createServerSslContext(
                        certFile.getAbsolutePath(),
                        keyFile.getAbsolutePath()
                );
                fail("expected GeneralSecurityException");
            } catch (GeneralSecurityException e) {
                // expected
            }
        });
    }

    @Test
    public void testMissingFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                PemReader.createServerSslContext(
                        "/nonexistent/cert.pem",
                        "/nonexistent/key.pem"
                );
                fail("expected IOException");
            } catch (IOException e) {
                // expected
            }
        });
    }

    @Test
    public void testPkcs1KeyFormatError() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable());
        TestUtils.assertMemoryLeak(() -> {
            // Use a valid cert (openssl-generated) but a fake PKCS1-format key file
            final var certFile = temp.newFile("pkcs1-cert.crt");
            final var keyFile = temp.newFile("pkcs1-key.key");
            final var genProc = new ProcessBuilder(
                    "openssl", "req", "-x509", "-newkey", "rsa:2048",
                    "-keyout", "/dev/null", "-out", certFile.getAbsolutePath(),
                    "-days", "1", "-nodes", "-subj", "/CN=localhost"
            ).redirectErrorStream(true).start();
            assertEquals(0, genProc.waitFor());
            // Write a PKCS1 header to trigger the detection
            Files.writeString(keyFile.toPath(),
                    "-----BEGIN RSA PRIVATE KEY-----\nfakedata\n-----END RSA PRIVATE KEY-----\n");
            try {
                PemReader.createServerSslContext(
                        certFile.getAbsolutePath(),
                        keyFile.getAbsolutePath()
                );
                fail("expected GeneralSecurityException for PKCS1 key");
            } catch (GeneralSecurityException e) {
                assertTrue(e.getMessage().contains("PKCS1"));
                assertTrue(e.getMessage().contains("openssl pkcs8"));
            }
        });
    }

    @Test
    public void testValidRsaCert() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable());
        TestUtils.assertMemoryLeak(() -> {
            final var certFile = temp.newFile("valid-cert.crt");
            final var keyFile = temp.newFile("valid-key.key");
            generateSelfSignedCert(certFile, keyFile);
            final var sslContext = PemReader.createServerSslContext(
                    certFile.getAbsolutePath(),
                    keyFile.getAbsolutePath()
            );
            assertNotNull(sslContext);
            assertEquals("TLS", sslContext.getProtocol());
        });
    }
}
