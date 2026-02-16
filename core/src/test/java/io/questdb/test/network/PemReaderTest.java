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

package io.questdb.test.network;

import io.questdb.network.PemReader;
import org.junit.Assume;
import org.junit.BeforeClass;
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
            final var exitCode = proc.waitFor();
            if (exitCode == 0) {
                certPath = certFile.getAbsolutePath();
                keyPath = keyFile.getAbsolutePath();
            } else {
                opensslAvailable = false;
            }
        }
    }

    @Test
    public void testInvalidPemContent() throws Exception {
        final var certFile = temp.newFile("invalid.crt");
        Files.writeString(certFile.toPath(), "not a PEM file");
        final var keyFile = temp.newFile("invalid.key");
        Files.writeString(keyFile.toPath(), "not a PEM file");
        try {
            PemReader.createServerSslContext(certFile.getAbsolutePath(), keyFile.getAbsolutePath());
            fail("expected exception");
        } catch (GeneralSecurityException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test(expected = IOException.class)
    public void testMissingFile() throws Exception {
        PemReader.createServerSslContext("/nonexistent/cert.pem", "/nonexistent/key.pem");
    }

    @Test
    public void testPkcs1KeyFormatError() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        final var keyFile = temp.newFile("pkcs1.key");
        Files.writeString(keyFile.toPath(),
                "-----BEGIN RSA PRIVATE KEY-----\nfakedata\n-----END RSA PRIVATE KEY-----\n");
        try {
            PemReader.createServerSslContext(certPath, keyFile.getAbsolutePath());
            fail("expected exception");
        } catch (GeneralSecurityException e) {
            assertTrue(e.getMessage().contains("PKCS1"));
            assertTrue(e.getMessage().contains("openssl pkcs8"));
        }
    }

    @Test
    public void testValidRsaCert() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        final var ctx = PemReader.createServerSslContext(certPath, keyPath);
        assertNotNull(ctx);
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
