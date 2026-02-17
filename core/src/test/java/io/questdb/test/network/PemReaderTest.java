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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.security.GeneralSecurityException;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.writeString;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PemReaderTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCreateServerSslContextFromPkcs8Pem() throws Exception {
        final var certPath = copyResourceToTemp("tls/server.crt");
        final var keyPath = copyResourceToTemp("tls/server.key");

        final var sslContext = PemReader.createServerSslContext(certPath, keyPath);

        assertNotNull(sslContext);
        assertNotNull(sslContext.createSSLEngine());
    }

    @Test
    public void testInvalidBase64InPrivateKeyThrowsClearError() throws Exception {
        final var certPath = copyResourceToTemp("tls/server.crt");
        final var keyPath = temporaryFolder.newFile("invalid.key").toPath();
        writeString(
                keyPath,
                "-----BEGIN PRIVATE KEY-----\n@@@\n-----END PRIVATE KEY-----\n",
                US_ASCII
        );

        final var error = assertThrows(
                GeneralSecurityException.class,
                () -> PemReader.createServerSslContext(certPath, keyPath.toString())
        );

        assertTrue(error.getMessage().contains("invalid base64 in private key PEM"));
    }

    @Test
    public void testMissingPrivateKeyFileThrowsIOException() throws Exception {
        final var certPath = copyResourceToTemp("tls/server.crt");
        final var missingKeyPath = temporaryFolder.getRoot().toPath().resolve("missing.key").toString();

        assertThrows(IOException.class, () -> PemReader.createServerSslContext(certPath, missingKeyPath));
    }

    @Test
    public void testPkcs1PrivateKeyThrowsActionableError() throws Exception {
        final var certPath = copyResourceToTemp("tls/server.crt");
        final var pkcs1KeyPath = copyResourceToTemp("tls/pkcs1-rsa.key");

        final var error = assertThrows(
                GeneralSecurityException.class,
                () -> PemReader.createServerSslContext(certPath, pkcs1KeyPath)
        );

        assertTrue(error.getMessage().contains("PKCS1 key format not supported"));
        assertTrue(error.getMessage().contains("openssl pkcs8 -topk8 -nocrypt"));
    }

    private String copyResourceToTemp(final String resourcePath) throws IOException {
        final var targetPath = temporaryFolder.newFile(resourcePath.substring(resourcePath.lastIndexOf('/') + 1)).toPath();
        try (final var inputStream = PemReaderTest.class.getClassLoader().getResourceAsStream(resourcePath)) {
            assertNotNull("resource not found: " + resourcePath, inputStream);
            copy(inputStream, targetPath, REPLACE_EXISTING);
        }
        return targetPath.toString();
    }
}
