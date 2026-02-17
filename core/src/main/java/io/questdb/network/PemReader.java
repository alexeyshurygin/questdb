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

package io.questdb.network;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Path.of;

public final class PemReader {
    private static final char[] KEY_PASSWORD = "questdb".toCharArray();
    private static final String PKCS1_CONVERSION_HINT = "PKCS1 key format not supported, convert with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem";

    private PemReader() {
    }

    public static SSLContext createServerSslContext(final String certPath, final String privateKeyPath) throws GeneralSecurityException, IOException {
        final var certificateChain = readCertificateChain(certPath);
        final var privateKey = readPrivateKey(privateKeyPath);

        final var keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setKeyEntry("questdb", privateKey, KEY_PASSWORD, certificateChain);

        final var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEY_PASSWORD);

        final var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, new SecureRandom());
        return sslContext;
    }

    private static Certificate[] readCertificateChain(final String certPath) throws GeneralSecurityException, IOException {
        final var certBytes = readAllBytes(of(certPath));
        final var certificateFactory = CertificateFactory.getInstance("X.509");
        final Collection<? extends Certificate> certificates = certificateFactory.generateCertificates(new ByteArrayInputStream(certBytes));
        if (certificates.isEmpty()) {
            throw new GeneralSecurityException("no certificates found in PEM file [path=" + certPath + "]");
        }
        return new ArrayList<>(certificates).toArray(new Certificate[0]);
    }

    private static PrivateKey readPrivateKey(final String privateKeyPath) throws GeneralSecurityException, IOException {
        final var keyBytes = readAllBytes(of(privateKeyPath));
        final var pem = new String(keyBytes, StandardCharsets.US_ASCII);

        if (pem.contains("-----BEGIN RSA PRIVATE KEY-----")
                || pem.contains("-----BEGIN EC PRIVATE KEY-----")
                || pem.contains("-----BEGIN DSA PRIVATE KEY-----")) {
            throw new GeneralSecurityException(PKCS1_CONVERSION_HINT + " [path=" + privateKeyPath + "]");
        }

        if (!pem.contains("-----BEGIN PRIVATE KEY-----")) {
            throw new GeneralSecurityException("unsupported private key format, expected PKCS8 PEM (-----BEGIN PRIVATE KEY-----) [path=" + privateKeyPath + "]");
        }

        final var normalizedPem = pem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s+", "");

        final byte[] pkcs8Bytes;
        try {
            pkcs8Bytes = Base64.getDecoder().decode(normalizedPem);
        } catch (IllegalArgumentException e) {
            throw new GeneralSecurityException("invalid base64 in private key PEM [path=" + privateKeyPath + ']', e);
        }

        final var keySpec = new PKCS8EncodedKeySpec(pkcs8Bytes);
        GeneralSecurityException lastError = null;
        for (final var algorithm : new String[]{"RSA", "EC"}) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(keySpec);
            } catch (GeneralSecurityException e) {
                lastError = e;
            }
        }
        throw new GeneralSecurityException("unsupported private key algorithm, expected RSA or EC [path=" + privateKeyPath + ']', lastError);
    }
}
