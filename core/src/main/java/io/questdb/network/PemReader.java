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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

public final class PemReader {

    private PemReader() {
    }

    public static SSLContext createServerSslContext(String certPath, String privateKeyPath)
            throws IOException, GeneralSecurityException {
        final var certChain = loadCertificates(certPath);
        final var privateKey = loadPrivateKey(privateKeyPath);
        final var keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("server", privateKey, new char[0], certChain);
        final var kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, new char[0]);
        final var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), null, new SecureRandom());
        return sslContext;
    }

    private static Certificate[] loadCertificates(String certPath) throws IOException, GeneralSecurityException {
        final var certBytes = Files.readAllBytes(Path.of(certPath));
        final var cf = CertificateFactory.getInstance("X.509");
        final var certs = cf.generateCertificates(new ByteArrayInputStream(certBytes));
        if (certs.isEmpty()) {
            throw new GeneralSecurityException("no certificates found in " + certPath);
        }
        return certs.toArray(new Certificate[0]);
    }

    private static PrivateKey loadPrivateKey(String keyPath) throws IOException, GeneralSecurityException {
        final var keyPem = new String(Files.readAllBytes(Path.of(keyPath)), StandardCharsets.US_ASCII);
        if (keyPem.contains("-----BEGIN RSA PRIVATE KEY-----") || keyPem.contains("-----BEGIN EC PRIVATE KEY-----")) {
            throw new GeneralSecurityException(
                    "PKCS1 key format not supported, convert with: openssl pkcs8 -topk8 -nocrypt -in key.pem -out key-pkcs8.pem"
            );
        }
        if (!keyPem.contains("-----BEGIN PRIVATE KEY-----")) {
            throw new GeneralSecurityException("expected PEM private key (PKCS8 format) in " + keyPath);
        }
        final var base64 = keyPem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        final var keyBytes = Base64.getDecoder().decode(base64);
        final var keySpec = new PKCS8EncodedKeySpec(keyBytes);
        try {
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        } catch (GeneralSecurityException e) {
            try {
                return KeyFactory.getInstance("EC").generatePrivate(keySpec);
            } catch (GeneralSecurityException e2) {
                throw new GeneralSecurityException(
                        "failed to parse private key as RSA or EC from " + keyPath, e2
                );
            }
        }
    }
}
