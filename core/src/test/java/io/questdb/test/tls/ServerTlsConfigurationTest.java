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

import io.questdb.PropertyKey;
import io.questdb.ServerConfigurationException;
import io.questdb.tls.ServerTlsConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ServerTlsConfigurationTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static final ServerTlsConfiguration.GetStringFn getString = (props, env, key, defaultValue) -> {
        final var val = props.getProperty(key.getPropertyPath());
        return val != null ? val : defaultValue;
    };

    private static String certPath;
    private static String keyPath;

    @BeforeClass
    public static void setUp() throws IOException {
        final var certFile = temp.newFile("server.crt");
        final var keyFile = temp.newFile("server.key");
        certPath = certFile.getAbsolutePath();
        keyPath = keyFile.getAbsolutePath();
    }

    @Test
    public void testGlobalTlsEnabledPropagates() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("tls.enabled", "true");
            props.setProperty("tls.cert.path", certPath);
            props.setProperty("tls.private.key.path", keyPath);
            final var rootDir = temp.getRoot().getAbsolutePath();
            final var global = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                    ServerTlsConfiguration.DISABLED, getString
            );
            final var http = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.HTTP_TLS_ENABLED, PropertyKey.HTTP_TLS_CERT_PATH, PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            final var pg = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.PG_TLS_ENABLED, PropertyKey.PG_TLS_CERT_PATH, PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            final var lineTcp = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.LINE_TCP_TLS_ENABLED, PropertyKey.LINE_TCP_TLS_CERT_PATH, PropertyKey.LINE_TCP_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            assertTrue(global.isEnabled());
            assertTrue(http.isEnabled());
            assertTrue(pg.isEnabled());
            assertTrue(lineTcp.isEnabled());
            assertEquals(certPath, http.getCertPath());
            assertEquals(keyPath, http.getPrivateKeyPath());
            assertEquals(certPath, pg.getCertPath());
            assertEquals(keyPath, lineTcp.getPrivateKeyPath());
        });
    }

    @Test
    public void testMissingCertPathThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("tls.enabled", "true");
            props.setProperty("tls.private.key.path", keyPath);
            final var rootDir = temp.getRoot().getAbsolutePath();
            try {
                ServerTlsConfiguration.parse(
                        props, null, rootDir,
                        PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                        ServerTlsConfiguration.DISABLED, getString
                );
                fail("expected ServerConfigurationException");
            } catch (ServerConfigurationException e) {
                assertTrue(e.getMessage().contains("certificate path is not set"));
            }
        });
    }

    @Test
    public void testMissingKeyPathThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("tls.enabled", "true");
            props.setProperty("tls.cert.path", certPath);
            final var rootDir = temp.getRoot().getAbsolutePath();
            try {
                ServerTlsConfiguration.parse(
                        props, null, rootDir,
                        PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                        ServerTlsConfiguration.DISABLED, getString
                );
                fail("expected ServerConfigurationException");
            } catch (ServerConfigurationException e) {
                assertTrue(e.getMessage().contains("private key path is not set"));
            }
        });
    }

    @Test
    public void testNonexistentCertFileThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("tls.enabled", "true");
            props.setProperty("tls.cert.path", "/nonexistent/cert.pem");
            props.setProperty("tls.private.key.path", keyPath);
            final var rootDir = temp.getRoot().getAbsolutePath();
            try {
                ServerTlsConfiguration.parse(
                        props, null, rootDir,
                        PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                        ServerTlsConfiguration.DISABLED, getString
                );
                fail("expected ServerConfigurationException");
            } catch (ServerConfigurationException e) {
                assertTrue(e.getMessage().contains("does not exist"));
            }
        });
    }

    @Test
    public void testPerProtocolDisableOverridesGlobal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("tls.enabled", "true");
            props.setProperty("tls.cert.path", certPath);
            props.setProperty("tls.private.key.path", keyPath);
            props.setProperty("pg.tls.enabled", "false");
            final var rootDir = temp.getRoot().getAbsolutePath();
            final var global = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                    ServerTlsConfiguration.DISABLED, getString
            );
            final var pg = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.PG_TLS_ENABLED, PropertyKey.PG_TLS_CERT_PATH, PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            final var http = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.HTTP_TLS_ENABLED, PropertyKey.HTTP_TLS_CERT_PATH, PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            assertFalse(pg.isEnabled());
            assertTrue(http.isEnabled());
        });
    }

    @Test
    public void testPerProtocolEnableWithoutGlobal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("http.tls.enabled", "true");
            props.setProperty("http.tls.cert.path", certPath);
            props.setProperty("http.tls.private.key.path", keyPath);
            final var rootDir = temp.getRoot().getAbsolutePath();
            final var global = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                    ServerTlsConfiguration.DISABLED, getString
            );
            final var http = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.HTTP_TLS_ENABLED, PropertyKey.HTTP_TLS_CERT_PATH, PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            final var pg = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.PG_TLS_ENABLED, PropertyKey.PG_TLS_CERT_PATH, PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
                    ServerTlsConfiguration.DISABLED, getString
            );
            assertFalse(global.isEnabled());
            assertTrue(http.isEnabled());
            assertFalse(pg.isEnabled());
        });
    }

    @Test
    public void testTlsDisabledByDefault() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            final var rootDir = temp.getRoot().getAbsolutePath();
            final var global = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                    ServerTlsConfiguration.DISABLED, getString
            );
            final var http = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.HTTP_TLS_ENABLED, PropertyKey.HTTP_TLS_CERT_PATH, PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            final var pg = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.PG_TLS_ENABLED, PropertyKey.PG_TLS_CERT_PATH, PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            final var lineTcp = ServerTlsConfiguration.parse(
                    props, null, rootDir,
                    PropertyKey.LINE_TCP_TLS_ENABLED, PropertyKey.LINE_TCP_TLS_CERT_PATH, PropertyKey.LINE_TCP_TLS_PRIVATE_KEY_PATH,
                    global, getString
            );
            assertFalse(global.isEnabled());
            assertFalse(http.isEnabled());
            assertFalse(pg.isEnabled());
            assertFalse(lineTcp.isEnabled());
        });
    }
}
