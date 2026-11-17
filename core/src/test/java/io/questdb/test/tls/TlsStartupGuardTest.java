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

import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfigurationException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.TlsBootstrapConfiguration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class TlsStartupGuardTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static final Log LOG = LogFactory.getLog(TlsStartupGuardTest.class);

    private static String certPath;
    private static String keyPath;
    private static String root;

    @BeforeClass
    public static void setUp() throws IOException {
        final var rootDir = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(rootDir.getAbsolutePath());
        root = rootDir.getAbsolutePath();
        final var certFile = temp.newFile("guard-cert.crt");
        final var keyFile = temp.newFile("guard-key.key");
        certPath = certFile.getAbsolutePath();
        keyPath = keyFile.getAbsolutePath();
    }

    @Test
    public void testEachListenerAloneTriggersGuard() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (String prefix : new String[]{"http.min.", "line.tcp.", "pg.", "http."}) {
                final var props = new Properties();
                props.setProperty(prefix + "tls.enabled", "true");
                props.setProperty(prefix + "tls.cert.path", certPath);
                props.setProperty(prefix + "tls.private.key.path", keyPath);
                try {
                    new PropServerConfiguration(root, props, null, LOG, new BuildInformationHolder());
                    fail("expected ServerConfigurationException for " + prefix);
                } catch (ServerConfigurationException e) {
                    TestUtils.assertContains(e.getMessage(), "Use io.questdb.tls.TlsServerMain as the entry point.");
                }
            }
        });
    }

    @Test
    public void testReadSettingPrefersEnvironmentAndTrims() {
        final var props = new Properties();
        props.setProperty("tls.cert.path", " /from/props.crt ");
        assertEquals("/from/props.crt", TlsBootstrapConfiguration.readSetting(props, null, PropertyKey.TLS_CERT_PATH, "dflt"));
        assertEquals("/from/props.crt", TlsBootstrapConfiguration.readSetting(props, Map.of(), PropertyKey.TLS_CERT_PATH, "dflt"));
        assertEquals("/from/env.crt", TlsBootstrapConfiguration.readSetting(props, Map.of("QDB_TLS_CERT_PATH", "/from/env.crt "), PropertyKey.TLS_CERT_PATH, "dflt"));
        assertEquals("dflt", TlsBootstrapConfiguration.readSetting(props, null, PropertyKey.TLS_PRIVATE_KEY_PATH, "dflt"));
        assertNull(TlsBootstrapConfiguration.readSetting(props, null, PropertyKey.TLS_ENABLED, null));
    }

    @Test
    public void testTlsDisabledSucceeds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            // No TLS properties set -- should construct normally
            final var config = new PropServerConfiguration(
                    root, props, null, LOG, new BuildInformationHolder()
            );
            assertNotNull(config);
        });
    }

    @Test
    public void testTlsEnabledWithoutOverlayFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var props = new Properties();
            props.setProperty("tls.enabled", "true");
            props.setProperty("tls.cert.path", certPath);
            props.setProperty("tls.private.key.path", keyPath);
            try {
                // The 5-arg constructor passes DefaultFactoryProvider lambda as fpf,
                // which is not instanceof TlsFactoryProviderFactory, triggering the guard
                new PropServerConfiguration(
                        root, props, null, LOG, new BuildInformationHolder()
                );
                fail("expected ServerConfigurationException");
            } catch (ServerConfigurationException e) {
                assertTrue(
                        "expected error to mention TlsServerMain, got: " + e.getMessage(),
                        e.getMessage().contains("TlsServerMain")
                );
            }
        });
    }
}
