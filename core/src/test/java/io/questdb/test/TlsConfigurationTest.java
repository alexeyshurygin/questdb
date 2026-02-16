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

package io.questdb.test;

import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerConfigurationException;
import io.questdb.ServerTlsConfiguration;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class TlsConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final Log LOG = LogFactory.getLog(TlsConfigurationTest.class);
    private static String certPath;
    private static String keyPath;
    private static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setUp() throws IOException {
        final var rootDir = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(rootDir.getAbsolutePath());
        root = rootDir.getAbsolutePath();
        final var certFile = temp.newFile("server.crt");
        certPath = certFile.getAbsolutePath();
        final var keyFile = temp.newFile("server.key");
        keyPath = keyFile.getAbsolutePath();
    }

    @Test
    public void testGlobalTlsEnabledPropagates() throws Exception {
        final var props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", certPath);
        props.setProperty("tls.private.key.path", keyPath);
        final var config = newConfig(props);
        final var httpTls = config.getHttpServerConfiguration().getServerTlsConfiguration();
        final var httpMinTls = config.getHttpMinServerConfiguration().getServerTlsConfiguration();
        final var lineTcpTls = config.getLineTcpReceiverConfiguration().getServerTlsConfiguration();
        final var pgTls = config.getPGWireConfiguration().getServerTlsConfiguration();
        assertTrue(httpTls.isEnabled());
        assertTrue(httpMinTls.isEnabled());
        assertTrue(lineTcpTls.isEnabled());
        assertTrue(pgTls.isEnabled());
        assertEquals(certPath, httpTls.getCertPath());
        assertEquals(keyPath, httpTls.getPrivateKeyPath());
    }

    @Test(expected = ServerConfigurationException.class)
    public void testMissingCertPathThrows() throws Exception {
        final var props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.private.key.path", keyPath);
        newConfig(props);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testMissingKeyPathThrows() throws Exception {
        final var props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", certPath);
        newConfig(props);
    }

    @Test(expected = ServerConfigurationException.class)
    public void testNonexistentCertFileThrows() throws Exception {
        final var props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", "/nonexistent/cert.pem");
        props.setProperty("tls.private.key.path", keyPath);
        newConfig(props);
    }

    @Test
    public void testPerProtocolDisableOverridesGlobal() throws Exception {
        final var props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", certPath);
        props.setProperty("tls.private.key.path", keyPath);
        props.setProperty("pg.tls.enabled", "false");
        final var config = newConfig(props);
        assertTrue(config.getHttpServerConfiguration().getServerTlsConfiguration().isEnabled());
        assertTrue(config.getLineTcpReceiverConfiguration().getServerTlsConfiguration().isEnabled());
        assertFalse(config.getPGWireConfiguration().getServerTlsConfiguration().isEnabled());
    }

    @Test
    public void testPerProtocolEnableWithoutGlobal() throws Exception {
        final var props = new Properties();
        props.setProperty("http.tls.enabled", "true");
        props.setProperty("http.tls.cert.path", certPath);
        props.setProperty("http.tls.private.key.path", keyPath);
        final var config = newConfig(props);
        assertTrue(config.getHttpServerConfiguration().getServerTlsConfiguration().isEnabled());
        assertFalse(config.getHttpMinServerConfiguration().getServerTlsConfiguration().isEnabled());
        assertFalse(config.getLineTcpReceiverConfiguration().getServerTlsConfiguration().isEnabled());
        assertFalse(config.getPGWireConfiguration().getServerTlsConfiguration().isEnabled());
    }

    @Test
    public void testTlsDisabledByDefault() throws Exception {
        final var config = newConfig(new Properties());
        final var httpTls = config.getHttpServerConfiguration().getServerTlsConfiguration();
        assertNotNull(httpTls);
        assertFalse(httpTls.isEnabled());
        assertFalse(config.getPGWireConfiguration().getServerTlsConfiguration().isEnabled());
    }

    private PropServerConfiguration newConfig(Properties props) throws Exception {
        return new PropServerConfiguration(root, props, null, LOG, new BuildInformationHolder());
    }
}
