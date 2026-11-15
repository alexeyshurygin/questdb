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
import io.questdb.FactoryProvider;
import io.questdb.PropServerConfiguration;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import io.questdb.test.tools.TestUtils;
import io.questdb.tls.JavaTlsServerSocketFactory;
import io.questdb.tls.ServerTlsConfiguration;
import io.questdb.tls.TlsFactoryProvider;
import io.questdb.tls.TlsFactoryProviderFactory;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Properties;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;

import static org.junit.Assert.*;

public class TlsFactoryProviderTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static final Log LOG = LogFactory.getLog(TlsFactoryProviderTest.class);

    private static String certPath;
    private static String keyPath;
    private static boolean opensslAvailable;
    private static String root;

    private static boolean checkOpensslAvailable() {
        try {
            final var process = new ProcessBuilder("openssl", "version")
                    .redirectErrorStream(true)
                    .start();
            return process.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        final var rootDir = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(rootDir.getAbsolutePath());
        root = rootDir.getAbsolutePath();
        opensslAvailable = checkOpensslAvailable();
        if (opensslAvailable) {
            final var certFile = temp.newFile("factory-cert.crt");
            final var keyFile = temp.newFile("factory-key.key");
            generateSelfSignedCert(certFile, keyFile);
            certPath = certFile.getAbsolutePath();
            keyPath = keyFile.getAbsolutePath();
        }
    }

    private static void generateSelfSignedCert(File certFile, File keyFile) throws Exception {
        final var tmpKey = temp.newFile("factory-raw.key");
        final var genProcess = new ProcessBuilder(
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", tmpKey.getAbsolutePath(),
                "-out", certFile.getAbsolutePath(),
                "-days", "1", "-nodes",
                "-subj", "/CN=localhost"
        ).redirectErrorStream(true).start();
        assertEquals(0, genProcess.waitFor());
        final var convertProcess = new ProcessBuilder(
                "openssl", "pkcs8", "-topk8", "-nocrypt",
                "-in", tmpKey.getAbsolutePath(),
                "-out", keyFile.getAbsolutePath()
        ).redirectErrorStream(true).start();
        assertEquals(0, convertProcess.waitFor());
    }

    @Test
    public void testDelegatesEveryNonSocketMethod() throws Exception {
        final var invoked = new ArrayList<String>();
        final var returned = new Object[1];
        final var delegate = (FactoryProvider) Proxy.newProxyInstance(
                FactoryProvider.class.getClassLoader(),
                new Class<?>[]{FactoryProvider.class},
                (proxy, method, args) -> {
                    invoked.add(method.getName());
                    final var returnType = method.getReturnType();
                    // a distinct instance of the declared return type, so the caller's result can be traced back
                    returned[0] = returnType.isInterface()
                            ? Proxy.newProxyInstance(returnType.getClassLoader(), new Class<?>[]{returnType}, (p, m, a) -> null)
                            : null;
                    return returned[0];
                }
        );
        final SocketFactory http = PlainSocketFactory.INSTANCE;
        final SocketFactory httpMin = (nf, log) -> null;
        final SocketFactory line = (nf, log) -> null;
        final SocketFactory pg = (nf, log) -> null;
        final var provider = new TlsFactoryProvider(delegate, http, httpMin, line, pg);
        for (Method method : FactoryProvider.class.getMethods()) {
            invoked.clear();
            returned[0] = null;
            final var paramTypes = method.getParameterTypes();
            final var args = new Object[paramTypes.length];
            for (var i = 0; i < args.length; i++) {
                args[i] = paramTypes[i] == int.class ? 3 : null;
            }
            final var result = method.invoke(provider, args);
            switch (method.getName()) {
                case "getHttpSocketFactory" -> assertSame(http, result);
                case "getHttpMinSocketFactory" -> assertSame(httpMin, result);
                case "getLineSocketFactory" -> assertSame(line, result);
                case "getPGWireSocketFactory" -> assertSame(pg, result);
                default -> {
                    assertEquals("method must reach the delegate: " + method, 1, invoked.size());
                    assertEquals(method.getName(), invoked.getFirst());
                    assertSame("method must return the delegate's result: " + method, returned[0], result);
                }
            }
        }
    }

    @Test
    public void testOverridesEveryFactoryProviderMethod() throws Exception {
        // A default method of the interface that TlsFactoryProvider does not override would bypass the delegate.
        for (Method method : FactoryProvider.class.getMethods()) {
            final var impl = TlsFactoryProvider.class.getMethod(method.getName(), method.getParameterTypes());
            assertEquals("TlsFactoryProvider must override " + method, TlsFactoryProvider.class, impl.getDeclaringClass());
        }
    }

    @Test
    public void testInvalidCertificateFailsProvider() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var badCert = temp.newFile("bad.crt");
            final var badKey = temp.newFile("bad.key");
            java.nio.file.Files.writeString(badCert.toPath(), "not a certificate");
            java.nio.file.Files.writeString(badKey.toPath(), "not a key");
            final var tlsConfig = new ServerTlsConfiguration(true, badCert.getAbsolutePath(), badKey.getAbsolutePath());
            final var fpf = new TlsFactoryProviderFactory(tlsConfig, ServerTlsConfiguration.DISABLED, ServerTlsConfiguration.DISABLED, ServerTlsConfiguration.DISABLED);
            final var config = new PropServerConfiguration(
                    root, new Properties(), null, LOG, new BuildInformationHolder(),
                    FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE, fpf
            );
            try {
                fpf.getInstance(config, null, null);
                fail("expected CairoException");
            } catch (io.questdb.cairo.CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "failed to initialize TLS: ");
            }
        });
    }

    @Test
    public void testMixedConfig() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var httpTls = new ServerTlsConfiguration(true, certPath, keyPath);
            final var pgTls = ServerTlsConfiguration.DISABLED;
            final var fpf = new TlsFactoryProviderFactory(
                    httpTls,
                    ServerTlsConfiguration.DISABLED,
                    ServerTlsConfiguration.DISABLED,
                    pgTls
            );
            final var props = new Properties();
            final var config = new PropServerConfiguration(
                    root, props, null, LOG, new BuildInformationHolder(),
                    FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE, fpf
            );
            final var provider = fpf.getInstance(config, null, null);
            assertTrue(provider.getHttpSocketFactory() instanceof JavaTlsServerSocketFactory);
            assertSame(PlainSocketFactory.INSTANCE, provider.getPGWireSocketFactory());
            provider.close();
        });
    }

    @Test
    public void testTlsDisabledReturnsPlainFactory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var fpf = new TlsFactoryProviderFactory(
                    ServerTlsConfiguration.DISABLED,
                    ServerTlsConfiguration.DISABLED,
                    ServerTlsConfiguration.DISABLED,
                    ServerTlsConfiguration.DISABLED
            );
            final var props = new Properties();
            final var config = new PropServerConfiguration(
                    root, props, null, LOG, new BuildInformationHolder(),
                    FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE, fpf
            );
            final var provider = fpf.getInstance(config, null, null);
            assertSame(PlainSocketFactory.INSTANCE, provider.getHttpSocketFactory());
            assertSame(PlainSocketFactory.INSTANCE, provider.getHttpMinSocketFactory());
            assertSame(PlainSocketFactory.INSTANCE, provider.getLineSocketFactory());
            assertSame(PlainSocketFactory.INSTANCE, provider.getPGWireSocketFactory());
            provider.close();
        });
    }

    @Test
    public void testTlsEnabledReturnsJavaTlsFactory() throws Exception {
        Assume.assumeTrue("openssl not available", opensslAvailable);
        TestUtils.assertMemoryLeak(() -> {
            final var tlsConfig = new ServerTlsConfiguration(true, certPath, keyPath);
            final var fpf = new TlsFactoryProviderFactory(tlsConfig, tlsConfig, tlsConfig, tlsConfig);
            final var props = new Properties();
            final var config = new PropServerConfiguration(
                    root, props, null, LOG, new BuildInformationHolder(),
                    FilesFacadeImpl.INSTANCE, MicrosecondClockImpl.INSTANCE, fpf
            );
            final var provider = fpf.getInstance(config, null, null);
            assertTrue(provider.getHttpSocketFactory() instanceof JavaTlsServerSocketFactory);
            assertTrue(provider.getHttpMinSocketFactory() instanceof JavaTlsServerSocketFactory);
            assertTrue(provider.getLineSocketFactory() instanceof JavaTlsServerSocketFactory);
            assertTrue(provider.getPGWireSocketFactory() instanceof JavaTlsServerSocketFactory);
            provider.close();
        });
    }
}
