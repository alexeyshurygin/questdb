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

package io.questdb.tls;

import io.questdb.FactoryProvider;
import io.questdb.FactoryProviderFactory;
import io.questdb.FactoryProviderImpl;
import io.questdb.FreeOnExit;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.network.PlainSocketFactory;
import io.questdb.network.SocketFactory;
import org.jetbrains.annotations.NotNull;

public class TlsFactoryProviderFactory implements FactoryProviderFactory {
    private final ServerTlsConfiguration httpMinTlsConfig;
    private final ServerTlsConfiguration httpTlsConfig;
    private final ServerTlsConfiguration lineTcpTlsConfig;
    private final ServerTlsConfiguration pgTlsConfig;

    public TlsFactoryProviderFactory(
            ServerTlsConfiguration httpTlsConfig,
            ServerTlsConfiguration httpMinTlsConfig,
            ServerTlsConfiguration lineTcpTlsConfig,
            ServerTlsConfiguration pgTlsConfig
    ) {
        this.httpTlsConfig = httpTlsConfig;
        this.httpMinTlsConfig = httpMinTlsConfig;
        this.lineTcpTlsConfig = lineTcpTlsConfig;
        this.pgTlsConfig = pgTlsConfig;
    }

    @Override
    public @NotNull FactoryProvider getInstance(
            ServerConfiguration configuration, CairoEngine engine, FreeOnExit freeOnExit
    ) {
        final var delegate = new FactoryProviderImpl(configuration);
        final var httpFactory = createSocketFactory(httpTlsConfig);
        final var httpMinFactory = createSocketFactory(httpMinTlsConfig);
        final var lineFactory = createSocketFactory(lineTcpTlsConfig);
        final var pgFactory = createSocketFactory(pgTlsConfig);
        return new TlsFactoryProvider(delegate, httpFactory, httpMinFactory, lineFactory, pgFactory);
    }

    private static SocketFactory createSocketFactory(ServerTlsConfiguration tls) {
        if (!tls.isEnabled()) {
            return PlainSocketFactory.INSTANCE;
        }
        try {
            final var ctx = PemReader.createServerSslContext(tls.getCertPath(), tls.getPrivateKeyPath());
            return new JavaTlsServerSocketFactory(ctx);
        } catch (Exception e) {
            throw CairoException.critical(0).put("failed to initialize TLS: ").put(e.getMessage());
        }
    }
}
