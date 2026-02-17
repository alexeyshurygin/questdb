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
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.WalJobFactory;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.auth.LineAuthenticatorFactory;
import io.questdb.cutlass.http.HttpAuthenticatorFactory;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpHeaderParserFactory;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.RejectProcessorFactory;
import io.questdb.cutlass.http.processors.TextImportRequestHeaderProcessor;
import io.questdb.cutlass.pgwire.PGAuthenticatorFactory;
import io.questdb.network.SocketFactory;
import org.jetbrains.annotations.NotNull;

public class TlsFactoryProvider implements FactoryProvider {
    private final FactoryProvider delegate;
    private final SocketFactory httpMinSocketFactory;
    private final SocketFactory httpSocketFactory;
    private final SocketFactory lineSocketFactory;
    private final SocketFactory pgWireSocketFactory;

    public TlsFactoryProvider(
            FactoryProvider delegate,
            SocketFactory httpSocketFactory,
            SocketFactory httpMinSocketFactory,
            SocketFactory lineSocketFactory,
            SocketFactory pgWireSocketFactory
    ) {
        this.delegate = delegate;
        this.httpMinSocketFactory = httpMinSocketFactory;
        this.httpSocketFactory = httpSocketFactory;
        this.lineSocketFactory = lineSocketFactory;
        this.pgWireSocketFactory = pgWireSocketFactory;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public @NotNull HttpAuthenticatorFactory getHttpAuthenticatorFactory() {
        return delegate.getHttpAuthenticatorFactory();
    }

    @Override
    public @NotNull HttpCookieHandler getHttpCookieHandler() {
        return delegate.getHttpCookieHandler();
    }

    @Override
    public @NotNull HttpHeaderParserFactory getHttpHeaderParserFactory() {
        return delegate.getHttpHeaderParserFactory();
    }

    @Override
    public @NotNull SocketFactory getHttpMinSocketFactory() {
        return httpMinSocketFactory;
    }

    @Override
    public @NotNull HttpSessionStore getHttpSessionStore() {
        return delegate.getHttpSessionStore();
    }

    @Override
    public @NotNull SocketFactory getHttpSocketFactory() {
        return httpSocketFactory;
    }

    @Override
    public @NotNull LineAuthenticatorFactory getLineAuthenticatorFactory() {
        return delegate.getLineAuthenticatorFactory();
    }

    @Override
    public @NotNull SocketFactory getLineSocketFactory() {
        return lineSocketFactory;
    }

    @Override
    public @NotNull SocketFactory getPGWireSocketFactory() {
        return pgWireSocketFactory;
    }

    @Override
    public @NotNull PGAuthenticatorFactory getPgWireAuthenticatorFactory() {
        return delegate.getPgWireAuthenticatorFactory();
    }

    @Override
    public @NotNull RejectProcessorFactory getRejectProcessorFactory() {
        return delegate.getRejectProcessorFactory();
    }

    @Override
    public @NotNull SecurityContextFactory getSecurityContextFactory() {
        return delegate.getSecurityContextFactory();
    }

    @Override
    public @NotNull TextImportRequestHeaderProcessor getTextImportRequestHeaderProcessor() {
        return delegate.getTextImportRequestHeaderProcessor();
    }

    @Override
    public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
        return delegate.getTickCalendarServiceFactory();
    }

    @Override
    public @NotNull WalJobFactory getWalJobFactory() {
        return delegate.getWalJobFactory();
    }
}
