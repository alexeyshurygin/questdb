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

import io.questdb.Bootstrap;
import io.questdb.ConfigPropertyKey;
import io.questdb.DynamicPropServerConfiguration;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.std.FilesFacadeImpl;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Properties;

/**
 * Bootstrap configuration of {@link TlsServerMain}: reads the per-listener TLS settings and installs the TLS
 * socket factories through {@link TlsFactoryProviderFactory}.
 */
public class TlsBootstrapConfiguration extends PropBootstrapConfiguration {

    /**
     * Reads a TLS setting the way the server reads any setting: the {@code QDB_}-prefixed environment variable
     * first, then the property, then the default. Values are trimmed.
     */
    public static String readSetting(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, String defaultValue) {
        final var envVarName = ServerMain.propertyPathToEnvVarName(key.getPropertyPath());
        var result = env != null ? env.get(envVarName) : null;
        if (result == null) {
            result = properties.getProperty(key.getPropertyPath());
        }
        return result != null ? result.trim() : defaultValue;
    }

    @Override
    public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) throws Exception {
        final var properties = bootstrap.loadProperties();
        final var rootDir = bootstrap.getRootDirectory();
        final ServerTlsConfiguration.GetStringFn simpleGetString = TlsBootstrapConfiguration::readSetting;
        final var globalTls = ServerTlsConfiguration.parse(
                properties, getEnv(), rootDir,
                PropertyKey.TLS_ENABLED, PropertyKey.TLS_CERT_PATH, PropertyKey.TLS_PRIVATE_KEY_PATH,
                ServerTlsConfiguration.DISABLED, simpleGetString
        );
        final var httpTls = ServerTlsConfiguration.parse(
                properties, getEnv(), rootDir,
                PropertyKey.HTTP_TLS_ENABLED, PropertyKey.HTTP_TLS_CERT_PATH, PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
                globalTls, simpleGetString
        );
        final var httpMinTls = ServerTlsConfiguration.parse(
                properties, getEnv(), rootDir,
                PropertyKey.HTTP_MIN_TLS_ENABLED, PropertyKey.HTTP_MIN_TLS_CERT_PATH, PropertyKey.HTTP_MIN_TLS_PRIVATE_KEY_PATH,
                globalTls, simpleGetString
        );
        final var lineTcpTls = ServerTlsConfiguration.parse(
                properties, getEnv(), rootDir,
                PropertyKey.LINE_TCP_TLS_ENABLED, PropertyKey.LINE_TCP_TLS_CERT_PATH, PropertyKey.LINE_TCP_TLS_PRIVATE_KEY_PATH,
                globalTls, simpleGetString
        );
        final var pgTls = ServerTlsConfiguration.parse(
                properties, getEnv(), rootDir,
                PropertyKey.PG_TLS_ENABLED, PropertyKey.PG_TLS_CERT_PATH, PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
                globalTls, simpleGetString
        );
        return new DynamicPropServerConfiguration(
                rootDir,
                properties,
                getEnv(),
                bootstrap.getLog(),
                bootstrap.getBuildInformation(),
                FilesFacadeImpl.INSTANCE,
                bootstrap.getMicrosecondClock(),
                new TlsFactoryProviderFactory(httpTls, httpMinTls, lineTcpTls, pgTls)
        );
    }
}
