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

import io.questdb.ConfigPropertyKey;
import io.questdb.ServerConfigurationException;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class ServerTlsConfiguration {
    public static final ServerTlsConfiguration DISABLED = new ServerTlsConfiguration(false, null, null);

    private final String certPath;
    private final boolean enabled;
    private final String privateKeyPath;

    public ServerTlsConfiguration(boolean enabled, String certPath, String privateKeyPath) {
        this.enabled = enabled;
        this.certPath = certPath;
        this.privateKeyPath = privateKeyPath;
    }

    public static ServerTlsConfiguration parse(
            Properties properties,
            @Nullable Map<String, String> env,
            String rootDir,
            ConfigPropertyKey enabledKey,
            ConfigPropertyKey certKey,
            ConfigPropertyKey keyKey,
            ServerTlsConfiguration fallback,
            GetStringFn getString
    ) throws ServerConfigurationException {
        final var enabledStr = getString.get(properties, env, enabledKey, null);
        final var enabled = enabledStr != null ? Boolean.parseBoolean(enabledStr) : fallback.isEnabled();
        final var certPath = getString.get(properties, env, certKey, fallback.getCertPath());
        final var keyPath = getString.get(properties, env, keyKey, fallback.getPrivateKeyPath());
        if (!enabled) {
            return new ServerTlsConfiguration(false, certPath, keyPath);
        }
        if (certPath == null || certPath.isEmpty()) {
            throw new ServerConfigurationException("TLS is enabled but certificate path is not set [key=" + certKey.getPropertyPath() + ']');
        }
        if (keyPath == null || keyPath.isEmpty()) {
            throw new ServerConfigurationException("TLS is enabled but private key path is not set [key=" + keyKey.getPropertyPath() + ']');
        }
        final var resolvedCertPath = resolvePath(rootDir, certPath);
        final var resolvedKeyPath = resolvePath(rootDir, keyPath);
        validateFileReadable(resolvedCertPath, certKey.getPropertyPath());
        validateFileReadable(resolvedKeyPath, keyKey.getPropertyPath());
        return new ServerTlsConfiguration(true, resolvedCertPath, resolvedKeyPath);
    }

    public String getCertPath() {
        return certPath;
    }

    public String getPrivateKeyPath() {
        return privateKeyPath;
    }

    public boolean isEnabled() {
        return enabled;
    }

    private static String resolvePath(String rootDir, String path) {
        final var file = new File(path);
        if (file.isAbsolute()) {
            return path;
        }
        return new File(rootDir, path).getPath();
    }

    private static void validateFileReadable(String path, String propertyPath) throws ServerConfigurationException {
        final var file = new File(path);
        if (!file.exists()) {
            throw new ServerConfigurationException("TLS file does not exist [key=" + propertyPath + ", path=" + path + ']');
        }
        if (!file.canRead()) {
            throw new ServerConfigurationException("TLS file is not readable [key=" + propertyPath + ", path=" + path + ']');
        }
    }

    @FunctionalInterface
    public interface GetStringFn {
        String get(Properties properties, @Nullable Map<String, String> env, ConfigPropertyKey key, String defaultValue);
    }
}
