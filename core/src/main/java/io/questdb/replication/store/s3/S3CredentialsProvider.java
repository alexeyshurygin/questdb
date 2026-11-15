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

package io.questdb.replication.store.s3;

import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreSettings;
import org.jetbrains.annotations.NotNull;

import java.net.http.HttpClient;
import java.util.Map;

/**
 * Supplies the credentials that sign S3 requests. Implementations are thread-safe.
 */
@FunctionalInterface
public interface S3CredentialsProvider {
    String ENV_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    String ENV_EC2_METADATA_DISABLED = "AWS_EC2_METADATA_DISABLED";
    String ENV_EC2_METADATA_SERVICE_ENDPOINT = "AWS_EC2_METADATA_SERVICE_ENDPOINT";
    String ENV_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
    String ENV_SESSION_TOKEN = "AWS_SESSION_TOKEN";

    /**
     * Picks the first source that has credentials, in the order of the AWS SDKs' default chain:
     * <ol>
     *     <li>{@code access_key_id}, {@code secret_access_key} and {@code session_token} in the object
     *     store settings;</li>
     *     <li>the {@code AWS_ACCESS_KEY_ID}, {@code AWS_SECRET_ACCESS_KEY} and
     *     {@code AWS_SESSION_TOKEN} environment variables;</li>
     *     <li>the EC2 instance metadata service (IMDSv2), unless
     *     {@code AWS_EC2_METADATA_DISABLED=true}; {@code AWS_EC2_METADATA_SERVICE_ENDPOINT}
     *     overrides its address.</li>
     * </ol>
     *
     * @param env           the process environment; the caller passes it so tests can supply
     *                      their own
     * @param httpClient    client for instance metadata requests
     * @param timeoutMillis timeout of each instance metadata request
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#FATAL} when no source applies or
     *                              a source is incomplete
     */
    static @NotNull S3CredentialsProvider fromSettings(
            @NotNull ObjectStoreSettings settings,
            @NotNull Map<String, String> env,
            @NotNull HttpClient httpClient,
            long timeoutMillis
    ) {
        final String accessKeyId = settings.get(ObjectStoreSettings.KEY_ACCESS_KEY_ID);
        if (accessKeyId != null) {
            // ObjectStoreSettings.parse() guarantees that the secret comes with the key id
            final S3Credentials credentials = new S3Credentials(
                    accessKeyId,
                    settings.get(ObjectStoreSettings.KEY_SECRET_ACCESS_KEY),
                    settings.get(ObjectStoreSettings.KEY_SESSION_TOKEN),
                    S3Credentials.NO_EXPIRATION
            );
            return () -> credentials;
        }
        final String envAccessKeyId = nonEmpty(env.get(ENV_ACCESS_KEY_ID));
        final String envSecretAccessKey = nonEmpty(env.get(ENV_SECRET_ACCESS_KEY));
        if (envAccessKeyId != null || envSecretAccessKey != null) {
            if (envAccessKeyId == null || envSecretAccessKey == null) {
                throw ObjectStoreException.fatal("environment variables " + ENV_ACCESS_KEY_ID + " and " + ENV_SECRET_ACCESS_KEY
                        + " must be set together");
            }
            final S3Credentials credentials = new S3Credentials(
                    envAccessKeyId,
                    envSecretAccessKey,
                    nonEmpty(env.get(ENV_SESSION_TOKEN)),
                    S3Credentials.NO_EXPIRATION
            );
            return () -> credentials;
        }
        final String disabled = env.get(ENV_EC2_METADATA_DISABLED);
        if (disabled != null && "true".equalsIgnoreCase(disabled.trim())) {
            throw ObjectStoreException.fatal("no AWS credentials found: set access_key_id and secret_access_key in the object store"
                    + " settings, set " + ENV_ACCESS_KEY_ID + " and " + ENV_SECRET_ACCESS_KEY + ", or enable the EC2 instance metadata"
                    + " service (" + ENV_EC2_METADATA_DISABLED + "=true disables it)");
        }
        final String endpoint = nonEmpty(env.get(ENV_EC2_METADATA_SERVICE_ENDPOINT));
        return new S3ImdsCredentialsProvider(
                httpClient,
                endpoint != null ? endpoint : S3ImdsCredentialsProvider.DEFAULT_ENDPOINT,
                timeoutMillis,
                System::currentTimeMillis
        );
    }

    private static String nonEmpty(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    /**
     * Returns credentials that are valid now.
     *
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#RETRYABLE} when the source is
     *                              temporarily unreachable, {@link ObjectStoreException.Kind#FATAL}
     *                              when it cannot provide credentials
     */
    @NotNull
    S3Credentials getCredentials();
}
