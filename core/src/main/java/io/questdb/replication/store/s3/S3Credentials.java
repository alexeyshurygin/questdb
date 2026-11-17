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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable AWS credentials. {@link #toString()} never reveals the secret parts.
 */
public final class S3Credentials {
    /**
     * Expiration of credentials that never expire.
     */
    public static final long NO_EXPIRATION = Long.MAX_VALUE;
    private final String accessKeyId;
    private final long expirationMillis;
    private final String secretAccessKey;
    private final String sessionToken;

    public S3Credentials(@NotNull String accessKeyId, @NotNull String secretAccessKey, @Nullable String sessionToken, long expirationMillis) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.expirationMillis = expirationMillis;
    }

    public @NotNull String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * Epoch milliseconds after which the credentials stop working, {@link #NO_EXPIRATION} for none.
     */
    public long getExpirationMillis() {
        return expirationMillis;
    }

    public @NotNull String getSecretAccessKey() {
        return secretAccessKey;
    }

    public @Nullable String getSessionToken() {
        return sessionToken;
    }

    @Override
    public String toString() {
        return "S3Credentials[accessKeyId=****, secretAccessKey=****, sessionToken=" + (sessionToken != null ? "****" : "none")
                + ", expirationMillis=" + expirationMillis + ']';
    }
}
