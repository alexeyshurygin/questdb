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


package io.questdb.replication.store;

import org.jetbrains.annotations.NotNull;

/**
 * Failure of an object store request. {@link #getKind()} tells callers whether to retry, treat the
 * object as absent, or stop.
 */
public class ObjectStoreException extends RuntimeException {
    private final Kind kind;
    private final int status;

    public ObjectStoreException(@NotNull Kind kind, int status, String message) {
        super(message, null, false, false);
        this.kind = kind;
        this.status = status;
    }

    public ObjectStoreException(@NotNull Kind kind, int status, String message, Throwable cause) {
        super(message, cause, false, false);
        this.kind = kind;
        this.status = status;
    }

    public static ObjectStoreException fatal(String message) {
        return new ObjectStoreException(Kind.FATAL, 0, message);
    }

    public static ObjectStoreException notFound(String key) {
        return new ObjectStoreException(Kind.NOT_FOUND, 404, "object not found [key=" + key + ']');
    }

    public static ObjectStoreException preconditionFailed(String key) {
        return new ObjectStoreException(Kind.PRECONDITION_FAILED, 412, "object store precondition failed [key=" + key + ']');
    }

    public static ObjectStoreException retryable(String message, Throwable cause) {
        return new ObjectStoreException(Kind.RETRYABLE, 0, message, cause);
    }

    public @NotNull Kind getKind() {
        return kind;
    }

    /**
     * HTTP status of the failed request, 0 when there was none (I/O error, filesystem store).
     */
    public int getStatus() {
        return status;
    }

    public boolean isRetryable() {
        return kind == Kind.RETRYABLE;
    }

    public enum Kind {
        /**
         * The request cannot succeed by retrying: bad credentials, missing bucket, access denied.
         */
        FATAL,
        NOT_FOUND,
        /**
         * A conditional write lost: the object exists ({@code If-None-Match: *}) or changed
         * ({@code If-Match}).
         */
        PRECONDITION_FAILED,
        /**
         * Transient: network error, timeout, throttling, server error.
         */
        RETRYABLE
    }
}
