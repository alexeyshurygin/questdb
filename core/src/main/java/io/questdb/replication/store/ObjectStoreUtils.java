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
import org.jetbrains.annotations.Nullable;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Helpers shared by the object store implementations: key validation, ETag handling, hashing and
 * the pause between retries.
 */
public final class ObjectStoreUtils {
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    private ObjectStoreUtils() {
    }

    /**
     * Lower-case hex of the first {@code length} bytes of {@code bytes}.
     */
    public static @NotNull String hex(byte @NotNull [] bytes, int length) {
        final char[] chars = new char[length * 2];
        for (int i = 0; i < length; i++) {
            final int b = bytes[i] & 0xff;
            chars[2 * i] = HEX_DIGITS[b >>> 4];
            chars[2 * i + 1] = HEX_DIGITS[b & 0x0f];
        }
        return new String(chars);
    }

    public static @NotNull MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // every Java runtime must provide SHA-256
            throw new AssertionError("SHA-256 is not available", e);
        }
    }

    /**
     * The pause before a retry doubles with every attempt, up to this multiple of the retry interval.
     */
    public static final int MAX_BACKOFF_MULTIPLIER = 10;

    /**
     * The pause after an attempt that followed a pause of {@code pause} milliseconds.
     */
    public static long nextRetryPause(long pause, long retryInterval) {
        return Math.min(pause * 2, retryInterval * MAX_BACKOFF_MULTIPLIER);
    }

    /**
     * How long the pauses between the attempts of one request add up to, when every attempt fails.
     */
    public static long totalRetryPause(int retryAttempts, long retryInterval) {
        long total = 0;
        long pause = retryInterval;
        for (int i = 0; i < retryAttempts; i++) {
            total += pause;
            pause = nextRetryPause(pause, retryInterval);
        }
        return total;
    }

    /**
     * Sleeps before the next attempt of a failed request.
     *
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#RETRYABLE} when the thread is
     *                              interrupted; the interrupt flag is restored
     */
    public static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ObjectStoreException.retryable("object store request interrupted", e);
        }
    }

    /**
     * Lower-case hex of the SHA-256 of a byte range.
     */
    public static @NotNull String sha256Hex(byte @NotNull [] data, int offset, int length) {
        final MessageDigest digest = newSha256();
        digest.update(data, offset, length);
        final byte[] hash = digest.digest();
        return hex(hash, hash.length);
    }

    /**
     * Removes the double quotes that HTTP puts around an ETag, so callers may pass either form.
     */
    public static @Nullable String stripQuotes(@Nullable String etag) {
        if (etag != null && etag.length() >= 2 && etag.charAt(0) == '"' && etag.charAt(etag.length() - 1) == '"') {
            return etag.substring(1, etag.length() - 1);
        }
        return etag;
    }

    /**
     * Rejects keys that the filesystem store cannot map to a file below its root. Both stores apply
     * the same rules so that code tested against one behaves the same against the other.
     *
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#FATAL} for an empty or absolute
     *                              key, a key with a backslash or NUL character, or a key with an
     *                              empty, '.' or '..' segment
     */
    public static void validateKey(@NotNull String key) {
        if (key.isEmpty()) {
            throw ObjectStoreException.fatal("object key is empty");
        }
        if (key.charAt(0) == '/') {
            throw ObjectStoreException.fatal("object key must be relative [key=" + key + ']');
        }
        if (key.indexOf('\\') >= 0 || key.indexOf('\0') >= 0) {
            throw ObjectStoreException.fatal("object key contains a backslash or NUL character [key=" + key.replace('\0', '?') + ']');
        }
        int segmentStart = 0;
        final int len = key.length();
        for (int i = 0; i <= len; i++) {
            if (i == len || key.charAt(i) == '/') {
                final int segmentLength = i - segmentStart;
                if (segmentLength == 0
                        || (segmentLength == 1 && key.charAt(segmentStart) == '.')
                        || (segmentLength == 2 && key.charAt(segmentStart) == '.' && key.charAt(segmentStart + 1) == '.')) {
                    throw ObjectStoreException.fatal("object key has an empty, '.' or '..' segment [key=" + key + ']');
                }
                segmentStart = i + 1;
            }
        }
    }
}
