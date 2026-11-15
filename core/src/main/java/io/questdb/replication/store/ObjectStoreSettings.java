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

import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Parsed {@code replication.object.store} string: {@code <scheme>::key=value;key=value;}. The
 * syntax and the key names follow QuestDB Enterprise, for example
 * {@code s3::bucket=b;root=db1;region=eu-west-1;} or {@code fs::root=/mnt/store;atomic_write_dir=/mnt/scratch;}.
 * Keys are case-insensitive; a value runs to the next {@code ;}. Unknown schemes and keys are
 * rejected so a typo cannot silently fall back to a default.
 */
public final class ObjectStoreSettings {
    public static final String KEY_ACCESS_KEY_ID = "access_key_id";
    public static final String KEY_ATOMIC_WRITE_DIR = "atomic_write_dir";
    public static final String KEY_BUCKET = "bucket";
    public static final String KEY_ENABLE_VIRTUAL_HOST_STYLE = "enable_virtual_host_style";
    public static final String KEY_ENDPOINT = "endpoint";
    public static final String KEY_REGION = "region";
    public static final String KEY_ROOT = "root";
    public static final String KEY_SECRET_ACCESS_KEY = "secret_access_key";
    public static final String KEY_SESSION_TOKEN = "session_token";
    public static final String SCHEME_FS = "fs";
    public static final String SCHEME_S3 = "s3";
    private static final String[] FS_KEYS = {KEY_ROOT, KEY_ATOMIC_WRITE_DIR};
    private static final String[] S3_KEYS = {
            KEY_ACCESS_KEY_ID, KEY_BUCKET, KEY_ENABLE_VIRTUAL_HOST_STYLE, KEY_ENDPOINT,
            KEY_REGION, KEY_ROOT, KEY_SECRET_ACCESS_KEY, KEY_SESSION_TOKEN
    };
    private final ObjList<String> keys = new ObjList<>();
    private final String scheme;
    private final LowerCaseCharSequenceObjHashMap<String> values = new LowerCaseCharSequenceObjHashMap<>();

    private ObjectStoreSettings(String scheme) {
        this.scheme = scheme;
    }

    /**
     * Parses and validates the store string.
     *
     * @throws IllegalArgumentException with a message naming the offending part; the message never
     *                                  contains a value, since values may be secrets
     */
    public static @NotNull ObjectStoreSettings parse(@Nullable CharSequence text) {
        if (text == null || text.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("object store string is empty");
        }
        final String s = text.toString().trim();
        final int sep = s.indexOf("::");
        if (sep < 1) {
            throw new IllegalArgumentException("object store string must start with '<scheme>::', for example 's3::' or 'fs::'");
        }
        final String scheme = s.substring(0, sep).trim().toLowerCase();
        final String[] allowed;
        switch (scheme) {
            case SCHEME_S3 -> allowed = S3_KEYS;
            case SCHEME_FS -> allowed = FS_KEYS;
            case "azblob", "gcs" -> throw new IllegalArgumentException("object store scheme is not supported in this build [scheme=" + scheme + ']');
            default -> throw new IllegalArgumentException("unknown object store scheme [scheme=" + scheme + ']');
        }
        final ObjectStoreSettings settings = new ObjectStoreSettings(scheme);
        int pos = sep + 2;
        final int len = s.length();
        while (pos < len) {
            int end = s.indexOf(';', pos);
            if (end < 0) {
                end = len;
            }
            final String pair = s.substring(pos, end).trim();
            pos = end + 1;
            if (pair.isEmpty()) {
                continue;
            }
            final int eq = pair.indexOf('=');
            if (eq < 1) {
                throw new IllegalArgumentException("object store setting must be 'key=value' [setting=" + safeKey(pair) + ']');
            }
            final String key = pair.substring(0, eq).trim().toLowerCase();
            final String value = pair.substring(eq + 1).trim();
            if (!contains(allowed, key)) {
                throw new IllegalArgumentException("unknown object store setting [scheme=" + scheme + ", key=" + key + ']');
            }
            if (settings.values.keyIndex(key) < 0) {
                throw new IllegalArgumentException("duplicate object store setting [key=" + key + ']');
            }
            if (value.isEmpty()) {
                throw new IllegalArgumentException("object store setting has an empty value [key=" + key + ']');
            }
            if (value.startsWith("${") && value.endsWith("}")) {
                throw new IllegalArgumentException("object store setting holds an unexpanded placeholder [key=" + key + ']');
            }
            settings.values.put(key, value);
            settings.keys.add(key);
        }
        switch (scheme) {
            case SCHEME_S3 -> settings.require(KEY_BUCKET);
            case SCHEME_FS -> settings.require(KEY_ROOT);
            default -> {
                // unreachable: unknown schemes were rejected above
                assert false : scheme;
            }
        }
        if (settings.has(KEY_ACCESS_KEY_ID) != settings.has(KEY_SECRET_ACCESS_KEY)) {
            throw new IllegalArgumentException("object store settings access_key_id and secret_access_key must be set together");
        }
        if (settings.has(KEY_SESSION_TOKEN) && !settings.has(KEY_ACCESS_KEY_ID)) {
            throw new IllegalArgumentException("object store setting session_token needs access_key_id and secret_access_key");
        }
        final String virtualHost = settings.get(KEY_ENABLE_VIRTUAL_HOST_STYLE);
        if (virtualHost != null && !"true".equalsIgnoreCase(virtualHost) && !"false".equalsIgnoreCase(virtualHost)) {
            throw new IllegalArgumentException("object store setting enable_virtual_host_style must be true or false");
        }
        return settings;
    }

    public @Nullable String get(@NotNull String key) {
        return values.get(key);
    }

    public @NotNull String getScheme() {
        return scheme;
    }

    public boolean has(@NotNull String key) {
        return values.keyIndex(key) < 0;
    }

    /**
     * The settings with secret values masked, safe to log.
     */
    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder(scheme).append("::");
        for (int i = 0, n = keys.size(); i < n; i++) {
            final String key = keys.getQuick(i);
            b.append(key).append('=');
            if (isSecret(key)) {
                b.append("****");
            } else {
                b.append(values.get(key));
            }
            b.append(';');
        }
        return b.toString();
    }

    private static boolean contains(String[] keys, String key) {
        for (String k : keys) {
            if (k.equals(key)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSecret(String key) {
        return KEY_SECRET_ACCESS_KEY.equals(key) || KEY_SESSION_TOKEN.equals(key) || KEY_ACCESS_KEY_ID.equals(key);
    }

    // A malformed pair may be a secret pasted without its key, so only its length is reported.
    private static String safeKey(String pair) {
        return "<" + pair.length() + " chars>";
    }

    private void require(String key) {
        if (!has(key)) {
            throw new IllegalArgumentException("object store setting is required [scheme=" + scheme + ", key=" + key + ']');
        }
    }
}
