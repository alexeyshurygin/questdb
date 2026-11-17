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

import io.questdb.replication.ReplicationConfiguration;
import io.questdb.replication.store.s3.S3ObjectStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Creates the object store that {@code replication.object.store} names.
 */
public final class ObjectStoreFactory {

    private ObjectStoreFactory() {
    }

    /**
     * @param env the process environment, which the S3 store reads for credentials; null for an
     *            empty environment. The caller passes {@code System.getenv()}, so the stores never
     *            read the environment themselves and tests can supply their own.
     * @throws ObjectStoreException {@link ObjectStoreException.Kind#FATAL} when the settings are
     *                              invalid or, for S3, no credentials are available
     */
    public static @NotNull ObjectStore create(
            @NotNull ObjectStoreSettings settings,
            @NotNull ReplicationConfiguration configuration,
            @Nullable Map<String, String> env
    ) {
        final String scheme = settings.getScheme();
        if (ObjectStoreSettings.SCHEME_S3.equals(scheme)) {
            return new S3ObjectStore(settings, configuration, env != null ? env : Map.of());
        }
        // ObjectStoreSettings.parse() accepts only the s3 and fs schemes
        assert ObjectStoreSettings.SCHEME_FS.equals(scheme) : scheme;
        return new FsObjectStore(settings, configuration);
    }
}
