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


package io.questdb.replication;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Role of a node in a primary/replica pair, set with {@code replication.role}. The names and meaning
 * match QuestDB Enterprise: {@code primary} is the read/write node that uploads its WAL to the object
 * store, {@code replica} is a read-only node that follows the object store, {@code none} disables
 * replication.
 */
public enum ReplicationRole {
    NONE("none"),
    PRIMARY("primary"),
    REPLICA("replica");

    private final String configName;

    ReplicationRole(String configName) {
        this.configName = configName;
    }

    /**
     * Parses a {@code replication.role} value, ignoring case and surrounding blanks. Returns null
     * for anything other than {@code none}, {@code primary} or {@code replica}.
     */
    public static @Nullable ReplicationRole fromConfig(@Nullable CharSequence value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.toString().trim();
        for (ReplicationRole role : values()) {
            if (role.configName.equalsIgnoreCase(trimmed)) {
                return role;
            }
        }
        return null;
    }

    public @NotNull String getConfigName() {
        return configName;
    }

    public boolean isEnabled() {
        return this != NONE;
    }
}
