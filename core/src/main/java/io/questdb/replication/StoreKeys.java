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

/**
 * Object store key layout, relative to the store root:
 * <pre>
 * _index                                              replication index: lease + table catalog
 * _checkpoints/&lt;instance id&gt;                          checkpoint history of one instance
 * t/&lt;table key&gt;/create                                table create manifest
 * t/&lt;table key&gt;/seq/&lt;part&gt;.&lt;epoch&gt;                     transaction log part
 * t/&lt;table key&gt;/wal/&lt;walId&gt;/&lt;segment&gt;/&lt;lo&gt;-&lt;hi&gt;.&lt;epoch&gt;  WAL segment delta
 * </pre>
 * A table key is {@code <table id>_<create timestamp>}, unique even if a table id is ever reused.
 * Keys that carry an epoch are written by one primary term only, so a deposed primary can never
 * overwrite what the new one wrote.
 */
public final class StoreKeys {
    public static final String CHECKPOINTS_PREFIX = "_checkpoints/";
    public static final String TABLES_PREFIX = "t/";

    private StoreKeys() {
    }

    public static @NotNull String checkpointHistory(@NotNull String instanceId) {
        return CHECKPOINTS_PREFIX + instanceId;
    }

    public static @NotNull String create(@NotNull String tableKey) {
        return TABLES_PREFIX + tableKey + "/create";
    }

    public static @NotNull String segmentDelta(@NotNull String tableKey, int walId, int segmentId, int lo, int hi, long epoch) {
        return TABLES_PREFIX + tableKey + "/wal/" + walId + '/' + segmentId + '/' + lo + '-' + hi + '.' + epoch;
    }

    public static @NotNull String seqPart(@NotNull String tableKey, long part, long epoch) {
        return TABLES_PREFIX + tableKey + "/seq/" + part + '.' + epoch;
    }

    public static @NotNull String tablePrefix(@NotNull String tableKey) {
        return TABLES_PREFIX + tableKey + '/';
    }
}
