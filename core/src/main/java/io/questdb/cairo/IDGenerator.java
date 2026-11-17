/*+*****************************************************************************
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

package io.questdb.cairo;

import io.questdb.std.str.Path;

import java.io.Closeable;

public interface IDGenerator extends Closeable {
    default void open() {
        open(null);
    }

    void open(Path path);

    /**
     * Makes sure no id {@code <= minId} is handed out from now on. Replication uses it to keep ids
     * taken from a primary (table ids, WAL ids) from being issued again after a promotion. Durable
     * under the generator's commit mode; never lowers the counter.
     */
    void advanceTo(long minId);

    long getCurrentId();

    long getNextId();

    void reset();

    void close();
}
