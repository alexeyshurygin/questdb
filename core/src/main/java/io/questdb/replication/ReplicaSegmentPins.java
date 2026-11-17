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

import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Remembers, per WAL directory, the newest segment a replica downloaded, so that the WAL purge job
 * keeps it: the primary may still be appending to it, and deleting it would make the replica write
 * the next delta into a fresh, sparse copy. A pin expires once its WAL received nothing for
 * {@code ttlMillis}, by which time the primary's writer has long closed it.
 */
public class ReplicaSegmentPins implements QuietCloseable {
    private final ConcurrentHashMap<String, Pin> pins = new ConcurrentHashMap<>();
    private final long ttlMillis;

    public ReplicaSegmentPins(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    private static String key(CharSequence tableDir, int walId) {
        return tableDir.toString() + '/' + walId;
    }

    @Override
    public void close() {
        pins.clear();
    }

    /**
     * True when the segment is the newest downloaded one of its WAL and its pin has not expired.
     */
    public boolean isPinned(@NotNull CharSequence tableDir, int walId, int segmentId, long nowMillis) {
        final Pin pin = pins.get(key(tableDir, walId));
        if (pin == null) {
            return false;
        }
        if (nowMillis - pin.lastUpdateMillis > ttlMillis) {
            pins.remove(key(tableDir, walId), pin);
            return false;
        }
        return pin.segmentId == segmentId;
    }

    public void onDownloaded(@NotNull CharSequence tableDir, int walId, int segmentId, long nowMillis) {
        pins.compute(key(tableDir, walId), (k, pin) -> {
            if (pin == null || pin.segmentId <= segmentId) {
                return new Pin(segmentId, nowMillis);
            }
            return pin;
        });
    }

    public int size() {
        return pins.size();
    }

    private record Pin(int segmentId, long lastUpdateMillis) {
    }
}
