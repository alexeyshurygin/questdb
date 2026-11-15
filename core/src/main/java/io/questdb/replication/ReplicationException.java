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

/**
 * A replication failure that retrying cannot fix on its own: the object store and the local
 * database disagree, an object is corrupt, or a table uses a feature replication does not support.
 * The affected table stops replicating until an operator intervenes; other tables continue.
 */
public class ReplicationException extends RuntimeException {
    private final Kind kind;

    private ReplicationException(Kind kind, String message) {
        super(message, null, false, false);
        this.kind = kind;
    }

    public static ReplicationException corrupt(String message) {
        return new ReplicationException(Kind.CORRUPT, message);
    }

    public static ReplicationException divergence(String message) {
        return new ReplicationException(Kind.DIVERGENCE, message);
    }

    public static ReplicationException gap(String message) {
        return new ReplicationException(Kind.GAP, message);
    }

    /**
     * An object the index names is not in the store: the cleaner may have removed it after this node
     * read the index, so a fresh index can tell whether it is really gone.
     */
    public static ReplicationException missing(String message) {
        return new ReplicationException(Kind.MISSING, message);
    }

    public static ReplicationException unsupported(String message) {
        return new ReplicationException(Kind.UNSUPPORTED, message);
    }

    public Kind getKind() {
        return kind;
    }

    public enum Kind {
        /**
         * An object fails validation.
         */
        CORRUPT,
        /**
         * Local history and the object store's history differ.
         */
        DIVERGENCE,
        /**
         * The object store no longer holds transactions this node needs.
         */
        GAP,
        /**
         * An object the index names is missing; retried with a fresh index before it counts as a gap.
         */
        MISSING,
        /**
         * The table uses something replication does not handle.
         */
        UNSUPPORTED
    }
}
