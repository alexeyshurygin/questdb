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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Keeps the WAL purge job away from what replication still needs.
 * <p>
 * On a primary, every WAL segment and sequencer directory of a replicated table gets an
 * {@value #UPLOAD_PENDING_MARKER} file when it is created; the purge job keeps a directory with the
 * marker, and the uploader removes it once the object store holds everything in it. The marker is a
 * file so that it survives a restart. Sequencer parts whose transactions are not uploaded yet are
 * pinned through {@link Pins}.
 * <p>
 * On a replica, the newest downloaded segment of each WAL is pinned through
 * {@link ReplicaSegmentPins}: the primary may still append to it.
 * <p>
 * Live views and system tables are node-local and never pinned.
 */
public class ReplicationWalDirectoryPolicy implements WalDirectoryPolicy, QuietCloseable {
    public static final String UPLOAD_PENDING_MARKER = "upload.pending";
    private final FilesFacade ff;
    private final Path markerPath = new Path();
    private final @Nullable Pins primaryPins;
    private final @Nullable ReplicaSegmentPins replicaPins;

    public ReplicationWalDirectoryPolicy(@NotNull FilesFacade ff, @Nullable Pins primaryPins, @Nullable ReplicaSegmentPins replicaPins) {
        this.ff = ff;
        this.primaryPins = primaryPins;
        this.replicaPins = replicaPins;
    }

    @Override
    public void close() {
        synchronized (markerPath) {
            markerPath.close();
        }
    }

    public static boolean isReplicated(@NotNull TableToken token) {
        return token.isWal() && !token.isLiveView() && !token.isSystem();
    }

    @Override
    public void initDirectory(Path dirPath) {
        // without a token we cannot tell a node-local table: no marker
    }

    @Override
    public void initDirectory(Path dirPath, TableToken tableToken) {
        if (primaryPins == null || tableToken == null || !isReplicated(tableToken)) {
            return;
        }
        synchronized (markerPath) {
            markerPath.of(dirPath);
            if (markerPath.size() > 0 && markerPath.byteAt(markerPath.size() - 1) != '/') {
                markerPath.slash();
            }
            markerPath.put(UPLOAD_PENDING_MARKER);
            final long fd = ff.openRW(markerPath.$(), 0);
            if (fd > -1) {
                ff.close(fd);
            }
        }
    }

    @Override
    public boolean isInUse(Path path) {
        if (primaryPins != null) {
            synchronized (markerPath) {
                markerPath.of(path);
                if (markerPath.size() > 0 && markerPath.byteAt(markerPath.size() - 1) != '/') {
                    markerPath.slash();
                }
                return ff.exists(markerPath.put(UPLOAD_PENDING_MARKER).$());
            }
        }
        if (replicaPins != null) {
            return isReplicaSegmentPinned(path);
        }
        return false;
    }

    @Override
    public boolean isSeqPartInUse(Path seqDirPath, long partId) {
        if (primaryPins == null) {
            return false;
        }
        final String tableDir = parentName(seqDirPath);
        return tableDir != null && primaryPins.isSeqPartPinned(tableDir, partId);
    }

    @Override
    public void rollbackDirectory(Path path) {
        // a segment closed without a single event record holds nothing to upload
        removeMarker(path);
    }

    /**
     * Removes the marker of a directory, once the object store holds all of it.
     */
    public void removeMarker(Path dirPath) {
        if (primaryPins == null) {
            return;
        }
        synchronized (markerPath) {
            markerPath.of(dirPath);
            if (markerPath.size() > 0 && markerPath.byteAt(markerPath.size() - 1) != '/') {
                markerPath.slash();
            }
            ff.removeQuiet(markerPath.put(UPLOAD_PENDING_MARKER).$());
        }
    }

    @Override
    public boolean truncateFilesOnClose() {
        return DefaultWalDirectoryPolicy.INSTANCE.truncateFilesOnClose();
    }

    // <root>/<tableDir>/wal<N>/<segment>
    private boolean isReplicaSegmentPinned(Utf8Sequence path) {
        final String s = path.toString();
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '/') {
            end--;
        }
        final int segSlash = s.lastIndexOf('/', end - 1);
        if (segSlash < 1) {
            return false;
        }
        final int walSlash = s.lastIndexOf('/', segSlash - 1);
        if (walSlash < 1) {
            return false;
        }
        final int dirSlash = s.lastIndexOf('/', walSlash - 1);
        final String walName = s.substring(walSlash + 1, segSlash);
        if (!walName.startsWith(WalUtils.WAL_NAME_BASE)) {
            return false;
        }
        try {
            final int walId = Integer.parseInt(walName.substring(WalUtils.WAL_NAME_BASE.length()));
            final int segmentId = Integer.parseInt(s.substring(segSlash + 1, end));
            final String tableDir = s.substring(dirSlash + 1, walSlash);
            return replicaPins.isPinned(tableDir, walId, segmentId, System.currentTimeMillis());
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // name of the directory that holds <path>, e.g. "t~3" for <root>/t~3/txn_seq
    private static @Nullable String parentName(Utf8Sequence path) {
        final String s = path.toString();
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == '/') {
            end--;
        }
        final int last = s.lastIndexOf('/', end - 1);
        if (last < 1) {
            return null;
        }
        final int prev = s.lastIndexOf('/', last - 1);
        return s.substring(prev + 1, last);
    }

    /**
     * Which sequencer parts the primary's uploader still needs.
     */
    public interface Pins {
        boolean isSeqPartPinned(@NotNull String tableDir, long partId);
    }
}
