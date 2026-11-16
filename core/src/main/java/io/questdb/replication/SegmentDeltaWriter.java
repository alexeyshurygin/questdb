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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.replication.codec.NativeReader;
import io.questdb.replication.codec.SegmentDelta;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

/**
 * Applies a {@link SegmentDelta} to a replica's copy of a WAL segment: writes every shipped range at
 * its offset, then the {@code _event} end marker and finally the {@code _event} header, so that the
 * header never names a transaction whose bytes are not there yet. With {@code isDurable}, it fsyncs
 * the files and the directories it created before returning: the caller publishes the segment's
 * transactions in the sequencer only after that. Rewriting a range the replica already holds writes
 * the same bytes again, so applying a delta twice is harmless. Not thread-safe.
 */
public class SegmentDeltaWriter implements QuietCloseable {
    private static final int SCRATCH_SIZE = 8;
    private final LongList fds = new LongList();
    private final FilesFacade ff;
    private final int mkDirMode;
    private final ObjList<String> names = new ObjList<>();
    private final Path path = new Path();
    private long scratch;

    public SegmentDeltaWriter(@NotNull FilesFacade ff, int mkDirMode) {
        this.ff = ff;
        this.mkDirMode = mkDirMode;
        this.scratch = Unsafe.malloc(SCRATCH_SIZE, MemoryTag.NATIVE_REPL);
    }

    /**
     * Writes the delta into {@code <dbRoot>/<tableDir>/wal<walId>/<segmentId>}.
     */
    public void apply(@NotNull CharSequence dbRoot, @NotNull CharSequence tableDir, @NotNull SegmentDelta delta, boolean isDurable) {
        path.of(dbRoot).concat(tableDir).concat(WalUtils.WAL_NAME_BASE).put(delta.getWalId());
        final int walLen = path.size();
        final boolean isNewWalDir = !ff.exists(path.$());
        path.slash().put(delta.getSegmentId());
        final int segLen = path.size();
        final boolean isNewSegmentDir = !ff.exists(path.$());
        if (isNewSegmentDir && ff.mkdirs(path.slash(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("could not create WAL segment directory [path=").put(path.trimTo(segLen)).put(']');
        }
        path.trimTo(segLen);
        long eventEnd = -1;
        try {
            while (delta.nextRange()) {
                final String name = delta.getFileName();
                final long fd = fd(segLen, name);
                final long len = delta.getFileLength();
                if (len > 0 && ff.write(fd, delta.getFilePtr(), len, delta.getFileOffset()) != len) {
                    throw CairoException.critical(ff.errno()).put("could not write WAL file [path=").put(path.trimTo(segLen))
                            .put(", file=").put(name).put(", offset=").put(delta.getFileOffset()).put(", length=").put(len).put(']');
                }
                if (WalUtils.EVENT_FILE_NAME.equals(name)) {
                    eventEnd = delta.getFileOffset() + len;
                }
            }
            if (eventEnd < WalUtils.WALE_HEADER_SIZE) {
                // a malformed object, as any other the decoder refuses: the table goes stale instead of
                // retrying it forever
                throw new NativeReader.CorruptObjectException("segment delta carries no event records [path=" + path.trimTo(segLen) + ']');
            }
            final long eventFd = fd(segLen, WalUtils.EVENT_FILE_NAME);
            writeInt(eventFd, eventEnd, -1);
            writeInt(eventFd, WalUtils.WAL_FORMAT_OFFSET_32, delta.getEventFormatVersion());
            if (isDurable) {
                // the header must not reach the device ahead of the records it names
                for (int i = 0, n = fds.size(); i < n; i++) {
                    ff.fsync(fds.getQuick(i));
                }
            }
            writeInt(eventFd, WalUtils.WALE_MAX_TXN_OFFSET_32, delta.getLastSegmentTxn());
            if (isDurable) {
                ff.fsync(eventFd);
                if (isNewSegmentDir) {
                    fsyncDir(path.trimTo(segLen));
                }
                if (isNewWalDir) {
                    fsyncDir(path.trimTo(walLen));
                }
            }
        } finally {
            closeFiles();
            path.trimTo(segLen);
        }
    }

    @Override
    public void close() {
        closeFiles();
        if (scratch != 0) {
            Unsafe.free(scratch, SCRATCH_SIZE, MemoryTag.NATIVE_REPL);
            scratch = 0;
        }
        path.close();
    }

    private void closeFiles() {
        for (int i = 0, n = fds.size(); i < n; i++) {
            ff.close(fds.getQuick(i));
        }
        fds.clear();
        names.clear();
    }

    private long fd(int segLen, String name) {
        for (int i = 0, n = names.size(); i < n; i++) {
            if (names.getQuick(i).equals(name)) {
                return fds.getQuick(i);
            }
        }
        final long fd = ff.openRW(path.trimTo(segLen).concat(name).$(), 0);
        if (fd < 0) {
            throw CairoException.critical(ff.errno()).put("could not open WAL file for writing [path=").put(path).put(']');
        }
        names.add(name);
        fds.add(fd);
        path.trimTo(segLen);
        return fd;
    }

    private void fsyncDir(Path dir) {
        TableUtils.fsyncDirDurable(ff, dir.$());
    }

    private void writeInt(long fd, long offset, int value) {
        Unsafe.getUnsafe().putInt(scratch, value);
        if (ff.write(fd, scratch, 4, offset) != 4) {
            throw CairoException.critical(ff.errno()).put("could not write WAL event file [path=").put(path).put(", offset=").put(offset).put(']');
        }
    }
}
