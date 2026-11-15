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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.ReplicationConfiguration;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Object store on a local or shared filesystem, configured with
 * {@code fs::root=<dir>;atomic_write_dir=<dir>;}. Each object is a file below {@code root}; the key
 * is the file's path relative to {@code root} with '/' separators.
 * <p>
 * A write lands in a temporary file in {@code atomic_write_dir} (default {@code <root>/.tmp}),
 * which must be on the same filesystem as {@code root}, and then moves into place atomically, so
 * readers never see a partial object:
 * <ul>
 *     <li>{@code If-None-Match: *} writes create a hard link to the temporary file, which fails
 *     when the object exists;</li>
 *     <li>{@code If-Match} writes compare the current ETag and rename while holding a JVM-wide
 *     lock for the root and an exclusive file lock on {@code <root>/.lock}, which serialises
 *     conditional writes across processes;</li>
 *     <li>other writes rename over the target.</li>
 * </ul>
 * Unconditional writes and deletes do not take the lock, so they race with conditional writes of
 * the same key; callers write a given key either always conditionally or never.
 * <p>
 * Keys follow {@link ObjectStoreUtils#validateKey}; the store also rejects {@code .lock} and keys
 * inside {@code atomic_write_dir}, and listings leave both out. A key cannot name both an object
 * and a "directory" of other objects, as it can in S3: such a write fails as FATAL.
 * <p>
 * The ETag is the lower-case hex of the first 16 bytes of the content's SHA-256. The store retries
 * an operation that fails with an {@link IOException} {@code retryAttempts} times, sleeping
 * {@code retryInterval} between attempts, and then throws a
 * {@link ObjectStoreException.Kind#RETRYABLE} exception.
 */
public class FsObjectStore implements ObjectStore {
    public static final String DEFAULT_ATOMIC_WRITE_DIR_NAME = ".tmp";
    public static final int DEFAULT_CLEANER_MAX_REQUESTS_PER_SECOND = 100;
    public static final String LOCK_FILE_NAME = ".lock";
    private static final int ETAG_BYTES = 16;
    private static final Log LOG = LogFactory.getLog(FsObjectStore.class);
    // the largest array ByteSink can grow to
    private static final long MAX_OBJECT_SIZE = Integer.MAX_VALUE - 8;
    private static final int READ_CHUNK_SIZE = 64 * 1024;
    // FileChannel locks belong to the process, so two stores on the same root in one JVM must also
    // exclude each other in memory, or the second lock() throws OverlappingFileLockException.
    private static final ConcurrentHashMap<Path, ReentrantLock> ROOT_LOCKS = new ConcurrentHashMap<>();
    private static final AtomicLong TEMP_FILE_COUNTER = new AtomicLong();
    private final Path atomicWriteDir;
    private final ReentrantLock conditionalWriteLock;
    private final String description;
    private final Path lockFile;
    private final int retryAttempts;
    private final long retryInterval;
    private final Path root;

    public FsObjectStore(@NotNull ObjectStoreSettings settings, @NotNull ReplicationConfiguration configuration) {
        if (!ObjectStoreSettings.SCHEME_FS.equals(settings.getScheme())) {
            throw ObjectStoreException.fatal("object store settings are not for the filesystem store [scheme=" + settings.getScheme() + ']');
        }
        final String rootSetting = settings.get(ObjectStoreSettings.KEY_ROOT);
        final String atomicSetting = settings.get(ObjectStoreSettings.KEY_ATOMIC_WRITE_DIR);
        this.root = toPath(ObjectStoreSettings.KEY_ROOT, rootSetting);
        this.atomicWriteDir = atomicSetting != null
                ? toPath(ObjectStoreSettings.KEY_ATOMIC_WRITE_DIR, atomicSetting)
                : root.resolve(DEFAULT_ATOMIC_WRITE_DIR_NAME);
        if (root.startsWith(atomicWriteDir)) {
            throw ObjectStoreException.fatal("object store setting atomic_write_dir must not be root or contain root [root="
                    + root + ", atomic_write_dir=" + atomicWriteDir + ']');
        }
        this.lockFile = root.resolve(LOCK_FILE_NAME);
        this.conditionalWriteLock = ROOT_LOCKS.computeIfAbsent(lockFile, path -> new ReentrantLock());
        this.retryAttempts = Math.max(0, configuration.getRetryAttempts());
        this.retryInterval = Math.max(0, configuration.getRetryInterval());
        this.description = "fs root=" + root + ", atomic_write_dir=" + atomicWriteDir;
    }

    /**
     * ETag of a byte range: lower-case hex of the first 16 bytes of its SHA-256.
     */
    public static @NotNull String computeETag(byte @NotNull [] data, int offset, int length) {
        final MessageDigest digest = ObjectStoreUtils.newSha256();
        digest.update(data, offset, length);
        return ObjectStoreUtils.hex(digest.digest(), ETAG_BYTES);
    }

    @Override
    public void close() {
        // the store holds no open resources between calls
    }

    @Override
    public void delete(@NotNull String key) {
        final Path target = resolveKey(key);
        execute("DELETE", "key=" + key, () -> {
            // a directory is not an object: the key names nothing deletable
            if (!Files.isDirectory(target)) {
                Files.deleteIfExists(target);
            }
            return null;
        });
    }

    @Override
    public @Nullable String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink) {
        final Path target = resolveKey(key);
        final String expected = ObjectStoreUtils.stripQuotes(ifNoneMatch);
        return execute("GET", "key=" + key, () -> getOnce(key, target, expected, sink));
    }

    public @NotNull Path getAtomicWriteDir() {
        return atomicWriteDir;
    }

    @Override
    public int getDefaultCleanerMaxRequestsPerSecond() {
        return DEFAULT_CLEANER_MAX_REQUESTS_PER_SECOND;
    }

    @Override
    public @NotNull String getDescription() {
        return description;
    }

    public @NotNull Path getRoot() {
        return root;
    }

    @Override
    public boolean isIntegrityChecked() {
        return false;
    }

    @Override
    public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
        // A retry resumes after the last key the consumer saw, so no key is reported twice.
        final String[] resumeAfter = {startAfter};
        final ListConsumer tracking = (key, size) -> {
            resumeAfter[0] = key;
            return consumer.onKey(key, size);
        };
        execute("LIST", "prefix=" + prefix, () -> {
            listDirectory(root, "", prefix, resumeAfter[0], tracking);
            return null;
        });
    }

    @Override
    public @NotNull String put(
            @NotNull String key,
            byte @NotNull [] data,
            int offset,
            int length,
            @Nullable String ifMatch,
            boolean isIfNoneMatch
    ) {
        Objects.checkFromIndexSize(offset, length, data.length);
        if (ifMatch != null && isIfNoneMatch) {
            throw ObjectStoreException.fatal("ifMatch and isIfNoneMatch are mutually exclusive [key=" + key + ']');
        }
        final Path target = resolveKey(key);
        final String expected = ObjectStoreUtils.stripQuotes(ifMatch);
        execute("PUT", "key=" + key, () -> {
            putOnce(key, target, data, offset, length, expected, isIfNoneMatch);
            return null;
        });
        return computeETag(data, offset, length);
    }

    /**
     * Called after an attempt fails with an I/O error and before the next attempt. The default logs
     * the failure and sleeps {@code retryInterval} milliseconds; tests override it to change the
     * environment between attempts.
     *
     * @param operation GET, PUT, DELETE or LIST
     * @param subject   what the operation acts on, {@code key=<key>} or {@code prefix=<prefix>}
     * @param attempt   number of the attempt that failed, starting at 1
     */
    protected void onRetry(@NotNull String operation, @NotNull String subject, int attempt, @NotNull IOException error) {
        LOG.info().$("filesystem object store I/O failed, retrying [op=").$(operation)
                .$(", ").$(subject)
                .$(", attempt=").$(attempt)
                .$(", error=").$(error.toString())
                .I$();
        ObjectStoreUtils.pause(retryInterval);
    }

    // Moves surrogates above U+E000..U+FFFF, so UTF-16 units compare in code point order.
    private static int codePointOrder(char c) {
        if (c >= Character.MIN_SURROGATE) {
            return c >= 0xe000 ? c - 0x800 : c + 0x2000;
        }
        return c;
    }

    /**
     * Compares keys by Unicode code point, which is the UTF-8 byte order that S3 lists keys in.
     */
    private static int compareKeys(String a, String b) {
        final int n = Math.min(a.length(), b.length());
        for (int i = 0; i < n; i++) {
            final char ca = a.charAt(i);
            final char cb = b.charAt(i);
            if (ca != cb) {
                return codePointOrder(ca) - codePointOrder(cb);
            }
        }
        return a.length() - b.length();
    }

    private static String hashChannel(FileChannel channel) throws IOException {
        final MessageDigest digest = ObjectStoreUtils.newSha256();
        final ByteBuffer buffer = ByteBuffer.allocate(READ_CHUNK_SIZE);
        long position = 0;
        int n;
        while ((n = channel.read(buffer, position)) > -1) {
            position += n;
            buffer.flip();
            digest.update(buffer);
            buffer.clear();
        }
        return ObjectStoreUtils.hex(digest.digest(), ETAG_BYTES);
    }

    private static void readFully(FileChannel channel, long size, ByteSink sink) throws IOException {
        sink.clear();
        byte[] buffer = sink.ensureCapacity((int) size);
        long position = 0;
        while (true) {
            if (sink.size() == buffer.length) {
                buffer = sink.ensureCapacity(READ_CHUNK_SIZE);
            }
            final int n = channel.read(ByteBuffer.wrap(buffer, sink.size(), buffer.length - sink.size()), position);
            if (n < 0) {
                return;
            }
            sink.advance(n);
            position += n;
        }
    }

    private static void syncDirectory(Path dir) {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignore) {
            // best effort: some platforms cannot open or sync a directory
        }
    }

    private static Path toPath(String key, String value) {
        try {
            return Paths.get(value).toAbsolutePath().normalize();
        } catch (InvalidPathException e) {
            throw ObjectStoreException.fatal("object store setting is not a valid path [key=" + key + ", reason=" + e.getReason() + ']');
        }
    }

    private void createParentDirectories(String key, Path target) throws IOException {
        try {
            Files.createDirectories(target.getParent());
        } catch (FileAlreadyExistsException e) {
            // a path element exists and is not a directory: another object has that name
            throw ObjectStoreException.fatal("object key conflicts with an existing object [key=" + key + ']');
        }
    }

    private <T> T execute(String operation, String subject, IoAction<T> action) {
        for (int attempt = 0; ; attempt++) {
            try {
                return action.run();
            } catch (IOException e) {
                if (attempt >= retryAttempts) {
                    throw new ObjectStoreException(
                            ObjectStoreException.Kind.RETRYABLE,
                            0,
                            "object store I/O failed [op=" + operation + ", " + subject + ", attempts=" + (attempt + 1)
                                    + ", error=" + e + ']',
                            e
                    );
                }
                onRetry(operation, subject, attempt + 1, e);
            }
        }
    }

    private String getOnce(String key, Path target, String expected, ByteSink sink) throws IOException {
        if (Files.isDirectory(target)) {
            throw ObjectStoreException.notFound(key);
        }
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.READ)) {
            // Objects never change in place, so two passes over one open file see the same bytes.
            if (expected != null && expected.equals(hashChannel(channel))) {
                return null;
            }
            final long size = channel.size();
            if (size > MAX_OBJECT_SIZE) {
                throw ObjectStoreException.fatal("object too large to buffer [key=" + key + ", size=" + size + ']');
            }
            readFully(channel, size, sink);
            return computeETag(sink.data(), 0, sink.size());
        } catch (NoSuchFileException e) {
            throw ObjectStoreException.notFound(key);
        }
    }

    private boolean listDirectory(Path dir, String dirKey, String prefix, String startAfter, ListConsumer consumer) throws IOException {
        final ObjList<Entry> entries = new ObjList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path child : stream) {
                if (child.equals(lockFile) || child.equals(atomicWriteDir)) {
                    continue;
                }
                final BasicFileAttributes attributes;
                try {
                    attributes = Files.readAttributes(child, BasicFileAttributes.class);
                } catch (NoSuchFileException e) {
                    // deleted while listing, or a dangling symbolic link
                    continue;
                }
                final String name = child.getFileName().toString();
                if (attributes.isDirectory()) {
                    entries.add(new Entry(child, dirKey + name + '/', true, 0));
                } else if (attributes.isRegularFile()) {
                    entries.add(new Entry(child, dirKey + name, false, attributes.size()));
                }
            }
        } catch (NoSuchFileException | NotDirectoryException e) {
            // the root does not exist yet, or a directory went away while listing
            return true;
        }
        // A directory sorts by its name plus '/', so walking entries in this order yields keys in
        // lexicographic order: every key below a directory starts with that string.
        entries.sort((a, b) -> compareKeys(a.key, b.key));
        for (int i = 0, n = entries.size(); i < n; i++) {
            final Entry entry = entries.getQuick(i);
            if (entry.isDirectory) {
                final boolean isMatchPossible = entry.key.startsWith(prefix) || prefix.startsWith(entry.key);
                final boolean isAfterPossible = startAfter == null
                        || startAfter.startsWith(entry.key)
                        || compareKeys(entry.key, startAfter) > 0;
                if (isMatchPossible && isAfterPossible && !listDirectory(entry.path, entry.key, prefix, startAfter, consumer)) {
                    return false;
                }
            } else if (entry.key.startsWith(prefix)
                    && (startAfter == null || compareKeys(entry.key, startAfter) > 0)
                    && !consumer.onKey(entry.key, entry.size)) {
                return false;
            }
        }
        return true;
    }

    private void putOnce(
            String key,
            Path target,
            byte[] data,
            int offset,
            int length,
            String expected,
            boolean isIfNoneMatch
    ) throws IOException {
        if (Files.isDirectory(target)) {
            throw ObjectStoreException.fatal("object key names a directory [key=" + key + ']');
        }
        final Path temp = writeTempFile(data, offset, length);
        try {
            createParentDirectories(key, target);
            if (isIfNoneMatch) {
                try {
                    Files.createLink(target, temp);
                } catch (FileAlreadyExistsException e) {
                    throw ObjectStoreException.preconditionFailed(key);
                }
            } else if (expected != null) {
                conditionalWriteLock.lock();
                try (
                        FileChannel lockChannel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        FileLock ignore = lockChannel.lock()
                ) {
                    final String current;
                    try (FileChannel channel = FileChannel.open(target, StandardOpenOption.READ)) {
                        current = hashChannel(channel);
                    } catch (NoSuchFileException e) {
                        throw ObjectStoreException.preconditionFailed(key);
                    }
                    if (!expected.equals(current)) {
                        throw ObjectStoreException.preconditionFailed(key);
                    }
                    Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } finally {
                    conditionalWriteLock.unlock();
                }
            } else {
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            syncDirectory(target.getParent());
        } finally {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignore) {
                // a leftover temporary file wastes space but does not affect correctness
            }
        }
    }

    private Path resolveKey(String key) {
        ObjectStoreUtils.validateKey(key);
        final Path target;
        try {
            target = root.resolve(key);
        } catch (InvalidPathException e) {
            throw ObjectStoreException.fatal("object key is not a valid path [key=" + key + ", reason=" + e.getReason() + ']');
        }
        if (target.equals(lockFile) || target.startsWith(atomicWriteDir)) {
            throw ObjectStoreException.fatal("object key is reserved by the filesystem store [key=" + key + ']');
        }
        return target;
    }

    private Path writeTempFile(byte[] data, int offset, int length) throws IOException {
        Files.createDirectories(atomicWriteDir);
        final Path temp = atomicWriteDir.resolve("put-" + ProcessHandle.current().pid() + '-' + TEMP_FILE_COUNTER.incrementAndGet() + ".tmp");
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final ByteBuffer buffer = ByteBuffer.wrap(data, offset, length);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignore) {
                // the original failure matters more
            }
            throw e;
        }
        return temp;
    }

    @FunctionalInterface
    private interface IoAction<T> {
        T run() throws IOException;
    }

    private static final class Entry {
        final boolean isDirectory;
        final String key;
        final Path path;
        final long size;

        Entry(Path path, String key, boolean isDirectory, long size) {
            this.path = path;
            this.key = key;
            this.isDirectory = isDirectory;
            this.size = size;
        }
    }
}
