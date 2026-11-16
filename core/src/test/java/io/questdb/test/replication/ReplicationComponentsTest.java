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


package io.questdb.test.replication;

import io.questdb.cairo.TableToken;
import io.questdb.replication.ChecksumMode;
import io.questdb.replication.IndexClient;
import io.questdb.replication.ObjectIO;
import io.questdb.replication.ReplicaSegmentPins;
import io.questdb.replication.ReplicationException;
import io.questdb.replication.ReplicationWalDirectoryPolicy;
import io.questdb.replication.codec.IndexTable;
import io.questdb.replication.codec.NativeBuffer;
import io.questdb.replication.codec.ReplicationIndex;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class ReplicationComponentsTest extends AbstractTest {

    @Test
    public void testIndexClientConditionalWrites() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            try (IndexClient a = new IndexClient(store, true); IndexClient b = new IndexClient(store, false)) {
                Assert.assertNull(a.read());
                Assert.assertNull(a.getEtag());
                // readIfChanged without a version reads in full
                Assert.assertNull(a.readIfChanged());
                final ReplicationIndex idx = new ReplicationIndex();
                idx.setDataId(1, 2);
                idx.setOwner("a", 1, 10, 1, false);
                a.write(idx, 1);
                final String first = a.getEtag();
                Assert.assertNotNull(first);
                // a second writer that read nothing cannot create it again
                try {
                    b.write(idx, 1);
                    Assert.fail();
                } catch (ObjectStoreException e) {
                    Assert.assertEquals(ObjectStoreException.Kind.PRECONDITION_FAILED, e.getKind());
                }
                final ReplicationIndex read = b.read();
                Assert.assertNotNull(read);
                Assert.assertEquals("a", read.getOwnerInstanceId());
                Assert.assertNull(b.readIfChanged());
                // a commits again: b's version is stale, its write loses
                idx.setOwner("a", 1, 20, 2, false);
                a.write(idx, 1);
                Assert.assertNotEquals(first, a.getEtag());
                try {
                    b.write(read, 1);
                    Assert.fail();
                } catch (ObjectStoreException e) {
                    Assert.assertEquals(ObjectStoreException.Kind.PRECONDITION_FAILED, e.getKind());
                }
                final ReplicationIndex changed = b.readIfChanged();
                Assert.assertNotNull(changed);
                Assert.assertEquals(2, changed.getSequence());
                // the index disappears
                store.delete(IndexClient.INDEX_KEY);
                try {
                    b.readIfChanged();
                    Assert.fail();
                } catch (ObjectStoreException e) {
                    Assert.assertEquals(ObjectStoreException.Kind.NOT_FOUND, e.getKind());
                }
                store.setFaultHook((op, key) -> {
                    throw ObjectStoreException.retryable("down", null);
                });
                try {
                    a.read();
                    Assert.fail();
                } catch (ObjectStoreException e) {
                    Assert.assertTrue(e.isRetryable());
                }
            }
        });
    }

    @Test
    public void testObjectIo() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final MemoryObjectStore store = new MemoryObjectStore();
            Assert.assertTrue(ObjectIO.isChecksum(ChecksumMode.ALWAYS, store));
            Assert.assertFalse(ObjectIO.isChecksum(ChecksumMode.NEVER, store));
            Assert.assertFalse(ObjectIO.isChecksum(ChecksumMode.SERVICE_DEPENDENT, store));
            store.setIntegrityChecked(false);
            Assert.assertTrue(ObjectIO.isChecksum(ChecksumMode.SERVICE_DEPENDENT, store));
            try (ObjectIO io = new ObjectIO(store, 3, true); NativeBuffer raw = new NativeBuffer(); NativeBuffer out = new NativeBuffer()) {
                Assert.assertSame(store, io.getStore());
                final byte[] text = "hello replication, hello replication, hello replication, hello".getBytes(StandardCharsets.UTF_8);
                raw.putBytes(text, 0, text.length);
                Assert.assertNotNull(io.put("k", raw, true));
                // an immutable object that exists keeps its content
                Assert.assertNull(io.put("k", raw, true));
                Assert.assertNotNull(io.put("k", raw, false));
                Assert.assertTrue(io.getBytesUploaded() > 0);
                Assert.assertTrue(io.get("k", out));
                Assert.assertEquals(text.length, out.size());
                Assert.assertTrue(io.getBytesDownloaded() > 0);
                Assert.assertFalse(io.get("missing", out));
                store.putRaw("bad", new byte[]{1, 2, 3});
                try {
                    io.get("bad", out);
                    Assert.fail();
                } catch (ReplicationException e) {
                    Assert.assertEquals(ReplicationException.Kind.CORRUPT, e.getKind());
                    TestUtils.assertContains(e.getMessage(), "corrupt object [key=bad");
                }
                store.setFaultHook((op, key) -> {
                    throw ObjectStoreException.fatal("denied");
                });
                try {
                    io.get("k", out);
                    Assert.fail();
                } catch (ObjectStoreException e) {
                    Assert.assertEquals(ObjectStoreException.Kind.FATAL, e.getKind());
                }
                try {
                    io.put("k2", raw, true);
                    Assert.fail();
                } catch (ObjectStoreException e) {
                    Assert.assertEquals(ObjectStoreException.Kind.FATAL, e.getKind());
                }
            }
        });
    }

    @Test
    public void testPolicyMarkersAndPins() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String dir = root + File.separator + "policy";
            TestUtils.removeTestPath(dir);
            new File(dir, "t~1/wal3/2").mkdirs();
            new File(dir, "t~1/txn_seq").mkdirs();
            final TableToken table = new TableToken("t", "t~1", null, 1, TableToken.Type.TABLE, true, false, false, false);
            final TableToken liveView = new TableToken("lv", "lv~2", null, 2, TableToken.Type.LIVE_VIEW, true, false, false, false);
            Assert.assertTrue(ReplicationWalDirectoryPolicy.isReplicated(table, "sys."));
            Assert.assertFalse(ReplicationWalDirectoryPolicy.isReplicated(liveView, "sys."));
            final boolean[] pinned = {true};
            try (Path path = new Path();
                 ReplicationWalDirectoryPolicy primary = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, "sys.", (tableDir, partId) -> pinned[0] && "t~1".equals(tableDir) && partId > 1, null)) {
                path.of(dir).concat("t~1").concat("wal3").concat("2").slash();
                Assert.assertFalse(primary.isInUse(path));
                primary.initDirectory(path); // no token: no marker
                Assert.assertFalse(primary.isInUse(path));
                primary.initDirectory(path, liveView);
                Assert.assertFalse(primary.isInUse(path));
                primary.initDirectory(path, table);
                Assert.assertTrue(primary.isInUse(path));
                // without the trailing slash too
                path.of(dir).concat("t~1").concat("wal3").concat("2");
                Assert.assertTrue(primary.isInUse(path));
                primary.rollbackDirectory(path);
                Assert.assertFalse(primary.isInUse(path));
                primary.initDirectory(path, table);
                primary.removeMarker(path);
                Assert.assertFalse(primary.isInUse(path));
                path.of(dir).concat("t~1").concat("txn_seq");
                Assert.assertTrue(primary.isSeqPartInUse(path, 2));
                Assert.assertFalse(primary.isSeqPartInUse(path, 1));
                path.of(dir).concat("t~1").concat("txn_seq").slash();
                Assert.assertTrue(primary.isSeqPartInUse(path, 5));
                path.of("txn_seq");
                Assert.assertFalse(primary.isSeqPartInUse(path, 5));
                Assert.assertTrue(primary.truncateFilesOnClose());


                // the engine still consults the policy after the replication service closed it, and nobody
                // closes it a second time: the answers stay right, and the leak check proves that each
                // call frees what it allocates
                final ReplicationWalDirectoryPolicy closed = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, "sys.", (tableDir, partId) -> false, null);
                closed.close();
                path.of(dir).concat("t~1").concat("wal3").concat("2");
                Assert.assertFalse(closed.isInUse(path));
                closed.initDirectory(path, table);
                Assert.assertTrue(closed.isInUse(path));
                Assert.assertTrue(new File(dir, "t~1/wal3/2/" + ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER).exists());
                closed.rollbackDirectory(path);
                Assert.assertFalse(closed.isInUse(path));
                closed.initDirectory(path, table);
                closed.removeMarker(path);
                Assert.assertFalse(new File(dir, "t~1/wal3/2/" + ReplicationWalDirectoryPolicy.UPLOAD_PENDING_MARKER).exists());
            }
            final ReplicaSegmentPins pins = new ReplicaSegmentPins(1_000);
            try (Path path = new Path();
                 ReplicationWalDirectoryPolicy replica = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, "sys.", null, pins)) {
                pins.onDownloaded("t~1", 3, 2, 1_000);
                pins.onDownloaded("t~1", 3, 1, 1_001); // older segment does not replace the newer pin
                Assert.assertEquals(1, pins.size());
                Assert.assertTrue(pins.isPinned("t~1", 3, 2, 1_500));
                Assert.assertFalse(pins.isPinned("t~1", 3, 1, 1_500));
                Assert.assertFalse(pins.isPinned("t~1", 4, 2, 1_500));
                path.of(dir).concat("t~1").concat("wal3").concat("2");
                pins.onDownloaded("t~1", 3, 2, System.currentTimeMillis());
                Assert.assertTrue(replica.isInUse(path));
                path.of(dir).concat("t~1").concat("wal3").concat("2").slash();
                Assert.assertTrue(replica.isInUse(path));
                path.of(dir).concat("t~1").concat("wal3").concat("1");
                Assert.assertFalse(replica.isInUse(path));
                path.of(dir).concat("t~1").concat("txn_seq");
                Assert.assertFalse(replica.isInUse(path));
                path.of(dir).concat("t~1").concat("walx").concat("1");
                Assert.assertFalse(replica.isInUse(path));
                path.of(dir).concat("t~1").concat("wal3").concat("abc");
                Assert.assertFalse(replica.isInUse(path));
                path.of("2");
                Assert.assertFalse(replica.isInUse(path));
                path.of("/2");
                Assert.assertFalse(replica.isInUse(path));
                path.of(dir).concat("t~1").concat("txn_seq");
                Assert.assertFalse(replica.isSeqPartInUse(path, 0));
                replica.initDirectory(path, new TableToken("t", "t~1", null, 1, TableToken.Type.TABLE, true, false, false, false));
                replica.removeMarker(path);
                // an expired pin goes
                Assert.assertFalse(pins.isPinned("t~1", 3, 2, System.currentTimeMillis() + 5_000));
                Assert.assertEquals(0, pins.size());
            } finally {
                pins.close();
            }
            try (Path path = new Path(); ReplicationWalDirectoryPolicy none = new ReplicationWalDirectoryPolicy(FilesFacadeImpl.INSTANCE, "sys.", null, null)) {
                path.of(dir).concat("t~1").concat("wal3").concat("2");
                Assert.assertFalse(none.isInUse(path));
            }
        });
    }

    @Test
    public void testReplicationExceptionKinds() {
        Assert.assertEquals(ReplicationException.Kind.CORRUPT, ReplicationException.corrupt("a").getKind());
        Assert.assertEquals(ReplicationException.Kind.DIVERGENCE, ReplicationException.divergence("b").getKind());
        Assert.assertEquals(ReplicationException.Kind.GAP, ReplicationException.gap("c").getKind());
        Assert.assertEquals(ReplicationException.Kind.MISSING, ReplicationException.missing("d").getKind());
        Assert.assertEquals(ReplicationException.Kind.UNSUPPORTED, ReplicationException.unsupported("e").getKind());
        Assert.assertEquals("e", ReplicationException.unsupported("e").getMessage());
        final IndexTable t = new IndexTable().of(1, 2, "d", "n", IndexTable.KIND_TABLE, 3);
        Assert.assertEquals("1_2", t.getKey());
    }
}
