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

package io.questdb.test.replication.store;

import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.replication.store.FsObjectStoreTest.assertFails;
import static io.questdb.test.replication.store.FsObjectStoreTest.bytes;
import static io.questdb.test.replication.store.FsObjectStoreTest.utf8;

public class ObjectStoreUtilsTest {

    @Test
    public void testByteSinkRejectsObjectsTooLargeToBuffer() {
        final ByteSink sink = new ByteSink(16);
        sink.put(bytes("abc"), 0, 3);
        final byte[] data = sink.data();
        // a byte array holds at most Integer.MAX_VALUE - 8 bytes: 3 + (MAX - 10) is one more
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object too large to buffer [size=" + (Integer.MAX_VALUE - 7L) + ']',
                () -> sink.ensureCapacity(Integer.MAX_VALUE - 10));
        // the size is computed in long arithmetic, so it does not wrap around past Integer.MAX_VALUE
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object too large to buffer [size=2147483650]",
                () -> sink.ensureCapacity(Integer.MAX_VALUE));
        // the sink neither grew nor lost its content, and still works
        Assert.assertSame(data, sink.data());
        Assert.assertEquals("abc", utf8(sink));
        sink.put(bytes("def"), 0, 3);
        Assert.assertEquals("abcdef", utf8(sink));
    }

    @Test
    public void testHex() {
        Assert.assertEquals("00ff7f80", ObjectStoreUtils.hex(new byte[]{0, -1, 127, -128, 5}, 4));
        Assert.assertEquals("", ObjectStoreUtils.hex(new byte[]{1}, 0));
    }

    @Test
    public void testPause() {
        ObjectStoreUtils.pause(0);
        Assert.assertFalse(Thread.currentThread().isInterrupted());
        Thread.currentThread().interrupt();
        try {
            final ObjectStoreException e = assertFails(ObjectStoreException.Kind.RETRYABLE, 0, "object store request interrupted",
                    () -> ObjectStoreUtils.pause(10_000));
            Assert.assertTrue(e.getCause() instanceof InterruptedException);
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testSha256() {
        // FIPS 180-2 test vector for "abc"
        Assert.assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", ObjectStoreUtils.sha256Hex(bytes("xabcx"), 1, 3));
        Assert.assertEquals("SHA-256", ObjectStoreUtils.newSha256().getAlgorithm());
    }

    @Test
    public void testStripQuotes() {
        Assert.assertNull(ObjectStoreUtils.stripQuotes(null));
        Assert.assertEquals("abc", ObjectStoreUtils.stripQuotes("\"abc\""));
        Assert.assertEquals("", ObjectStoreUtils.stripQuotes("\"\""));
        Assert.assertEquals("\"", ObjectStoreUtils.stripQuotes("\""));
        Assert.assertEquals("abc", ObjectStoreUtils.stripQuotes("abc"));
        Assert.assertEquals("\"abc", ObjectStoreUtils.stripQuotes("\"abc"));
        Assert.assertEquals("abc\"", ObjectStoreUtils.stripQuotes("abc\""));
        Assert.assertEquals("", ObjectStoreUtils.stripQuotes(""));
    }

    @Test
    public void testValidateKey() {
        for (String key : new String[]{"a", "a/b", ".a", "a.", "...", "a/.b/c..", "x y/\u00e9"}) {
            ObjectStoreUtils.validateKey(key);
        }
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object key is empty", () -> ObjectStoreUtils.validateKey(""));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object key must be relative [key=/a]", () -> ObjectStoreUtils.validateKey("/a"));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object key contains a backslash or NUL character [key=a\\b]", () -> ObjectStoreUtils.validateKey("a\\b"));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object key contains a backslash or NUL character [key=a?]", () -> ObjectStoreUtils.validateKey("a\u0000"));
        for (String key : new String[]{"a//b", "a/", ".", "..", "./a", "a/.", "../a", "a/..", "a/./b", "a/../b"}) {
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key has an empty, '.' or '..' segment [key=" + key + ']',
                    () -> ObjectStoreUtils.validateKey(key));
        }
    }
}
