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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.IDGenerator;
import io.questdb.cairo.IDGeneratorFactory;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class IDGeneratorTest extends AbstractCairoTest {

    @Test
    public void testAdvanceTo() throws Exception {
        assertMemoryLeak(() -> {
            for (int commitMode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC, CommitMode.SYNC}) {
                final String fileName = "id_generator_advance_" + commitMode + ".test";
                final CairoConfiguration config = new CairoConfigurationWrapper(configuration) {
                    @Override
                    public int getCommitMode() {
                        return commitMode;
                    }
                };
                try (IDGenerator generator = IDGeneratorFactory.newIDGenerator(config, fileName, 16)) {
                    generator.open();
                    Assert.assertEquals(1, generator.getNextId());
                    // replication raises the floor to the primary's highest id
                    generator.advanceTo(100);
                    Assert.assertEquals(100, generator.getCurrentId());
                    Assert.assertEquals(101, generator.getNextId());
                    // never lowers it
                    generator.advanceTo(50);
                    Assert.assertEquals(102, generator.getNextId());
                    generator.advanceTo(102);
                    Assert.assertEquals(103, generator.getNextId());
                }
                // the floor survives a reopen
                try (IDGenerator generator = IDGeneratorFactory.newIDGenerator(config, fileName, 16)) {
                    generator.open();
                    Assert.assertTrue("commitMode=" + commitMode, generator.getNextId() > 103);
                }
            }
        });
    }

    @Test
    public void testAdvanceToConcurrentWithNextId() throws Exception {
        assertMemoryLeak(() -> {
            for (int commitMode : new int[]{CommitMode.NOSYNC, CommitMode.SYNC}) {
                final CairoConfiguration config = new CairoConfigurationWrapper(configuration) {
                    @Override
                    public int getCommitMode() {
                        return commitMode;
                    }
                };
                try (IDGenerator generator = IDGeneratorFactory.newIDGenerator(config, "id_generator_race_" + commitMode + ".test", 8)) {
                    generator.open();
                    final int count = 20_000;
                    final CyclicBarrier start = new CyclicBarrier(2);
                    final AtomicInteger errors = new AtomicInteger();
                    final Thread advancer = new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 1; i <= count; i++) {
                                generator.advanceTo(i * 2L);
                            }
                        } catch (Throwable th) {
                            errors.incrementAndGet();
                        }
                    });
                    advancer.start();
                    final LongList ids = new LongList();
                    start.await();
                    for (int i = 0; i < count; i++) {
                        ids.add(generator.getNextId());
                    }
                    advancer.join();
                    Assert.assertEquals(0, errors.get());
                    // ids stay unique and increasing, and the floor holds
                    for (int i = 1, n = ids.size(); i < n; i++) {
                        Assert.assertTrue(ids.getQuick(i) > ids.getQuick(i - 1));
                    }
                    Assert.assertTrue(generator.getCurrentId() >= count * 2L);
                    Assert.assertTrue(generator.getNextId() > count * 2L);
                }
            }
        });
    }

    @Test
    public void testASyncMode() throws Exception {
        assertMemoryLeak(() -> {
            Set<Long> set = testGenerateId(configuration, CommitMode.ASYNC, 256, 5000);
            Assert.assertEquals(10000, set.size());
        });
    }

    @Test
    public void testGetCurrentAsync() {
        testGetCurrent(CommitMode.ASYNC);
    }

    @Test
    public void testGetCurrentNosync() {
        testGetCurrent(CommitMode.NOSYNC);
    }

    @Test
    public void testGetCurrentSync() {
        testGetCurrent(CommitMode.SYNC);
    }

    @Test
    public void testNoSyncMode() throws Exception {
        assertMemoryLeak(() -> {
            Set<Long> set = testGenerateId(configuration, CommitMode.NOSYNC, 1, 5000);
            Assert.assertEquals(10000, set.size());
        });
    }

    @Test
    public void testSwitchMode() throws Exception {
        assertMemoryLeak(() -> {
            int count = 10000;
            Set<Long> set1 = testGenerateId(configuration, CommitMode.SYNC, 512, count);
            Assert.assertEquals(count * 2, set1.size());
            Set<Long> set2 = testGenerateId(configuration, CommitMode.NOSYNC, 1, count);
            Assert.assertEquals(count * 2, set2.size());
            set1.addAll(set2);
            Assert.assertEquals(count * 4, set1.size());
            Set<Long> set3 = testGenerateId(configuration, CommitMode.ASYNC, 100, count);
            Assert.assertEquals(count * 2, set3.size());
            set1.addAll(set3);
            Assert.assertTrue(set1.size() <= count * 6);
        });
    }

    @Test
    public void testSyncMode() throws Exception {
        assertMemoryLeak(() -> {
            Set<Long> set = testGenerateId(configuration, CommitMode.SYNC, 512, 5000);
            Assert.assertEquals(10000, set.size());
        });
    }

    @Test
    public void testSyncModeDoesNotReissueIdsClaimedAfterOpen() throws Exception {
        // A batching generator caches its watermark at open(), but the file it caches from is not
        // private to it. A replica's _wal_index.d advances by replication, recording ids the primary
        // allocated, while the replica's own generator was opened earlier against a file that still
        // read 0. Promote that replica and the stale watermark hands out ids the old primary already
        // used: the promoted node writes into a WAL directory that still exists on its peer, the wal
        // lock is refused, and replication stalls one transaction short indefinitely.
        //
        // The non-batching generator is immune -- it re-reads the shared mapping on every call -- so
        // this only bites once a syncing commit mode is in force.
        assertMemoryLeak(() -> {
            final String fileName = "id_generator_claimed.test";
            final CairoConfiguration syncConfig = new CairoConfigurationWrapper(configuration) {
                @Override
                public int getCommitMode() {
                    return CommitMode.SYNC;
                }
            };
            final CairoConfiguration claimerConfig = new CairoConfigurationWrapper(configuration) {
                @Override
                public int getCommitMode() {
                    return CommitMode.NOSYNC;
                }
            };

            try (IDGenerator generator = IDGeneratorFactory.newIDGenerator(syncConfig, fileName, 512)) {
                generator.open();

                // Someone else claims ids and records that in the very file this generator reads its
                // watermark from -- exactly what replication does to a replica's _wal_index.d.
                long claimed = 0;
                try (IDGenerator claimer = IDGeneratorFactory.newIDGenerator(claimerConfig, fileName, 1)) {
                    claimer.open();
                    for (int i = 0; i < 8; i++) {
                        claimed = claimer.getNextId();
                    }
                }

                final long next = generator.getNextId();
                Assert.assertTrue(
                        "a generator must not reissue an id already claimed in its own watermark file"
                                + " [claimed=" + claimed + ", issued=" + next + ']',
                        next > claimed
                );
            }
        });
    }

    private Set<Long> testGenerateId(CairoConfiguration configuration, int commitMode, int step, int count) throws Exception {
        try (
                IDGenerator idGenerator = IDGeneratorFactory.newIDGenerator(
                        new CairoConfigurationWrapper(configuration) {
                            @Override
                            public int getCommitMode() {
                                return commitMode;
                            }
                        }, "id_generator.test", step
                )
        ) {
            idGenerator.open();
            final LongList listA = new LongList();
            final LongList listB = new LongList();
            final CyclicBarrier startBarrier = new CyclicBarrier(2);
            final AtomicInteger errors = new AtomicInteger();

            final Thread th = new Thread(() -> {
                try {
                    startBarrier.await();
                    for (int i = 0; i < count; i++) {
                        listA.add(idGenerator.getNextId());
                    }
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                }
            });
            th.start();

            try {
                startBarrier.await();
                for (int i = 0; i < count; i++) {
                    listB.add(idGenerator.getNextId());
                }
                th.join();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
                errors.incrementAndGet();
            }

            Set<Long> set = new HashSet<>();
            // add both arrays to the set and assert that there are no duplicates
            for (int i = 0, n = listA.size(); i < n; i++) {
                Assert.assertTrue(set.add(listA.getQuick(i)));
            }
            for (int i = 0, n = listB.size(); i < n; i++) {
                Assert.assertTrue(set.add(listB.getQuick(i)));
            }
            return set;
        }
    }

    private void testGetCurrent(int commitMode) {
        try (
                IDGenerator idGenerator = IDGeneratorFactory.newIDGenerator(
                        new CairoConfigurationWrapper(configuration) {
                            @Override
                            public int getCommitMode() {
                                return commitMode;
                            }
                        }, "get_current.test", 7
                )
        ) {
            idGenerator.open();
            Assert.assertEquals(0, idGenerator.getCurrentId());
            Assert.assertEquals(1, idGenerator.getNextId());
            Assert.assertEquals(1, idGenerator.getCurrentId());
            Assert.assertEquals(2, idGenerator.getNextId());
            Assert.assertEquals(2, idGenerator.getCurrentId());
            Assert.assertEquals(3, idGenerator.getNextId());
            Assert.assertEquals(3, idGenerator.getCurrentId());
        }
    }
}