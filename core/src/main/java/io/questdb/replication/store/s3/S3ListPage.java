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

package io.questdb.replication.store.s3;

import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One page of an S3 ListObjectsV2 response: the keys and sizes of its {@code Contents} entries,
 * whether more pages follow, and the token that requests the next page.
 */
public final class S3ListPage {
    private final ObjList<String> keys = new ObjList<>();
    private final LongList sizes = new LongList();
    private boolean isTruncated;
    private String nextContinuationToken;

    /**
     * Parses a ListObjectsV2 response body.
     *
     * @throws IllegalArgumentException when the body is not a well-formed ListBucketResult
     */
    public static @NotNull S3ListPage parse(@NotNull String xml) {
        final S3ListPage page = new S3ListPage();
        final String[] key = {null};
        final long[] size = {-1};
        final boolean[] isListResult = {false};
        S3XmlParser.parse(xml, new S3XmlParser.Handler() {
            @Override
            public void onEndElement(@NotNull String name) {
                if ("Contents".equals(name)) {
                    if (key[0] == null || size[0] < 0) {
                        throw new IllegalArgumentException("ListObjectsV2 entry lacks Key or Size");
                    }
                    page.keys.add(key[0]);
                    page.sizes.add(size[0]);
                    key[0] = null;
                    size[0] = -1;
                } else if ("ListBucketResult".equals(name)) {
                    isListResult[0] = true;
                }
            }

            @Override
            public void onLeafElement(@Nullable String parent, @NotNull String name, @NotNull String text) {
                if ("Contents".equals(parent)) {
                    if ("Key".equals(name)) {
                        key[0] = text;
                    } else if ("Size".equals(name)) {
                        try {
                            size[0] = Long.parseLong(text.trim());
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("ListObjectsV2 entry has an invalid Size");
                        }
                    }
                } else if ("ListBucketResult".equals(parent)) {
                    if ("IsTruncated".equals(name)) {
                        page.isTruncated = "true".equals(text.trim());
                    } else if ("NextContinuationToken".equals(name)) {
                        page.nextContinuationToken = text;
                    }
                } else if (parent == null && "ListBucketResult".equals(name)) {
                    // an empty result written as a single element
                    isListResult[0] = true;
                }
            }
        });
        if (!isListResult[0]) {
            throw new IllegalArgumentException("response is not a ListBucketResult");
        }
        if (page.isTruncated && (page.nextContinuationToken == null || page.nextContinuationToken.isEmpty())) {
            throw new IllegalArgumentException("truncated ListObjectsV2 response lacks NextContinuationToken");
        }
        return page;
    }

    public @NotNull String getKey(int index) {
        return keys.getQuick(index);
    }

    public @Nullable String getNextContinuationToken() {
        return nextContinuationToken;
    }

    public long getSize(int index) {
        return sizes.getQuick(index);
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public int size() {
        return keys.size();
    }
}
