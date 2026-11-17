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

import io.questdb.replication.ReplicationConfigurationImpl;
import io.questdb.replication.store.FsObjectStore;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreFactory;
import io.questdb.replication.store.ObjectStoreSettings;
import io.questdb.replication.store.s3.S3CredentialsProvider;
import io.questdb.replication.store.s3.S3ObjectStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Map;

import static io.questdb.test.replication.store.FsObjectStoreTest.assertFails;

public class ObjectStoreFactoryTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private final ReplicationConfigurationImpl configuration = new ReplicationConfigurationImpl();

    @Test
    public void testCreatesFilesystemStore() throws Exception {
        final Path root = temp.newFolder("store").toPath();
        try (ObjectStore store = ObjectStoreFactory.create(ObjectStoreSettings.parse("fs::root=" + root + ';'), configuration, null)) {
            Assert.assertEquals(FsObjectStore.class, store.getClass());
            Assert.assertEquals("fs root=" + root + ", atomic_write_dir=" + root.resolve(".tmp"), store.getDescription());
        }
    }

    @Test
    public void testCreatesS3Store() {
        try (ObjectStore store = ObjectStoreFactory.create(
                ObjectStoreSettings.parse("s3::bucket=b;region=eu-west-1;access_key_id=AK;secret_access_key=SK;"), configuration, null)) {
            Assert.assertEquals(S3ObjectStore.class, store.getClass());
            Assert.assertEquals("s3 bucket=b, region=eu-west-1, endpoint=https://b.s3.eu-west-1.amazonaws.com, style=virtual-host", store.getDescription());
        }
        // a null environment is an empty one: no keys there, so the chain falls through to the metadata service
        try (ObjectStore store = ObjectStoreFactory.create(ObjectStoreSettings.parse("s3::bucket=b;region=eu-west-1;"), configuration, null)) {
            Assert.assertEquals(S3ObjectStore.class, store.getClass());
        }
    }

    @Test
    public void testPassesEnvironmentToS3Store() {
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together",
                () -> ObjectStoreFactory.create(
                        ObjectStoreSettings.parse("s3::bucket=b;region=eu-west-1;"),
                        configuration,
                        Map.of(S3CredentialsProvider.ENV_ACCESS_KEY_ID, "AK")
                )
        );
        assertFails(
                ObjectStoreException.Kind.FATAL,
                0,
                "object store setting region is required when endpoint is not set",
                () -> ObjectStoreFactory.create(ObjectStoreSettings.parse("s3::bucket=b;"), configuration, Map.of())
        );
    }
}
