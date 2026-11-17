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

import io.questdb.replication.store.ObjectStoreSettings;
import org.junit.Assert;
import org.junit.Test;

public class ObjectStoreSettingsTest {

    @Test
    public void testEmptySettingsAreSkipped() {
        // empty and blank settings between the separators carry nothing
        final ObjectStoreSettings s = ObjectStoreSettings.parse("s3::;bucket=b;; ;region=eu-west-1;;");
        Assert.assertEquals("b", s.get("bucket"));
        Assert.assertEquals("eu-west-1", s.get("region"));
        Assert.assertEquals("s3::bucket=b;region=eu-west-1;", s.toString());
    }

    @Test
    public void testFsSettings() {
        final ObjectStoreSettings s = ObjectStoreSettings.parse(" FS:: root = /nfs/final ; atomic_write_dir=/nfs/scratch; ");
        Assert.assertEquals("fs", s.getScheme());
        Assert.assertEquals("/nfs/final", s.get("root"));
        Assert.assertEquals("/nfs/scratch", s.get("ATOMIC_WRITE_DIR"));
        Assert.assertTrue(s.has("root"));
        Assert.assertFalse(s.has("bucket"));
        Assert.assertNull(s.get("bucket"));
        Assert.assertEquals("fs::root=/nfs/final;atomic_write_dir=/nfs/scratch;", s.toString());
    }

    @Test
    public void testIncompletePlaceholderIsAValue() {
        // only a value that both starts with ${ and ends with } reads as an unexpanded placeholder
        final ObjectStoreSettings s = ObjectStoreSettings.parse("fs::root=${HOME;atomic_write_dir=/scratch}");
        Assert.assertEquals("${HOME", s.get("root"));
        Assert.assertEquals("/scratch}", s.get("atomic_write_dir"));
    }

    @Test
    public void testInvalidStrings() {
        assertInvalid(null, "object store string is empty");
        assertInvalid("   ", "object store string is empty");
        assertInvalid("s3:bucket=b", "object store string must start with '<scheme>::', for example 's3::' or 'fs::'");
        assertInvalid("::bucket=b", "object store string must start with '<scheme>::', for example 's3::' or 'fs::'");
        assertInvalid("ftp::root=x", "unknown object store scheme [scheme=ftp]");
        assertInvalid("gcs::bucket=x", "object store scheme is not supported in this build [scheme=gcs]");
        assertInvalid("azblob::container=x", "object store scheme is not supported in this build [scheme=azblob]");
        assertInvalid("s3::region=x;", "object store setting is required [scheme=s3, key=bucket]");
        assertInvalid("fs::atomic_write_dir=/x;", "object store setting is required [scheme=fs, key=root]");
        assertInvalid("s3::bucket=b;bucket=c;", "duplicate object store setting [key=bucket]");
        assertInvalid("s3::bucket=b;Bucket=c;", "duplicate object store setting [key=bucket]");
        assertInvalid("s3::bucket=;", "object store setting has an empty value [key=bucket]");
        assertInvalid("s3::bucket=b;=x;", "object store setting must be 'key=value' [setting=<2 chars>]");
        assertInvalid("s3::bucket=b;nonsense;", "object store setting must be 'key=value' [setting=<8 chars>]");
        assertInvalid("s3::bucket=b;root=/a;atomic_write_dir=/b;", "unknown object store setting [scheme=s3, key=atomic_write_dir]");
        assertInvalid("fs::root=/a;bucket=b;", "unknown object store setting [scheme=fs, key=bucket]");
        assertInvalid("s3::bucket=${BUCKET_NAME};", "object store setting holds an unexpanded placeholder [key=bucket]");
        assertInvalid("s3::bucket=b;access_key_id=AK;", "object store settings access_key_id and secret_access_key must be set together");
        assertInvalid("s3::bucket=b;secret_access_key=SK;", "object store settings access_key_id and secret_access_key must be set together");
        assertInvalid("s3::bucket=b;session_token=T;", "object store setting session_token needs access_key_id and secret_access_key");
        assertInvalid("s3::bucket=b;enable_virtual_host_style=yes;", "object store setting enable_virtual_host_style must be true or false");
    }

    @Test
    public void testS3SettingsAndMasking() {
        final ObjectStoreSettings s = ObjectStoreSettings.parse(
                "s3::bucket=my-bucket;root=db1;region=eu-west-1;endpoint=http://127.0.0.1:9000;"
                        + "access_key_id=AKIAEXAMPLE;secret_access_key=SECRET/KEY=;session_token=TOKEN;enable_virtual_host_style=TRUE"
        );
        Assert.assertEquals("s3", s.getScheme());
        Assert.assertEquals("my-bucket", s.get("bucket"));
        Assert.assertEquals("db1", s.get("root"));
        Assert.assertEquals("eu-west-1", s.get("region"));
        Assert.assertEquals("http://127.0.0.1:9000", s.get("endpoint"));
        Assert.assertEquals("AKIAEXAMPLE", s.get("access_key_id"));
        // a value may contain '=' and '/'
        Assert.assertEquals("SECRET/KEY=", s.get("secret_access_key"));
        Assert.assertEquals("TOKEN", s.get("session_token"));
        Assert.assertEquals("TRUE", s.get("enable_virtual_host_style"));
        final String text = s.toString();
        Assert.assertEquals(
                "s3::bucket=my-bucket;root=db1;region=eu-west-1;endpoint=http://127.0.0.1:9000;"
                        + "access_key_id=****;secret_access_key=****;session_token=****;enable_virtual_host_style=TRUE;",
                text
        );
        Assert.assertFalse(text.contains("SECRET"));
        Assert.assertFalse(text.contains("AKIAEXAMPLE"));
    }

    @Test
    public void testSchemeOnly() {
        // the bucket is mandatory for S3, the root for the filesystem
        assertInvalid("s3::", "object store setting is required [scheme=s3, key=bucket]");
        final ObjectStoreSettings s = ObjectStoreSettings.parse("s3::bucket=b");
        Assert.assertEquals("s3::bucket=b;", s.toString());
    }

    private static void assertInvalid(String text, String message) {
        try {
            ObjectStoreSettings.parse(text);
            Assert.fail("expected failure for " + text);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }
}
