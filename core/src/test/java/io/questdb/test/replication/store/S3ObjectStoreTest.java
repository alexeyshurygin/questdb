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
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreSettings;
import io.questdb.replication.store.s3.S3CredentialsProvider;
import io.questdb.replication.store.s3.S3ObjectStore;
import io.questdb.replication.store.s3.S3RequestSigner;
import io.questdb.std.Os;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.replication.store.FakeS3Server.ACCESS_KEY_ID;
import static io.questdb.test.replication.store.FakeS3Server.BUCKET;
import static io.questdb.test.replication.store.FakeS3Server.Fault;
import static io.questdb.test.replication.store.FakeS3Server.SECRET_ACCESS_KEY;
import static io.questdb.test.replication.store.FakeS3Server.md5Hex;
import static io.questdb.test.replication.store.FakeS3Server.sha256Hex;
import static io.questdb.test.replication.store.FsObjectStoreTest.assertFails;
import static io.questdb.test.replication.store.FsObjectStoreTest.assertOneWinner;
import static io.questdb.test.replication.store.FsObjectStoreTest.bytes;
import static io.questdb.test.replication.store.FsObjectStoreTest.list;
import static io.questdb.test.replication.store.FsObjectStoreTest.race;
import static io.questdb.test.replication.store.FsObjectStoreTest.utf8;

public class S3ObjectStoreTest {
    private static final String KEYS = ";access_key_id=" + ACCESS_KEY_ID + ";secret_access_key=" + SECRET_ACCESS_KEY + ';';
    private ReplicationConfigurationImpl configuration;
    private FakeImdsServer imds;
    private FakeS3Server s3;

    @Before
    public void setUp() throws IOException {
        s3 = new FakeS3Server();
        configuration = new ReplicationConfigurationImpl()
                .setRetryAttempts(2)
                .setRetryInterval(1)
                .setRequestBaseTimeout(2_000);
    }

    @After
    public void tearDown() throws IOException {
        if (s3 != null) {
            s3.close();
        }
        if (imds != null) {
            imds.close();
        }
    }

    @Test
    public void testBodyAfterFailedSubscribeIsIgnored() throws Exception {
        // The response announces more than a byte array holds, so the body subscriber cancels in
        // onSubscribe. A buffer the client delivers after the cancel must not reach the sink.
        configuration.setRetryAttempts(0);
        final ScriptedHttpClient client = new ScriptedHttpClient();
        final AtomicInteger cancels = new AtomicInteger();
        client.add(exchange -> {
            exchange.respond(200, Map.of("Content-Length", "3000000000", "ETag", "\"e1\""));
            exchange.subscribe(cancels);
            exchange.next(ByteBuffer.wrap(bytes("late")));
            exchange.finish();
        });
        try (S3ObjectStore store = newScriptedStore(client)) {
            final ByteSink sink = new ByteSink();
            sink.put(bytes("old"), 0, 3);
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object too large to buffer [key=k, size=3000000000]", () -> store.get("k", null, sink));
            client.awaitScripts();
            Assert.assertEquals("old", utf8(sink));
            // once in onSubscribe, and again for the buffer that came after it
            Assert.assertEquals(2, cancels.get());
        }
    }

    @Test
    public void testBodyTooLargeWithoutContentLength() throws Exception {
        Assume.assumeFalse("Windows cannot delete a file while it is mapped", Os.isWindows());
        configuration.setRetryAttempts(0);
        final Path file = Files.createTempFile("s3-body", ".bin");
        try {
            final MappedByteBuffer huge;
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                // a sparse file maps to a buffer of Integer.MAX_VALUE bytes that takes neither memory nor disk
                channel.write(ByteBuffer.wrap(new byte[]{1}), Integer.MAX_VALUE - 1L);
                huge = channel.map(FileChannel.MapMode.READ_ONLY, 0, Integer.MAX_VALUE);
            }
            final ScriptedHttpClient client = new ScriptedHttpClient();
            final AtomicInteger cancels = new AtomicInteger();
            client.add(exchange -> {
                // without a Content-Length the size shows only as the body arrives, here in one buffer
                exchange.respond(200, Map.of("ETag", "\"e1\""));
                exchange.subscribe(cancels);
                exchange.next(huge);
                exchange.finish();
            });
            try (S3ObjectStore store = newScriptedStore(client)) {
                assertFails(ObjectStoreException.Kind.FATAL, 0, "object too large to buffer [size=2147483647]", () -> store.get("k", null, new ByteSink()));
                client.awaitScripts();
                Assert.assertEquals(1, cancels.get());
            }
        } finally {
            Files.delete(file);
        }
    }

    @Test
    public void testBodyWithoutContentLength() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            // bodies that end when the server closes the connection
            s3.addFault("GET", Fault.raw("HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nConnection: close\r\n\r\nhello"));
            final ByteSink sink = new ByteSink();
            Assert.assertEquals("e1", store.get("k", null, sink));
            Assert.assertEquals("hello", utf8(sink));
            s3.addFault("GET", Fault.raw("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n<Error><Code>AccessDenied</Code></Error>"));
            assertFails(ObjectStoreException.Kind.FATAL, 403, "object store request failed [op=GET, key=k, status=403, code=AccessDenied]",
                    () -> store.get("k", null, sink));
            Assert.assertEquals("hello", utf8(sink));
        }
    }

    @Test
    public void testCloseKeepsSharedClientOpen() throws Exception {
        final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        try {
            final S3ObjectStore first = new S3ObjectStore(settings(""), configuration, Map.of(), client);
            first.put("k", bytes("1"), 0, 1, null, false);
            first.close();
            try (S3ObjectStore second = new S3ObjectStore(settings(""), configuration, Map.of(), client)) {
                Assert.assertEquals(md5Hex(bytes("2")), second.put("k", bytes("2"), 0, 1, null, false));
            }
        } finally {
            client.shutdownNow();
        }
    }

    @Test
    public void testCredentialsFailureLeavesSharedClientOpen() throws Exception {
        final HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        try {
            assertFails(ObjectStoreException.Kind.FATAL, 0, "environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together",
                    () -> new S3ObjectStore(
                            ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint() + ';'),
                            configuration,
                            Map.of(S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, "SK"),
                            client
                    ));
            try (S3ObjectStore store = new S3ObjectStore(settings(""), configuration, Map.of(), client)) {
                Assert.assertEquals(md5Hex(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            }
        } finally {
            client.shutdownNow();
        }
    }

    @Test
    public void testCloseShutsDownOwnClient() throws Exception {
        final S3ObjectStore store = newStore("");
        store.put("k", bytes("1"), 0, 1, null, false);
        store.close();
        final ObjectStoreException e = assertRetryableWithPrefix(
                "object store request failed [op=PUT, key=k, attempts=3, reason=I/O error [error=java.io.IOException",
                () -> store.put("k", bytes("2"), 0, 1, null, false)
        );
        Assert.assertTrue(e.getCause().getCause() instanceof IOException);
        Assert.assertEquals(1, s3.getRequests().size());
    }

    @Test
    public void testConcurrentIfMatchCounter() throws Exception {
        s3.setKeepAlive(true);
        final int threads = 4;
        final int increments = 10;
        try (S3ObjectStore store = newStore("")) {
            store.put("counter", bytes("0"), 0, 1, null, true);
            final CyclicBarrier start = new CyclicBarrier(threads);
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final Thread worker = new Thread(() -> {
                    try {
                        start.await();
                        final ByteSink sink = new ByteSink();
                        // a broken If-Match fails every write; give up instead of spinning
                        for (int i = 0, attempts = 0; i < increments; ) {
                            Assert.assertTrue("too many conflicts", ++attempts < 1_000);
                            final String etag = store.get("counter", null, sink);
                            final byte[] next = bytes(String.valueOf(Integer.parseInt(utf8(sink)) + 1));
                            try {
                                store.put("counter", next, 0, next.length, etag, false);
                                i++;
                            } catch (ObjectStoreException e) {
                                Assert.assertEquals(ObjectStoreException.Kind.PRECONDITION_FAILED, e.getKind());
                            }
                        }
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            Assert.assertNull(failure.get());
            Assert.assertEquals(String.valueOf(threads * increments), new String(s3.getObject("counter")));
            Assert.assertEquals(0, s3.getSignatureFailures());
        }
    }

    @Test
    public void testConcurrentIfMatchOneWinner() throws Exception {
        s3.setKeepAlive(true);
        try (S3ObjectStore store = newStore("")) {
            for (int round = 0; round < 20; round++) {
                final String key = "race/" + round;
                final String base = store.put(key, bytes("base"), 0, 4, null, false);
                final String[] winners = race(
                        () -> store.put(key, bytes("one"), 0, 3, base, false),
                        () -> store.put(key, bytes("two"), 0, 3, '"' + base + '"', false)
                );
                assertOneWinner(store, key, winners, "one", "two");
            }
        }
    }

    @Test
    public void testConcurrentIfNoneMatchOneWinner() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            for (int round = 0; round < 20; round++) {
                final String key = "race/" + round;
                final String[] winners = race(
                        () -> store.put(key, bytes("one"), 0, 3, null, true),
                        () -> store.put(key, bytes("two"), 0, 3, null, true)
                );
                assertOneWinner(store, key, winners, "one", "two");
            }
        }
    }

    @Test
    public void testConfigurationErrors() {
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object store setting region is required when endpoint is not set",
                () -> newStoreFromSettings("s3::bucket=b" + KEYS));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object store setting region is not a valid AWS region [region=EU_WEST]",
                () -> newStoreFromSettings("s3::bucket=b;region=EU_WEST" + KEYS));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object store setting region is not a valid AWS region [region=a/b]",
                () -> newStoreFromSettings("s3::bucket=b;region=a/b" + KEYS));
        for (String endpoint : new String[]{
                "ftp://host", "http://user@host", "http://host/path", "http://host?x=1", "http://host#f", "not a uri", "/relative", "http:opaque", "http://"
        }) {
            assertFails(ObjectStoreException.Kind.FATAL, 0,
                    "object store setting endpoint must be http://host[:port] or https://host[:port] [endpoint=" + endpoint + ']',
                    () -> newStoreFromSettings("s3::bucket=b;endpoint=" + endpoint + KEYS));
        }
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object store setting region is not a valid AWS region [region=eu~west]",
                () -> newStoreFromSettings("s3::bucket=b;region=eu~west" + KEYS));
        assertFails(ObjectStoreException.Kind.FATAL, 0,
                "bucket name cannot be used in a host name, set enable_virtual_host_style=false [bucket=Bad_Bucket]",
                () -> newStoreFromSettings("s3::bucket=Bad_Bucket;region=eu-west-1" + KEYS));
        assertFails(ObjectStoreException.Kind.FATAL, 0,
                "bucket name cannot be used in a host name, set enable_virtual_host_style=false [bucket=b~1]",
                () -> newStoreFromSettings("s3::bucket=b~1;region=eu-west-1" + KEYS));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "object store settings are not for the S3 store [scheme=fs]",
                () -> new S3ObjectStore(ObjectStoreSettings.parse("fs::root=/x;"), configuration, Map.of()));
        assertFails(ObjectStoreException.Kind.FATAL, 0, "no AWS credentials found: set access_key_id and secret_access_key in the object store"
                        + " settings, set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or enable the EC2 instance metadata service"
                        + " (AWS_EC2_METADATA_DISABLED=true disables it)",
                () -> new S3ObjectStore(ObjectStoreSettings.parse("s3::bucket=b;region=eu-west-1;"), configuration,
                        Map.of(S3CredentialsProvider.ENV_EC2_METADATA_DISABLED, "true")));
        Assert.assertEquals(0, s3.getRequests().size());
    }

    @Test
    public void testDescription() {
        assertDescription("s3::bucket=b;region=eu-west-1" + KEYS, "s3 bucket=b, region=eu-west-1, endpoint=https://b.s3.eu-west-1.amazonaws.com, style=virtual-host");
        assertDescription("s3::bucket=b;region=eu-west-1;enable_virtual_host_style=false" + KEYS,
                "s3 bucket=b, region=eu-west-1, endpoint=https://s3.eu-west-1.amazonaws.com, style=path");
        assertDescription("s3::bucket=Bad_Bucket;region=eu-west-1;enable_virtual_host_style=FALSE" + KEYS,
                "s3 bucket=Bad_Bucket, region=eu-west-1, endpoint=https://s3.eu-west-1.amazonaws.com, style=path");
        assertDescription("s3::bucket=b;endpoint=http://127.0.0.1:9000" + KEYS, "s3 bucket=b, region=us-east-1, endpoint=http://127.0.0.1:9000, style=path");
        assertDescription("s3::bucket=b;endpoint=https://minio.local:443/;root=/x/y/;region=garage" + KEYS,
                "s3 bucket=b, root=x/y, region=garage, endpoint=https://minio.local, style=path");
        assertDescription("s3::bucket=b;endpoint=http://minio.local:80;enable_virtual_host_style=true" + KEYS,
                "s3 bucket=b, region=us-east-1, endpoint=http://b.minio.local, style=virtual-host");
        assertDescription("s3::bucket=b;endpoint=HTTP://Minio.Local:9000;root=///" + KEYS, "s3 bucket=b, region=us-east-1, endpoint=http://Minio.Local:9000, style=path");
        assertDescription("s3::bucket=b;endpoint=https://minio.local:9443;region=eu-west-1;enable_virtual_host_style=true" + KEYS,
                "s3 bucket=b, region=eu-west-1, endpoint=https://b.minio.local:9443, style=virtual-host");
        assertDescription("s3::bucket=b;endpoint=http://[::1]:9000" + KEYS, "s3 bucket=b, region=us-east-1, endpoint=http://[::1]:9000, style=path");
        assertDescription("s3::bucket=my.bucket-2;region=ap-southeast-2" + KEYS,
                "s3 bucket=my.bucket-2, region=ap-southeast-2, endpoint=https://my.bucket-2.s3.ap-southeast-2.amazonaws.com, style=virtual-host");
    }

    @Test
    public void testEnvironmentCredentials() throws Exception {
        try (S3ObjectStore store = new S3ObjectStore(
                ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint() + ';'),
                configuration,
                Map.of(S3CredentialsProvider.ENV_ACCESS_KEY_ID, ACCESS_KEY_ID, S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, SECRET_ACCESS_KEY)
        )) {
            Assert.assertEquals(md5Hex(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            Assert.assertEquals(0, s3.getSignatureFailures());
        }
    }

    @Test
    public void testErrorBodyIsTruncated() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            final String body = "<Error><Code>AccessDenied</Code><Message>" + "x".repeat(S3ObjectStore.MAX_BUFFERED_RESPONSE_SIZE + 100_000) + "</Message></Error>";
            s3.addFault("GET", Fault.response(403, Map.of(), body));
            assertFails(ObjectStoreException.Kind.FATAL, 403, "object store request failed [op=GET, key=k, status=403, code=AccessDenied]",
                    () -> store.get("k", null, new ByteSink()));
        }
    }

    @Test
    public void testFatalStatuses() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            final Object[][] cases = {
                    {400, "InvalidRequest"}, {401, "Unauthorized"}, {403, "AccessDenied"}, {301, "PermanentRedirect"}, {405, "MethodNotAllowed"},
                    {403, "RequestTimeTooSkewed"}
            };
            for (Object[] c : cases) {
                s3.addFault("GET", Fault.status((Integer) c[0], (String) c[1]));
                assertFails(ObjectStoreException.Kind.FATAL, (Integer) c[0],
                        "object store request failed [op=GET, key=k, status=" + c[0] + ", code=" + c[1] + ']',
                        () -> store.get("k", null, new ByteSink()));
            }
            Assert.assertEquals(cases.length, s3.getRequests().size());
            s3.addFault("DELETE", Fault.status(403, "AccessDenied"));
            assertFails(ObjectStoreException.Kind.FATAL, 403, "object store request failed [op=DELETE, key=k, status=403, code=AccessDenied]",
                    () -> store.delete("k"));
            s3.addFault("GET", Fault.status(412, "PreconditionFailed"));
            assertFails(ObjectStoreException.Kind.PRECONDITION_FAILED, 412,
                    "object store precondition failed [op=GET, key=k, status=412, code=PreconditionFailed]",
                    () -> store.get("k", null, new ByteSink()));
            // a 404 without a body is a missing object
            s3.addFault("GET", Fault.response(404, Map.of(), ""));
            assertFails(ObjectStoreException.Kind.NOT_FOUND, 404, "object not found [key=k]", () -> store.get("k", null, new ByteSink()));
            // DELETE treats 404 NoSuchKey as success
            s3.addFault("DELETE", Fault.status(404, "NoSuchKey"));
            store.delete("k");
            s3.addFault("DELETE", Fault.response(200, Map.of(), ""));
            store.delete("k");
            Assert.assertEquals(0, s3.getPendingFaultCount());
        }
    }

    @Test
    public void testGetBodyDeadlineGrowsWithContentLength() throws Exception {
        configuration.setRequestBaseTimeout(300).setMinThroughput(100_000);
        final byte[] data = new byte[100_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        s3.putObject("big", data);
        try (S3ObjectStore store = newStore("")) {
            // the body arrives over about 600ms, beyond the 300ms base timeout but within
            // 300ms + 100_000 bytes at 100_000 bytes/s
            s3.addFault("GET", Fault.slowBody(5, 120));
            final ByteSink sink = new ByteSink();
            Assert.assertEquals(md5Hex(data), store.get("big", null, sink));
            Assert.assertArrayEquals(data, sink.toByteArray());
            Assert.assertEquals(1, s3.getRequests().size());
        }
    }

    @Test
    public void testGetIfNoneMatch() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            final String etag = store.put("k", bytes("payload"), 0, 7, null, false);
            final ByteSink sink = new ByteSink(16);
            sink.put(bytes("old"), 0, 3);
            Assert.assertNull(store.get("k", etag, sink));
            Assert.assertEquals("old", utf8(sink));
            Assert.assertNull(store.get("k", '"' + etag + '"', sink));
            Assert.assertEquals("old", utf8(sink));
            Assert.assertEquals(etag, store.get("k", "0123456789abcdef0123456789abcdef", sink));
            Assert.assertEquals("payload", utf8(sink));
            final List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            Assert.assertEquals('"' + etag + '"', requests.get(1).headers.get("if-none-match"));
            Assert.assertEquals('"' + etag + '"', requests.get(2).headers.get("if-none-match"));
            Assert.assertEquals("\"0123456789abcdef0123456789abcdef\"", requests.get(3).headers.get("if-none-match"));
        }
    }

    @Test
    public void testGetStalledBodyRetries() throws Exception {
        configuration.setRequestBaseTimeout(300).setMinThroughput(1_000_000_000);
        final byte[] data = new byte[10_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 7);
        }
        s3.putObject("k", data);
        try (S3ObjectStore store = newStore("")) {
            // half the body, then silence: the attempt times out and the retry gets the whole object
            s3.addFault("GET", Fault.stallBody(1_500));
            final ByteSink sink = new ByteSink();
            Assert.assertEquals(md5Hex(data), store.get("k", null, sink));
            Assert.assertArrayEquals(data, sink.toByteArray());
            Assert.assertEquals(2, s3.getRequests().size());
            // with no retries left the timeout surfaces
            configuration.setRetryAttempts(0);
            try (S3ObjectStore noRetry = newStore("")) {
                s3.addFault("GET", Fault.stallBody(1_500));
                assertFails(ObjectStoreException.Kind.RETRYABLE, 0,
                        "object store request failed [op=GET, key=k, attempts=1, reason=request timed out [timeout=300ms]]",
                        () -> noRetry.get("k", null, sink));
            }
        }
    }

    @Test
    public void testIfMatchAndIfNoneMatchAreExclusive() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            assertFails(ObjectStoreException.Kind.FATAL, 0, "ifMatch and isIfNoneMatch are mutually exclusive [key=k]",
                    () -> store.put("k", bytes("v"), 0, 1, "abc", true));
            Assert.assertThrows(IndexOutOfBoundsException.class, () -> store.put("k", bytes("v"), 1, 1, null, false));
            Assert.assertEquals(0, s3.getRequests().size());
        }
    }

    @Test
    public void testIfMatchPut() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            final String e1 = store.put("k", bytes("v1"), 0, 2, null, false);
            final String e2 = store.put("k", bytes("v2"), 0, 2, e1, false);
            Assert.assertEquals(md5Hex(bytes("v2")), e2);
            assertFails(ObjectStoreException.Kind.PRECONDITION_FAILED, 412,
                    "object store precondition failed [op=PUT, key=k, status=412, code=PreconditionFailed]",
                    () -> store.put("k", bytes("v3"), 0, 2, e1, false));
            Assert.assertEquals("v2", new String(s3.getObject("k")));
            // quoted input is not quoted again
            final String e4 = store.put("k", bytes("v4"), 0, 2, '"' + e2 + '"', false);
            Assert.assertEquals(md5Hex(bytes("v4")), e4);
            // S3 answers If-Match on a missing object with 404 NoSuchKey
            assertFails(ObjectStoreException.Kind.PRECONDITION_FAILED, 404,
                    "object store precondition failed [op=PUT, key=missing, status=404, code=NoSuchKey]",
                    () -> store.put("missing", bytes("v"), 0, 1, e4, false));
            // a conflicting concurrent conditional write
            s3.addFault("PUT", Fault.status(409, "ConditionalRequestConflict"));
            assertFails(ObjectStoreException.Kind.PRECONDITION_FAILED, 409,
                    "object store precondition failed [op=PUT, key=k, status=409, code=ConditionalRequestConflict]",
                    () -> store.put("k", bytes("v5"), 0, 2, e4, false));
            final List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            Assert.assertNull(requests.get(0).headers.get("if-match"));
            Assert.assertEquals('"' + e1 + '"', requests.get(1).headers.get("if-match"));
            Assert.assertEquals('"' + e1 + '"', requests.get(2).headers.get("if-match"));
            Assert.assertEquals('"' + e2 + '"', requests.get(3).headers.get("if-match"));
            Assert.assertNull(requests.get(3).headers.get("if-none-match"));
        }
    }

    @Test
    public void testIfNoneMatchPut() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            Assert.assertEquals(md5Hex(bytes("first")), store.put("k", bytes("first"), 0, 5, null, true));
            assertFails(ObjectStoreException.Kind.PRECONDITION_FAILED, 412,
                    "object store precondition failed [op=PUT, key=k, status=412, code=PreconditionFailed]",
                    () -> store.put("k", bytes("second"), 0, 6, null, true));
            Assert.assertEquals("first", new String(s3.getObject("k")));
            Assert.assertEquals("*", s3.getRequests().get(1).headers.get("if-none-match"));
            Assert.assertNull(s3.getRequests().get(1).headers.get("if-match"));
        }
    }

    @Test
    public void testImdsCredentials() throws Exception {
        imds = new FakeImdsServer();
        imds.linkS3(s3);
        try (S3ObjectStore store = newImdsStore()) {
            store.put("k", bytes("v"), 0, 1, null, false);
            store.get("k", null, new ByteSink());
            Assert.assertEquals(List.of("k:1"), list(store, "", null));
            Assert.assertEquals(1, imds.getCredentialFetches());
            for (FakeS3Server.RecordedRequest request : s3.getRequests()) {
                Assert.assertEquals(FakeImdsServer.SESSION_TOKEN_PREFIX + 1, request.headers.get("x-amz-security-token"));
                final String authorization = request.headers.get("authorization");
                Assert.assertTrue(authorization, authorization.contains("Credential=" + FakeImdsServer.accessKeyId(1) + '/'));
                Assert.assertTrue(authorization, authorization.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,"));
            }
        }
        // credentials within five minutes of expiry are fetched again for every request
        imds.setExpiration(java.time.Instant.now().plusSeconds(60).toString());
        try (S3ObjectStore store = newImdsStore()) {
            store.put("k", bytes("v1"), 0, 2, null, false);
            store.put("k", bytes("v2"), 0, 2, null, false);
            Assert.assertEquals(3, imds.getCredentialFetches());
            final List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            Assert.assertTrue(requests.get(3).headers.get("authorization").contains("Credential=" + FakeImdsServer.accessKeyId(2) + '/'));
            Assert.assertTrue(requests.get(4).headers.get("authorization").contains("Credential=" + FakeImdsServer.accessKeyId(3) + '/'));
            Assert.assertEquals(FakeImdsServer.SESSION_TOKEN_PREFIX + 3, requests.get(4).headers.get("x-amz-security-token"));
        }
        Assert.assertEquals(0, s3.getSignatureFailures());
    }

    @Test
    public void testImdsFailures() throws Exception {
        imds = new FakeImdsServer();
        imds.linkS3(s3);
        // a transient metadata failure is retried like any request
        imds.addFault(FakeImdsServer.TOKEN_PATH, 500);
        try (S3ObjectStore store = newImdsStore()) {
            store.put("k", bytes("v"), 0, 1, null, false);
        }
        Assert.assertEquals(List.of(
                "PUT /latest/api/token",
                "PUT /latest/api/token", "GET " + FakeImdsServer.CREDENTIALS_PATH, "GET " + FakeImdsServer.CREDENTIALS_PATH + "test-role"
        ), imds.getRequests());
        Assert.assertEquals(1, s3.getRequests().size());
        // a permanent one is not
        imds.setRole("");
        try (S3ObjectStore store = newImdsStore()) {
            assertFails(ObjectStoreException.Kind.FATAL, 0, "EC2 instance metadata lists no IAM role, attach an instance profile to the instance",
                    () -> store.put("k2", bytes("v"), 0, 1, null, false));
        }
        Assert.assertEquals(6, imds.getRequests().size());
        Assert.assertEquals(1, s3.getRequests().size());
    }

    @Test
    public void testInterruptedBeforeRequest() throws Exception {
        configuration.setRetryAttempts(0);
        try (S3ObjectStore store = newStore("")) {
            Thread.currentThread().interrupt();
            try {
                final ObjectStoreException e = assertFails(ObjectStoreException.Kind.RETRYABLE, 0,
                        "object store request failed [op=PUT, key=k, attempts=1, reason=request interrupted]",
                        () -> store.put("k", bytes("v"), 0, 1, null, false));
                Assert.assertTrue(e.getCause().getCause() instanceof InterruptedException);
                Assert.assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            Assert.assertEquals(0, s3.getRequests().size());
        }
    }

    @Test
    public void testInterruptedDuringRequest() throws Exception {
        for (int retryAttempts : new int[]{0, 2}) {
            configuration.setRetryAttempts(retryAttempts);
            final int requestsBefore = s3.getRequests().size();
            try (S3ObjectStore store = newStore("")) {
                s3.addFault("GET", Fault.delayAndProcess(3_000));
                final AtomicReference<ObjectStoreException> failure = new AtomicReference<>();
                final AtomicReference<Boolean> isInterrupted = new AtomicReference<>();
                final Thread reader = new Thread(() -> {
                    try {
                        store.get("k", null, new ByteSink());
                    } catch (ObjectStoreException e) {
                        failure.set(e);
                    }
                    isInterrupted.set(Thread.currentThread().isInterrupted());
                });
                reader.start();
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (s3.getRequests().size() == requestsBefore && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                reader.interrupt();
                reader.join();
                final ObjectStoreException e = failure.get();
                Assert.assertNotNull(e);
                Assert.assertEquals(ObjectStoreException.Kind.RETRYABLE, e.getKind());
                // with retries left the pause before the next attempt sees the interrupt
                Assert.assertEquals(
                        retryAttempts == 0 ? "object store request failed [op=GET, key=k, attempts=1, reason=request interrupted]" : "object store request interrupted",
                        e.getMessage()
                );
                Assert.assertTrue(isInterrupted.get());
                Assert.assertEquals(requestsBefore + 1, s3.getRequests().size());
            }
        }
    }

    @Test
    public void testKeyValidation() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key is empty", () -> store.get("", null, new ByteSink()));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key must be relative [key=/abs]", () -> store.delete("/abs"));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object key has an empty, '.' or '..' segment [key=a/../b]",
                    () -> store.put("a/../b", bytes("v"), 0, 1, null, false));
            Assert.assertEquals(0, s3.getRequests().size());
        }
    }

    @Test
    public void testLateResponseLeavesSinkAlone() throws Exception {
        // An interrupted request leaves its exchange behind. Its body arrives only after a later
        // request has filled the sink, and must neither clear nor overwrite it.
        configuration.setRetryAttempts(0);
        final ScriptedHttpClient client = new ScriptedHttpClient();
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger lateCancels = new AtomicInteger();
        client.add(exchange -> {
            exchange.respond(200, Map.of("Content-Length", "4", "ETag", "\"e1\""));
            // the headers are in, and the caller gives up before the body starts
            exchange.interruptCaller();
            Assert.assertTrue(release.await(30, TimeUnit.SECONDS));
            exchange.subscribe(lateCancels);
            exchange.next(ByteBuffer.wrap(bytes("late")));
            exchange.complete();
        });
        client.add(exchange -> {
            exchange.respond(200, Map.of("Content-Length", "5", "ETag", "\"e2\""));
            exchange.subscribe(new AtomicInteger());
            exchange.next(ByteBuffer.wrap(bytes("fresh")));
            exchange.complete();
            exchange.finish();
        });
        try (S3ObjectStore store = newScriptedStore(client)) {
            final ByteSink sink = new ByteSink();
            sink.put(bytes("old"), 0, 3);
            try {
                try {
                    assertFails(ObjectStoreException.Kind.RETRYABLE, 0, "object store request failed [op=GET, key=k, attempts=1, reason=request interrupted]",
                            () -> store.get("k", null, sink));
                    Assert.assertTrue(Thread.currentThread().isInterrupted());
                } finally {
                    Thread.interrupted();
                }
                Assert.assertEquals("e2", store.get("k", null, sink));
                Assert.assertEquals("fresh", utf8(sink));
            } finally {
                release.countDown();
            }
            client.awaitScripts();
            Assert.assertEquals("fresh", utf8(sink));
            Assert.assertEquals(1, lateCancels.get());
        }
    }

    @Test
    public void testList() throws Exception {
        s3.setPageSize(2);
        final String[] keys = {"a&b<c>\"'", "b", "c/d", "c/e", "\u00e9", "\ufffd", "\uD83D\uDE00"};
        try (S3ObjectStore store = newStore("")) {
            for (int i = 0; i < keys.length; i++) {
                s3.putObject(keys[i], new byte[i + 1]);
            }
            final List<String> expected = List.of("a&b<c>\"':1", "b:2", "c/d:3", "c/e:4", "\u00e9:5", "\ufffd:6", "\uD83D\uDE00:7");
            Assert.assertEquals(expected, list(store, "", null));
            final List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            Assert.assertEquals(4, requests.size());
            Assert.assertEquals("GET /test-bucket?list-type=2", requests.get(0).toString());
            Assert.assertEquals(S3RequestSigner.EMPTY_PAYLOAD_SHA256, requests.get(0).headers.get("x-amz-content-sha256"));
            // the token is base64 of the last key of the page, which needs encoding in the query
            Assert.assertEquals("GET /test-bucket?continuation-token=Yg%3D%3D&list-type=2", requests.get(1).toString());
            // numeric character references decode to the same keys
            s3.setNumericEscapes(true);
            Assert.assertEquals(expected, list(store, "", null));
            Assert.assertEquals(0, s3.getSignatureFailures());
        }
    }

    @Test
    public void testListEmpty() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            Assert.assertEquals(List.of(), list(store, "", null));
            Assert.assertEquals(List.of(), list(store, "nothing/", "nothing/a"));
            Assert.assertEquals(2, s3.getRequests().size());
            Assert.assertEquals("GET /test-bucket?list-type=2&prefix=nothing%2F&start-after=nothing%2Fa", s3.getRequests().get(1).toString());
        }
    }

    @Test
    public void testListMalformedResponse() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            s3.addFault("GET", Fault.response(200, Map.of(), "<html>oops</html>"));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "malformed ListObjectsV2 response [prefix=x, reason=response is not a ListBucketResult]",
                    () -> list(store, "x", null));
            s3.addFault("GET", Fault.response(200, Map.of(), "not xml"));
            assertFails(ObjectStoreException.Kind.FATAL, 0,
                    "malformed ListObjectsV2 response [prefix=, reason=malformed XML [position=0, reason=text outside the root element]]",
                    () -> list(store, "", null));
            s3.addFault("GET", Fault.response(200, Map.of(), ""));
            assertFails(ObjectStoreException.Kind.FATAL, 0,
                    "malformed ListObjectsV2 response [prefix=, reason=malformed XML [position=0, reason=no root element]]",
                    () -> list(store, "", null));
        }
    }

    @Test
    public void testListPrefixStartAfterAndEarlyStop() throws Exception {
        s3.setPageSize(2);
        for (String key : new String[]{"a/1", "a/2", "a/3", "a/4", "b/1"}) {
            s3.putObject(key, bytes(key));
        }
        try (S3ObjectStore store = newStore("")) {
            Assert.assertEquals(List.of("a/2:3", "a/3:3", "a/4:3"), list(store, "a/", "a/1"));
            List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            Assert.assertEquals("GET /test-bucket?list-type=2&prefix=a%2F&start-after=a%2F1", requests.get(0).toString());
            // base64("a/3") = "YS8z"; the next page carries the token instead of start-after
            Assert.assertEquals("GET /test-bucket?continuation-token=YS8z&list-type=2&prefix=a%2F", requests.get(1).toString());
            Assert.assertEquals(2, requests.size());

            final List<String> seen = new ArrayList<>();
            store.list("", null, (key, size) -> {
                seen.add(key);
                return seen.size() < 3;
            });
            Assert.assertEquals(List.of("a/1", "a/2", "a/3"), seen);
            Assert.assertEquals(4, s3.getRequests().size());
        }
        // a token that needs percent-encoding: base64 of "a/?>" is "YS8/Pg=="
        s3.setPageSize(1);
        s3.putObject("a/?>", bytes("q"));
        s3.putObject("a/z", bytes("zz"));
        try (S3ObjectStore store = newStore("")) {
            final int before = s3.getRequests().size();
            Assert.assertEquals(List.of("a/1:3", "a/2:3", "a/3:3", "a/4:3", "a/?>:1", "a/z:2"), list(store, "a/", null));
            Assert.assertEquals(before + 6, s3.getRequests().size());
            Assert.assertEquals(
                    "GET /test-bucket?continuation-token=YS8%2FPg%3D%3D&list-type=2&prefix=a%2F",
                    s3.getRequests().get(before + 5).toString()
            );
            Assert.assertEquals(0, s3.getSignatureFailures());
        }
    }

    @Test
    public void testListSkipsForeignKeys() throws Exception {
        try (S3ObjectStore store = newStore("root=db1;")) {
            s3.addFault("GET", Fault.response(200, Map.of(),
                    "<ListBucketResult><Contents><Key>other/x</Key><Size>1</Size></Contents>"
                            + "<Contents><Key>db1/k</Key><Size>3</Size></Contents></ListBucketResult>"));
            Assert.assertEquals(List.of("k:3"), list(store, "", null));
            Assert.assertEquals("GET /test-bucket?list-type=2&prefix=db1%2F", s3.getRequests().get(0).toString());
        }
    }

    @Test
    public void testMaxConcurrentRequests() throws Exception {
        for (int limit : new int[]{1, 4}) {
            configuration.setMaxConcurrentRequests(limit);
            s3.close();
            s3 = new FakeS3Server();
            s3.putObject("k", bytes("v"));
            try (S3ObjectStore store = newStore("")) {
                final int threads = 4;
                for (int i = 0; i < threads; i++) {
                    s3.addFault("GET", Fault.delayAndProcess(150));
                }
                final CyclicBarrier start = new CyclicBarrier(threads);
                final AtomicInteger successes = new AtomicInteger();
                final List<Thread> readers = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    final Thread reader = new Thread(() -> {
                        try {
                            start.await();
                            if (store.get("k", null, new ByteSink()) != null) {
                                successes.incrementAndGet();
                            }
                        } catch (Exception ignore) {
                        }
                    });
                    readers.add(reader);
                    reader.start();
                }
                for (Thread reader : readers) {
                    reader.join();
                }
                Assert.assertEquals(threads, successes.get());
                if (limit == 1) {
                    Assert.assertEquals(1, s3.getMaxInFlight());
                } else {
                    Assert.assertTrue("max in flight " + s3.getMaxInFlight(), s3.getMaxInFlight() > 1);
                }
            }
        }
    }

    @Test
    public void testMissingETag() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            s3.addFault("PUT", Fault.response(200, Map.of(), ""));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object store response lacks an ETag [op=PUT, key=k]",
                    () -> store.put("k", bytes("v"), 0, 1, null, false));
            s3.addFault("GET", Fault.response(200, Map.of("ETag", "\"\""), "data"));
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object store response lacks an ETag [op=GET, key=k]",
                    () -> store.get("k", null, new ByteSink()));
        }
    }

    @Test
    public void testNoSuchBucket() throws Exception {
        try (S3ObjectStore store = newStoreFromSettings("s3::bucket=other-bucket;endpoint=" + s3.getEndpoint() + KEYS)) {
            assertFails(ObjectStoreException.Kind.FATAL, 404, "object store request failed [op=LIST, prefix=, status=404, code=NoSuchBucket]",
                    () -> list(store, "", null));
            assertFails(ObjectStoreException.Kind.FATAL, 404, "object store request failed [op=GET, key=k, status=404, code=NoSuchBucket]",
                    () -> store.get("k", null, new ByteSink()));
            assertFails(ObjectStoreException.Kind.FATAL, 404, "object store request failed [op=DELETE, key=k, status=404, code=NoSuchBucket]",
                    () -> store.delete("k"));
            assertFails(ObjectStoreException.Kind.FATAL, 404, "object store request failed [op=PUT, key=k, status=404, code=NoSuchBucket]",
                    () -> store.put("k", bytes("v"), 0, 1, null, false));
            assertFails(ObjectStoreException.Kind.FATAL, 404, "object store request failed [op=PUT, key=k, status=404, code=NoSuchBucket]",
                    () -> store.put("k", bytes("v"), 0, 1, "abc", false));
            Assert.assertEquals("PUT /other-bucket/k", s3.getRequests().get(3).toString());
        }
    }

    @Test
    public void testObjectTooLargeToBuffer() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            s3.addFault("GET", Fault.raw("HTTP/1.1 200 OK\r\nETag: \"abc\"\r\nContent-Length: 3000000000\r\n\r\n"));
            final ByteSink sink = new ByteSink();
            sink.put(bytes("old"), 0, 3);
            assertFails(ObjectStoreException.Kind.FATAL, 0, "object too large to buffer [key=k, size=3000000000]", () -> store.get("k", null, sink));
            Assert.assertEquals("old", utf8(sink));
            Assert.assertEquals(1, s3.getRequests().size());
        }
    }

    @Test
    public void testProperties() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            Assert.assertTrue(store.isIntegrityChecked());
            Assert.assertEquals(1750, store.getDefaultCleanerMaxRequestsPerSecond());
            Assert.assertEquals("s3 bucket=test-bucket, region=us-east-1, endpoint=" + s3.getEndpoint() + ", style=path", store.getDescription());
        }
    }

    @Test
    public void testPutGetDelete() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            final byte[] data = bytes("hello s3");
            final String key = "dir/a b+c~\u00e9.bin";
            final String etag = store.put(key, data, 0, data.length, null, false);
            Assert.assertEquals(md5Hex(data), etag);
            Assert.assertArrayEquals(data, s3.getObject(key));
            final ByteSink sink = new ByteSink(16);
            Assert.assertEquals(etag, store.get(key, null, sink));
            Assert.assertEquals("hello s3", utf8(sink));
            Assert.assertEquals(md5Hex(bytes("s3")), store.put(key, data, 6, 2, null, false));
            Assert.assertArrayEquals(bytes("s3"), s3.getObject(key));
            store.delete(key);
            Assert.assertNull(s3.getObject(key));
            assertFails(ObjectStoreException.Kind.NOT_FOUND, 404, "object not found [key=" + key + ']', () -> store.get(key, null, sink));
            store.delete(key);

            final List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            final String path = "/test-bucket/dir/a%20b%2Bc~%C3%A9.bin";
            Assert.assertEquals(
                    List.of("PUT " + path, "GET " + path, "PUT " + path, "DELETE " + path, "GET " + path, "DELETE " + path),
                    requests.stream().map(FakeS3Server.RecordedRequest::toString).toList()
            );
            final FakeS3Server.RecordedRequest put = requests.get(0);
            Assert.assertEquals(sha256Hex(data), put.headers.get("x-amz-content-sha256"));
            Assert.assertEquals(sha256Hex(bytes("s3")), requests.get(2).headers.get("x-amz-content-sha256"));
            Assert.assertEquals("127.0.0.1:" + s3.getPort(), put.headers.get("host"));
            Assert.assertTrue(put.headers.get("x-amz-date"), put.headers.get("x-amz-date").matches("\\d{8}T\\d{6}Z"));
            Assert.assertNull(put.headers.get("if-match"));
            Assert.assertNull(put.headers.get("if-none-match"));
            Assert.assertNull(put.headers.get("x-amz-security-token"));
            Assert.assertEquals(S3RequestSigner.EMPTY_PAYLOAD_SHA256, requests.get(1).headers.get("x-amz-content-sha256"));
            Assert.assertEquals(0, s3.getSignatureFailures());
        }
    }

    @Test
    public void testPutTimeoutAllowsForPayloadTransfer() throws Exception {
        configuration.setRequestBaseTimeout(200).setMinThroughput(100_000);
        try (S3ObjectStore store = newStore("")) {
            // 100_000 bytes at 100_000 bytes/s add a second to the 200ms base timeout
            final byte[] big = new byte[100_000];
            s3.addFault("PUT", Fault.delayAndProcess(600));
            Assert.assertEquals(md5Hex(big), store.put("big", big, 0, big.length, null, false));
            Assert.assertEquals(1, s3.getRequests().size());
            // a small payload gets only the base timeout, so the same delay costs a retry
            s3.addFault("PUT", Fault.delayAndProcess(600));
            Assert.assertEquals(md5Hex(bytes("small")), store.put("small", bytes("small"), 0, 5, null, false));
            Assert.assertEquals(3, s3.getRequests().size());
        }
    }

    @Test
    public void testPutTimeoutWithoutMinThroughput() throws Exception {
        // without a minimum throughput the payload adds nothing to the base timeout
        configuration.setRequestBaseTimeout(200).setMinThroughput(0);
        try (S3ObjectStore store = newStore("")) {
            final byte[] big = new byte[100_000];
            s3.addFault("PUT", Fault.delayAndProcess(600));
            Assert.assertEquals(md5Hex(big), store.put("big", big, 0, big.length, null, false));
            Assert.assertEquals(2, s3.getRequests().size());
        }
    }

    @Test
    public void testRetryableStatuses() throws Exception {
        try (S3ObjectStore store = newStore("")) {
            s3.addFault("PUT", Fault.status(500, "InternalError"));
            s3.addFault("PUT", Fault.status(503, "SlowDown"));
            Assert.assertEquals(md5Hex(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            Assert.assertEquals(3, s3.getRequests().size());
            s3.addFault("GET", Fault.status(429, "TooManyRequests"));
            s3.addFault("GET", Fault.status(408, "RequestTimeout"));
            final ByteSink sink = new ByteSink();
            Assert.assertEquals(md5Hex(bytes("v")), store.get("k", null, sink));
            Assert.assertEquals(6, s3.getRequests().size());

            for (int i = 0; i < 3; i++) {
                s3.addFault("PUT", Fault.status(503, "SlowDown"));
            }
            final ObjectStoreException e = assertFails(ObjectStoreException.Kind.RETRYABLE, 503,
                    "object store request failed [op=PUT, key=k2, attempts=3, reason=status=503, code=SlowDown]",
                    () -> store.put("k2", bytes("v"), 0, 1, null, false));
            final ObjectStoreException lastAttempt = (ObjectStoreException) e.getCause();
            Assert.assertEquals("status=503, code=SlowDown", lastAttempt.getMessage());
            Assert.assertEquals(503, lastAttempt.getStatus());
            Assert.assertNull(s3.getObject("k2"));

            for (int i = 0; i < 3; i++) {
                s3.addFault("DELETE", Fault.response(502, Map.of(), ""));
            }
            assertFails(ObjectStoreException.Kind.RETRYABLE, 502, "object store request failed [op=DELETE, key=k, attempts=3, reason=status=502]",
                    () -> store.delete("k"));
            for (int i = 0; i < 3; i++) {
                s3.addFault("GET", Fault.status(503, "SlowDown"));
            }
            assertFails(ObjectStoreException.Kind.RETRYABLE, 503, "object store request failed [op=LIST, prefix=p/, attempts=3, reason=status=503, code=SlowDown]",
                    () -> list(store, "p/", null));
            Assert.assertEquals(0, s3.getPendingFaultCount());
        }
    }

    @Test
    public void testRetryOnConnectionReset() throws Exception {
        s3.putObject("k", bytes("value"));
        try (S3ObjectStore store = newStore("")) {
            s3.addFault("GET", Fault.close());
            final ByteSink sink = new ByteSink();
            Assert.assertEquals(md5Hex(bytes("value")), store.get("k", null, sink));
            Assert.assertEquals("value", utf8(sink));
            Assert.assertEquals(2, s3.getRequests().size());
            for (int i = 0; i < 3; i++) {
                s3.addFault("PUT", Fault.close());
            }
            final ObjectStoreException e = assertRetryableWithPrefix(
                    "object store request failed [op=PUT, key=k, attempts=3, reason=I/O error [error=java.",
                    () -> store.put("k", bytes("v"), 0, 1, null, false)
            );
            Assert.assertTrue(e.getCause().getCause() instanceof IOException);
            Assert.assertEquals("value", new String(s3.getObject("k")));
        }
    }

    @Test
    public void testRootPrefix() throws Exception {
        try (S3ObjectStore store = newStore("root=/db1/sub/;")) {
            store.put("k", bytes("v"), 0, 1, null, false);
            Assert.assertArrayEquals(bytes("v"), s3.getObject("db1/sub/k"));
            s3.putObject("db1/other", bytes("x"));
            s3.putObject("outside", bytes("x"));
            Assert.assertEquals(List.of("k:1"), list(store, "", null));
            Assert.assertEquals(List.of(), list(store, "", "k"));
            Assert.assertEquals("PUT /test-bucket/db1/sub/k", s3.getRequests().get(0).toString());
            Assert.assertEquals("GET /test-bucket?list-type=2&prefix=db1%2Fsub%2F", s3.getRequests().get(1).toString());
            Assert.assertEquals("GET /test-bucket?list-type=2&prefix=db1%2Fsub%2F&start-after=db1%2Fsub%2Fk", s3.getRequests().get(2).toString());
            Assert.assertTrue(store.getDescription(), store.getDescription().startsWith("s3 bucket=test-bucket, root=db1/sub, region=us-east-1"));
        }
    }

    @Test
    public void testSessionTokenFromSettings() throws Exception {
        s3.addCredentials("AKTOKEN", "secret-2", "tok-123");
        try (S3ObjectStore store = newStoreFromSettings("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint()
                + ";access_key_id=AKTOKEN;secret_access_key=secret-2;session_token=tok-123;")) {
            store.put("k", bytes("v"), 0, 1, null, false);
            Assert.assertEquals("tok-123", s3.getRequests().get(0).headers.get("x-amz-security-token"));
        }
        try (S3ObjectStore store = newStoreFromSettings("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint()
                + ";access_key_id=AKTOKEN;secret_access_key=secret-2;")) {
            assertFails(ObjectStoreException.Kind.FATAL, 403, "object store request failed [op=PUT, key=k, status=403, code=InvalidToken]",
                    () -> store.put("k", bytes("v"), 0, 1, null, false));
        }
        Assert.assertEquals(1, s3.getSignatureFailures());
    }

    @Test
    public void testSignatureFailuresAreFatal() throws Exception {
        try (S3ObjectStore store = newStoreFromSettings("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint()
                + ";access_key_id=" + ACCESS_KEY_ID + ";secret_access_key=wrong-secret;")) {
            final ObjectStoreException e = assertFails(ObjectStoreException.Kind.FATAL, 403,
                    "object store request failed [op=PUT, key=k, status=403, code=SignatureDoesNotMatch]",
                    () -> store.put("k", bytes("v"), 0, 1, null, false));
            Assert.assertFalse(e.getMessage().contains("wrong-secret"));
        }
        try (S3ObjectStore store = newStoreFromSettings("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint()
                + ";access_key_id=AKUNKNOWN;secret_access_key=s;")) {
            assertFails(ObjectStoreException.Kind.FATAL, 403, "object store request failed [op=GET, key=k, status=403, code=InvalidAccessKeyId]",
                    () -> store.get("k", null, new ByteSink()));
        }
        // the signer must agree with the region the server expects
        s3.setRegion("eu-central-1");
        try (S3ObjectStore store = newStore("")) {
            assertFails(ObjectStoreException.Kind.FATAL, 403, "object store request failed [op=DELETE, key=k, status=403, code=AuthorizationHeaderMalformed]",
                    () -> store.delete("k"));
        }
        Assert.assertEquals(3, s3.getSignatureFailures());
        Assert.assertEquals(3, s3.getRequests().size());
    }

    @Test
    public void testTimeoutWithoutResponse() throws Exception {
        configuration.setRequestBaseTimeout(300);
        try (S3ObjectStore store = newStore("")) {
            s3.addFault("PUT", Fault.delayAndDrop(2_000));
            Assert.assertEquals(md5Hex(bytes("v")), store.put("k", bytes("v"), 0, 1, null, false));
            Assert.assertEquals(2, s3.getRequests().size());
            for (int i = 0; i < 3; i++) {
                s3.addFault("GET", Fault.delayAndDrop(1_000));
            }
            assertFails(ObjectStoreException.Kind.RETRYABLE, 0,
                    "object store request failed [op=GET, key=k, attempts=3, reason=request timed out [timeout=300ms]]",
                    () -> store.get("k", null, new ByteSink()));
            Assert.assertEquals(5, s3.getRequests().size());
        }
    }

    @Test
    public void testVirtualHostStyleThroughProxy() throws Exception {
        s3.setVirtualHost(true);
        final HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .proxy(ProxySelector.of(new InetSocketAddress("127.0.0.1", s3.getPort())))
                .build();
        try {
            try (S3ObjectStore store = new S3ObjectStore(
                    ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=http://s3.fake.test:1234;enable_virtual_host_style=true" + KEYS),
                    configuration,
                    Map.of(),
                    client
            )) {
                Assert.assertEquals(md5Hex(bytes("v")), store.put("dir/k", bytes("v"), 0, 1, null, false));
                final ByteSink sink = new ByteSink();
                Assert.assertEquals(md5Hex(bytes("v")), store.get("dir/k", null, sink));
                Assert.assertEquals(List.of("dir/k:1"), list(store, "", null));
                store.delete("dir/k");
                Assert.assertNull(s3.getObject("dir/k"));
            }
            // the default port is left out of the Host header, which the signature covers
            try (S3ObjectStore store = new S3ObjectStore(
                    ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=http://s3.fake.test:80;enable_virtual_host_style=true" + KEYS),
                    configuration,
                    Map.of(),
                    client
            )) {
                store.put("k", bytes("v"), 0, 1, null, false);
            }
            final List<FakeS3Server.RecordedRequest> requests = s3.getRequests();
            Assert.assertEquals(
                    List.of("PUT /dir/k", "GET /dir/k", "GET /?list-type=2", "DELETE /dir/k", "PUT /k"),
                    requests.stream().map(FakeS3Server.RecordedRequest::toString).toList()
            );
            Assert.assertEquals("test-bucket.s3.fake.test:1234", requests.get(0).headers.get("host"));
            Assert.assertEquals("test-bucket.s3.fake.test", requests.get(4).headers.get("host"));
            Assert.assertEquals(0, s3.getSignatureFailures());
        } finally {
            client.shutdownNow();
        }
    }

    private static ObjectStoreException assertRetryableWithPrefix(String prefix, Runnable action) {
        try {
            action.run();
        } catch (ObjectStoreException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().startsWith(prefix));
            Assert.assertEquals(ObjectStoreException.Kind.RETRYABLE, e.getKind());
            Assert.assertEquals(0, e.getStatus());
            return e;
        }
        Assert.fail("expected an exception starting with " + prefix);
        return null;
    }

    private void assertDescription(String settings, String expected) {
        try (S3ObjectStore store = newStoreFromSettings(settings)) {
            Assert.assertEquals(expected, store.getDescription());
            Assert.assertFalse(store.getDescription().contains(ACCESS_KEY_ID));
            Assert.assertFalse(store.getDescription().contains(SECRET_ACCESS_KEY));
        }
    }

    private S3ObjectStore newImdsStore() {
        return new S3ObjectStore(
                ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint() + ';'),
                configuration,
                Map.of(S3CredentialsProvider.ENV_EC2_METADATA_SERVICE_ENDPOINT, imds.getEndpoint())
        );
    }

    private S3ObjectStore newScriptedStore(HttpClient client) {
        // the scripted client answers every request itself and never connects to the endpoint
        return new S3ObjectStore(
                ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=http://s3.scripted.test" + KEYS),
                configuration,
                Map.of(),
                client
        );
    }

    private S3ObjectStore newStore(String extraSettings) {
        return new S3ObjectStore(settings(extraSettings), configuration, Map.of());
    }

    private S3ObjectStore newStoreFromSettings(String settings) {
        return new S3ObjectStore(ObjectStoreSettings.parse(settings), configuration, Map.of());
    }

    private ObjectStoreSettings settings(String extraSettings) {
        return ObjectStoreSettings.parse("s3::bucket=" + BUCKET + ";endpoint=" + s3.getEndpoint() + KEYS + extraSettings);
    }

    @FunctionalInterface
    private interface ExchangeScript {
        void run(ScriptedExchange<?> exchange) throws Exception;
    }

    /**
     * One request sent through {@link ScriptedHttpClient}. Its script plays the HTTP client with these
     * methods, signalling the store's body subscriber in whatever order the test needs.
     */
    private static final class ScriptedExchange<T> {
        private final Thread caller;
        private final HttpResponse.BodyHandler<T> handler;
        private final HttpRequest request;
        private final CompletableFuture<HttpResponse<T>> response = new CompletableFuture<>();
        private HttpHeaders headers;
        private int status;
        private HttpResponse.BodySubscriber<T> subscriber;

        ScriptedExchange(HttpRequest request, HttpResponse.BodyHandler<T> handler, Thread caller) {
            this.request = request;
            this.handler = handler;
            this.caller = caller;
        }

        void complete() {
            subscriber.onComplete();
        }

        // completes the response once the body subscriber has its body, or has failed
        void finish() {
            subscriber.getBody().whenComplete((body, error) -> {
                if (error != null) {
                    response.completeExceptionally(error);
                } else {
                    response.complete(new ScriptedResponse<>(request, status, headers, body));
                }
            });
        }

        void interruptCaller() {
            caller.interrupt();
        }

        void next(ByteBuffer buffer) {
            subscriber.onNext(List.of(buffer));
        }

        // delivers the status and headers, for which the store's body handler picks a subscriber
        void respond(int status, Map<String, String> headers) {
            final Map<String, List<String>> values = new HashMap<>();
            headers.forEach((name, value) -> values.put(name, List.of(value)));
            final HttpHeaders responseHeaders = HttpHeaders.of(values, (name, value) -> true);
            this.status = status;
            this.headers = responseHeaders;
            subscriber = handler.apply(new HttpResponse.ResponseInfo() {
                @Override
                public HttpHeaders headers() {
                    return responseHeaders;
                }

                @Override
                public int statusCode() {
                    return status;
                }

                @Override
                public HttpClient.Version version() {
                    return HttpClient.Version.HTTP_1_1;
                }
            });
        }

        void subscribe(AtomicInteger cancels) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void cancel() {
                    cancels.incrementAndGet();
                }

                @Override
                public void request(long n) {
                }
            });
        }
    }

    /**
     * HTTP client that answers each request with the next script a test added, run on a thread of its
     * own, so a test decides when the store's body subscriber hears about the response, which a real
     * client gives no control over. It never opens a connection.
     */
    private static final class ScriptedHttpClient extends HttpClient {
        private final List<Throwable> failures = new CopyOnWriteArrayList<>();
        private final ArrayDeque<ExchangeScript> scripts = new ArrayDeque<>();
        private final List<Thread> threads = new CopyOnWriteArrayList<>();

        @Override
        public Optional<Authenticator> authenticator() {
            return Optional.empty();
        }

        @Override
        public Optional<Duration> connectTimeout() {
            return Optional.empty();
        }

        @Override
        public Optional<CookieHandler> cookieHandler() {
            return Optional.empty();
        }

        @Override
        public Optional<Executor> executor() {
            return Optional.empty();
        }

        @Override
        public Redirect followRedirects() {
            return Redirect.NEVER;
        }

        @Override
        public Optional<ProxySelector> proxy() {
            return Optional.empty();
        }

        @Override
        public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
            final ScriptedExchange<T> exchange = new ScriptedExchange<>(request, handler, Thread.currentThread());
            final ExchangeScript script;
            synchronized (this) {
                script = scripts.poll();
            }
            if (script == null) {
                final AssertionError error = new AssertionError("no script for " + request.method() + ' ' + request.uri());
                failures.add(error);
                exchange.response.completeExceptionally(error);
                return exchange.response;
            }
            final Thread thread = new Thread(() -> {
                try {
                    script.run(exchange);
                } catch (Throwable e) {
                    failures.add(e);
                    exchange.response.completeExceptionally(e);
                }
            }, "scripted-http-exchange");
            thread.setDaemon(true);
            threads.add(thread);
            thread.start();
            return exchange.response;
        }

        @Override
        public <T> CompletableFuture<HttpResponse<T>> sendAsync(
                HttpRequest request,
                HttpResponse.BodyHandler<T> handler,
                HttpResponse.PushPromiseHandler<T> pushPromiseHandler
        ) {
            return sendAsync(request, handler);
        }

        @Override
        public SSLContext sslContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLParameters sslParameters() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Version version() {
            return Version.HTTP_1_1;
        }

        synchronized void add(ExchangeScript script) {
            scripts.add(script);
        }

        // waits for the scripts to end, and fails when one threw or one was never used
        void awaitScripts() throws InterruptedException {
            for (Thread thread : threads) {
                thread.join(30_000);
                Assert.assertFalse("a script is still running", thread.isAlive());
            }
            if (!failures.isEmpty()) {
                throw new AssertionError("a script failed", failures.get(0));
            }
            synchronized (this) {
                Assert.assertTrue("scripts left unused", scripts.isEmpty());
            }
        }
    }

    private record ScriptedResponse<T>(HttpRequest request, int statusCode, HttpHeaders headers, T body) implements HttpResponse<T> {
        @Override
        public Optional<HttpResponse<T>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return request.uri();
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }
}
