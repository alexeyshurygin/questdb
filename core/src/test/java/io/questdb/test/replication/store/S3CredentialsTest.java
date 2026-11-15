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

import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreSettings;
import io.questdb.replication.store.s3.S3Credentials;
import io.questdb.replication.store.s3.S3CredentialsProvider;
import io.questdb.replication.store.s3.S3ImdsCredentialsProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.replication.store.FsObjectStoreTest.assertFails;

public class S3CredentialsTest {
    private static final String CREDENTIALS = FakeImdsServer.CREDENTIALS_PATH;
    private static final long NOW = Instant.parse("2026-11-14T12:00:00Z").toEpochMilli();
    private static final String TOKEN = FakeImdsServer.TOKEN_PATH;
    private final AtomicLong clock = new AtomicLong(NOW);
    private HttpClient httpClient;
    private FakeImdsServer imds;

    @Before
    public void setUp() throws IOException {
        imds = new FakeImdsServer();
        httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }

    @After
    public void tearDown() throws IOException {
        if (httpClient != null) {
            httpClient.shutdownNow();
        }
        if (imds != null) {
            imds.close();
        }
    }

    @Test
    public void testChainPrefersSettings() {
        final S3CredentialsProvider provider = S3CredentialsProvider.fromSettings(
                ObjectStoreSettings.parse("s3::bucket=b;access_key_id=AKSET;secret_access_key=secret/set;session_token=tok;"),
                Map.of(S3CredentialsProvider.ENV_ACCESS_KEY_ID, "AKENV", S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, "secret-env"),
                httpClient,
                1000
        );
        final S3Credentials credentials = provider.getCredentials();
        Assert.assertEquals("AKSET", credentials.getAccessKeyId());
        Assert.assertEquals("secret/set", credentials.getSecretAccessKey());
        Assert.assertEquals("tok", credentials.getSessionToken());
        Assert.assertEquals(S3Credentials.NO_EXPIRATION, credentials.getExpirationMillis());
        Assert.assertSame(credentials, provider.getCredentials());
        Assert.assertEquals(List.of(), imds.getRequests());
    }

    @Test
    public void testChainReadsEnvironment() {
        S3Credentials credentials = S3CredentialsProvider.fromSettings(
                ObjectStoreSettings.parse("s3::bucket=b;"),
                Map.of(
                        S3CredentialsProvider.ENV_ACCESS_KEY_ID, "AKENV",
                        S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, "secret-env",
                        S3CredentialsProvider.ENV_SESSION_TOKEN, "token-env",
                        S3CredentialsProvider.ENV_EC2_METADATA_SERVICE_ENDPOINT, imds.getEndpoint()
                ),
                httpClient,
                1000
        ).getCredentials();
        Assert.assertEquals("AKENV", credentials.getAccessKeyId());
        Assert.assertEquals("secret-env", credentials.getSecretAccessKey());
        Assert.assertEquals("token-env", credentials.getSessionToken());
        Assert.assertEquals(S3Credentials.NO_EXPIRATION, credentials.getExpirationMillis());
        // an empty session token counts as none
        credentials = S3CredentialsProvider.fromSettings(
                ObjectStoreSettings.parse("s3::bucket=b;"),
                Map.of(S3CredentialsProvider.ENV_ACCESS_KEY_ID, "AKENV", S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, "secret-env", S3CredentialsProvider.ENV_SESSION_TOKEN, ""),
                httpClient,
                1000
        ).getCredentials();
        Assert.assertNull(credentials.getSessionToken());
        Assert.assertEquals(List.of(), imds.getRequests());
    }

    @Test
    public void testChainRejectsHalfOfEnvironmentPair() {
        final String message = "environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together";
        final ObjectStoreSettings settings = ObjectStoreSettings.parse("s3::bucket=b;");
        assertFails(ObjectStoreException.Kind.FATAL, 0, message,
                () -> S3CredentialsProvider.fromSettings(settings, Map.of(S3CredentialsProvider.ENV_ACCESS_KEY_ID, "AK"), httpClient, 1000));
        assertFails(ObjectStoreException.Kind.FATAL, 0, message,
                () -> S3CredentialsProvider.fromSettings(settings, Map.of(S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, "SK"), httpClient, 1000));
        assertFails(ObjectStoreException.Kind.FATAL, 0, message,
                () -> S3CredentialsProvider.fromSettings(settings, Map.of(S3CredentialsProvider.ENV_ACCESS_KEY_ID, "", S3CredentialsProvider.ENV_SECRET_ACCESS_KEY, "SK"), httpClient, 1000));
    }

    @Test
    public void testChainUsesImds() {
        final ObjectStoreSettings settings = ObjectStoreSettings.parse("s3::bucket=b;");
        final String message = "no AWS credentials found: set access_key_id and secret_access_key in the object store settings, set"
                + " AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or enable the EC2 instance metadata service (AWS_EC2_METADATA_DISABLED=true disables it)";
        assertFails(ObjectStoreException.Kind.FATAL, 0, message,
                () -> S3CredentialsProvider.fromSettings(settings, Map.of(S3CredentialsProvider.ENV_EC2_METADATA_DISABLED, "true"), httpClient, 1000));
        assertFails(ObjectStoreException.Kind.FATAL, 0, message,
                () -> S3CredentialsProvider.fromSettings(settings, Map.of(S3CredentialsProvider.ENV_EC2_METADATA_DISABLED, " TRUE "), httpClient, 1000));
        // any other value leaves the metadata service on
        final S3CredentialsProvider provider = S3CredentialsProvider.fromSettings(
                settings,
                Map.of(S3CredentialsProvider.ENV_EC2_METADATA_DISABLED, "false", S3CredentialsProvider.ENV_EC2_METADATA_SERVICE_ENDPOINT, imds.getEndpoint() + '/'),
                httpClient,
                1000
        );
        Assert.assertEquals(S3ImdsCredentialsProvider.class, provider.getClass());
        Assert.assertEquals(FakeImdsServer.accessKeyId(1), provider.getCredentials().getAccessKeyId());
        // without an override the provider targets the link-local address; nothing contacts it here
        Assert.assertEquals(
                S3ImdsCredentialsProvider.class,
                S3CredentialsProvider.fromSettings(settings, Map.of(S3CredentialsProvider.ENV_EC2_METADATA_SERVICE_ENDPOINT, ""), httpClient, 1000).getClass()
        );
        Assert.assertEquals(S3ImdsCredentialsProvider.class, S3CredentialsProvider.fromSettings(settings, Map.of(), httpClient, 1000).getClass());
    }

    @Test
    public void testCredentialsToStringHidesSecrets() {
        Assert.assertEquals(
                "S3Credentials[accessKeyId=****, secretAccessKey=****, sessionToken=****, expirationMillis=5]",
                new S3Credentials("AKID", "secret", "token", 5).toString()
        );
        Assert.assertEquals(
                "S3Credentials[accessKeyId=****, secretAccessKey=****, sessionToken=none, expirationMillis=9223372036854775807]",
                new S3Credentials("AKID", "secret", null, S3Credentials.NO_EXPIRATION).toString()
        );
    }

    @Test
    public void testImdsCachesAndRefreshesBeforeExpiry() {
        final long expiration = NOW + 3_600_000;
        imds.setExpiration(Instant.ofEpochMilli(expiration).toString());
        final S3ImdsCredentialsProvider provider = newProvider(1000);
        final S3Credentials first = provider.getCredentials();
        Assert.assertEquals(expiration, first.getExpirationMillis());
        Assert.assertEquals(FakeImdsServer.accessKeyId(1), first.getAccessKeyId());
        clock.set(expiration - S3ImdsCredentialsProvider.REFRESH_MARGIN_MILLIS - 1);
        Assert.assertSame(first, provider.getCredentials());
        Assert.assertEquals(1, imds.getCredentialFetches());
        Assert.assertEquals(3, imds.getRequests().size());
        // five minutes before expiry the provider fetches new credentials
        clock.set(expiration - S3ImdsCredentialsProvider.REFRESH_MARGIN_MILLIS);
        final S3Credentials second = provider.getCredentials();
        Assert.assertEquals(FakeImdsServer.accessKeyId(2), second.getAccessKeyId());
        Assert.assertEquals(FakeImdsServer.SECRET_PREFIX + 2, second.getSecretAccessKey());
        Assert.assertEquals(FakeImdsServer.SESSION_TOKEN_PREFIX + 2, second.getSessionToken());
        Assert.assertEquals(6, imds.getRequests().size());
    }

    @Test
    public void testImdsCodeNotSuccess() {
        imds.setCredentialsBody("{\"Code\":\"AssumeRoleUnauthorizedAccess\",\"Message\":\"denied\"}");
        assertFails(ObjectStoreException.Kind.FATAL, 0,
                "EC2 instance metadata did not return credentials [role=test-role, code=AssumeRoleUnauthorizedAccess]",
                () -> newProvider(1000).getCredentials());
    }

    @Test
    public void testImdsConnectionReset() {
        imds.addFault(TOKEN, 0);
        final ObjectStoreException e = assertFailsWithPrefix(
                "EC2 instance metadata request failed [path=/latest/api/token, error=java.io.IOException",
                () -> newProvider(1000).getCredentials()
        );
        Assert.assertTrue(e.getCause() instanceof IOException);
    }

    @Test
    public void testImdsEndpointValidation() {
        for (String endpoint : new String[]{"not a uri", "ftp://host", "/relative", "http:opaque", "mailto:x@y"}) {
            assertFails(ObjectStoreException.Kind.FATAL, 0, "invalid EC2 instance metadata endpoint [endpoint=" + endpoint + ']',
                    () -> new S3ImdsCredentialsProvider(httpClient, endpoint, 1000, clock::get));
        }
        // https is accepted; nothing contacts this endpoint
        Assert.assertNotNull(new S3ImdsCredentialsProvider(httpClient, "https://127.0.0.1:1", 1000, clock::get));
        // surrounding blanks and trailing slashes are dropped
        final S3ImdsCredentialsProvider provider = new S3ImdsCredentialsProvider(httpClient, ' ' + imds.getEndpoint() + "// ", 1000, clock::get);
        Assert.assertEquals(FakeImdsServer.accessKeyId(1), provider.getCredentials().getAccessKeyId());
        Assert.assertEquals(List.of("PUT " + TOKEN, "GET " + CREDENTIALS, "GET " + CREDENTIALS + "test-role"), imds.getRequests());
    }

    @Test
    public void testImdsInterrupted() {
        Thread.currentThread().interrupt();
        try {
            final ObjectStoreException e = assertFails(ObjectStoreException.Kind.RETRYABLE, 0,
                    "EC2 instance metadata request interrupted [path=/latest/api/token]",
                    () -> newProvider(1000).getCredentials());
            Assert.assertTrue(e.getCause() instanceof InterruptedException);
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testImdsMalformedCredentials() {
        imds.setCredentialsBody("not json");
        assertFails(ObjectStoreException.Kind.FATAL, 0,
                "malformed EC2 instance metadata credentials [role=test-role, reason=malformed JSON [position=1, reason=expected '{']]",
                () -> newProvider(1000).getCredentials());
        imds.setCredentialsBody("{\"AccessKeyId\":\"A\",\"SecretAccessKey\":\"S\",\"Token\":\"T\",\"Expiration\":\"tomorrow\"}");
        assertFails(ObjectStoreException.Kind.FATAL, 0,
                "malformed EC2 instance metadata credentials [role=test-role, reason=invalid Expiration]",
                () -> newProvider(1000).getCredentials());
        imds.setCredentialsBody("{\"AccessKeyId\":\"A\",\"SecretAccessKey\":\"S\",\"Expiration\":\"2100-01-01T00:00:00Z\"}");
        assertFails(ObjectStoreException.Kind.FATAL, 0, "EC2 instance metadata credentials lack a field [field=Token]",
                () -> newProvider(1000).getCredentials());
        imds.setCredentialsBody("{\"AccessKeyId\":\"\",\"SecretAccessKey\":\"S\",\"Token\":\"T\",\"Expiration\":\"2100-01-01T00:00:00Z\"}");
        assertFails(ObjectStoreException.Kind.FATAL, 0, "EC2 instance metadata credentials lack a field [field=AccessKeyId]",
                () -> newProvider(1000).getCredentials());
        imds.setCredentialsBody("{\"AccessKeyId\":\"A\",\"Token\":\"T\",\"Expiration\":\"2100-01-01T00:00:00Z\"}");
        assertFails(ObjectStoreException.Kind.FATAL, 0, "EC2 instance metadata credentials lack a field [field=SecretAccessKey]",
                () -> newProvider(1000).getCredentials());
        imds.setCredentialsBody("{\"AccessKeyId\":\"A\",\"SecretAccessKey\":\"S\",\"Token\":\"T\"}");
        assertFails(ObjectStoreException.Kind.FATAL, 0, "EC2 instance metadata credentials lack a field [field=Expiration]",
                () -> newProvider(1000).getCredentials());
        // without a Code member the document is still accepted
        imds.setCredentialsBody("{\"AccessKeyId\":\"A\",\"SecretAccessKey\":\"S\",\"Token\":\"T\",\"Expiration\":\"2100-01-01T00:00:00Z\"}");
        final S3Credentials credentials = newProvider(1000).getCredentials();
        Assert.assertEquals("A", credentials.getAccessKeyId());
        Assert.assertEquals(Instant.parse("2100-01-01T00:00:00Z").toEpochMilli(), credentials.getExpirationMillis());
    }

    @Test
    public void testImdsNoRole() {
        imds.setRole("");
        assertFails(ObjectStoreException.Kind.FATAL, 0, "EC2 instance metadata lists no IAM role, attach an instance profile to the instance",
                () -> newProvider(1000).getCredentials());
        Assert.assertEquals(List.of("PUT " + TOKEN, "GET " + CREDENTIALS), imds.getRequests());
    }

    @Test
    public void testImdsRefreshFailureKeepsValidCredentials() {
        final long expiration = NOW + 3_600_000;
        imds.setExpiration(Instant.ofEpochMilli(expiration).toString());
        final S3ImdsCredentialsProvider provider = newProvider(1000);
        final S3Credentials first = provider.getCredentials();
        // inside the refresh window the refresh fails, but the cached credentials still work
        clock.set(expiration - 60_000);
        imds.addFault(TOKEN, 500);
        Assert.assertSame(first, provider.getCredentials());
        imds.addFault(CREDENTIALS, 404);
        Assert.assertSame(first, provider.getCredentials());
        // once they expire, the failure surfaces
        clock.set(expiration);
        imds.addFault(TOKEN, 503);
        assertFails(ObjectStoreException.Kind.RETRYABLE, 503, "EC2 instance metadata request failed [path=/latest/api/token, status=503]",
                provider::getCredentials);
        // and the next successful fetch replaces them
        imds.setExpiration(Instant.ofEpochMilli(expiration + 7_200_000).toString());
        Assert.assertEquals(FakeImdsServer.accessKeyId(2), provider.getCredentials().getAccessKeyId());
    }

    @Test
    public void testImdsRoleNameWithDots() {
        imds.setRole("role.name-1_x~");
        Assert.assertEquals(FakeImdsServer.accessKeyId(1), newProvider(1000).getCredentials().getAccessKeyId());
        Assert.assertEquals(List.of("PUT " + TOKEN, "GET " + CREDENTIALS, "GET " + CREDENTIALS + "role.name-1_x~"), imds.getRequests());
    }

    @Test
    public void testImdsRoleListWithSeveralRoles() {
        imds.setRoleList("\n first-role \nsecond-role\n");
        imds.setRole("first-role");
        Assert.assertEquals(FakeImdsServer.accessKeyId(1), newProvider(1000).getCredentials().getAccessKeyId());
        Assert.assertEquals("GET " + CREDENTIALS + "first-role", imds.getRequests().get(2));
    }

    @Test
    public void testImdsStalledBody() {
        // the response head arrives, the body does not: the request times out all the same
        imds.addStallFault(CREDENTIALS, 2_000);
        final long start = System.nanoTime();
        assertFails(ObjectStoreException.Kind.RETRYABLE, 0, "EC2 instance metadata request timed out [path=" + CREDENTIALS + ", timeout=150ms]",
                () -> newProvider(150).getCredentials());
        Assert.assertTrue((System.nanoTime() - start) / 1_000_000 < 1_500);
    }

    @Test
    public void testImdsStatusMapping() {
        final int[] statuses = {400, 401, 403, 404, 408, 429, 500, 503};
        for (int status : statuses) {
            final boolean isRetryable = status == 408 || status == 429 || status >= 500;
            imds.addFault(CREDENTIALS, status);
            assertFails(
                    isRetryable ? ObjectStoreException.Kind.RETRYABLE : ObjectStoreException.Kind.FATAL,
                    status,
                    "EC2 instance metadata request failed [path=" + CREDENTIALS + ", status=" + status + ']',
                    () -> newProvider(1000).getCredentials()
            );
        }
        // the third request fails too
        imds.addFault(CREDENTIALS + "test-role", 500);
        assertFails(ObjectStoreException.Kind.RETRYABLE, 500, "EC2 instance metadata request failed [path=" + CREDENTIALS + "test-role, status=500]",
                () -> newProvider(1000).getCredentials());
    }

    @Test
    public void testImdsTimeout() {
        imds.addFault(TOKEN, -2_000);
        final long start = System.nanoTime();
        assertFails(ObjectStoreException.Kind.RETRYABLE, 0, "EC2 instance metadata request timed out [path=/latest/api/token, timeout=150ms]",
                () -> newProvider(150).getCredentials());
        Assert.assertTrue((System.nanoTime() - start) / 1_000_000 < 1_500);
    }

    @Test
    public void testImdsTokenFlow() {
        imds.setExpiration("2026-11-14T18:30:00Z");
        final S3Credentials credentials = newProvider(1000).getCredentials();
        Assert.assertEquals(FakeImdsServer.accessKeyId(1), credentials.getAccessKeyId());
        // the JSON escapes the slash in the secret as \/
        Assert.assertEquals(FakeImdsServer.SECRET_PREFIX + 1, credentials.getSecretAccessKey());
        Assert.assertEquals(FakeImdsServer.SESSION_TOKEN_PREFIX + 1, credentials.getSessionToken());
        Assert.assertEquals(Instant.parse("2026-11-14T18:30:00Z").toEpochMilli(), credentials.getExpirationMillis());
        Assert.assertEquals(List.of("PUT " + TOKEN, "GET " + CREDENTIALS, "GET " + CREDENTIALS + "test-role"), imds.getRequests());
    }

    @Test
    public void testJsonObject() {
        Assert.assertEquals(Map.of(), S3ImdsCredentialsProvider.parseJsonObject(" { } "));
        Assert.assertEquals(
                Map.of("a", "x", "s", "q\"\\/\b\f\n\r\tA\u00e9", "e", ""),
                S3ImdsCredentialsProvider.parseJsonObject(
                        "{\"a\":\"x\", \"n\" : -12.5E+3 ,\"t\":true,\"f\":false,\"z\":null,\r\n\t\"s\":\"q\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0041\\u00E9\",\"e\":\"\"}"
                )
        );
    }

    @Test
    public void testJsonObjectRejectsMalformedText() {
        assertJsonMalformed("malformed JSON [position=0, reason=unexpected end of text]", "");
        assertJsonMalformed("malformed JSON [position=1, reason=expected '{']", "[]");
        assertJsonMalformed("malformed JSON [position=1, reason=unexpected end of text]", "{");
        assertJsonMalformed("malformed JSON [position=2, reason=expected '\"']", "{a:1}");
        assertJsonMalformed("malformed JSON [position=6, reason=expected ':']", "{\"a\" \"b\"}");
        assertJsonMalformed("malformed JSON [position=5, reason=unsupported value]", "{\"a\":}");
        assertJsonMalformed("malformed JSON [position=5, reason=unsupported value]", "{\"a\":{}}");
        assertJsonMalformed("malformed JSON [position=8, reason=expected ',' or '}']", "{\"a\":1 2}");
        assertJsonMalformed("malformed JSON [position=8, reason=unexpected text after the object]", "{\"a\":1} x");
        assertJsonMalformed("malformed JSON [position=7, reason=unexpected end of text]", "{\"a\":\"x");
        assertJsonMalformed("malformed JSON [position=7, reason=control character in string]", "{\"a\":\"\u0001\"}");
        assertJsonMalformed("malformed JSON [position=8, reason=invalid escape]", "{\"a\":\"\\q\"}");
        assertJsonMalformed("malformed JSON [position=11, reason=invalid unicode escape]", "{\"a\":\"\\u12\"}");
        assertJsonMalformed("malformed JSON [position=8, reason=truncated unicode escape]", "{\"a\":\"\\u1");
        assertJsonMalformed("malformed JSON [position=7, reason=unexpected end of text]", "{\"a\":\"\\");
        assertJsonMalformed("malformed JSON [position=6, reason=unexpected end of text]", "{\"a\":1");
    }

    private static ObjectStoreException assertFailsWithPrefix(String prefix, Runnable action) {
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

    private static void assertJsonMalformed(String message, String json) {
        try {
            S3ImdsCredentialsProvider.parseJsonObject(json);
            Assert.fail("expected IllegalArgumentException for " + json);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(json, message, e.getMessage());
        }
    }

    private S3ImdsCredentialsProvider newProvider(long timeoutMillis) {
        return new S3ImdsCredentialsProvider(httpClient, imds.getEndpoint(), timeoutMillis, clock::get);
    }
}
