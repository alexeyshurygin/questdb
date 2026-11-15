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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.replication.ReplicationConfiguration;
import io.questdb.replication.store.ByteSink;
import io.questdb.replication.store.ObjectStore;
import io.questdb.replication.store.ObjectStoreException;
import io.questdb.replication.store.ObjectStoreSettings;
import io.questdb.replication.store.ObjectStoreUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Object store on Amazon S3 or an S3-compatible service, configured with
 * {@code s3::bucket=<bucket>;root=<prefix>;region=<region>;endpoint=<url>;enable_virtual_host_style=<bool>;}
 * and optional credentials (see {@link S3CredentialsProvider#fromSettings}).
 * <p>
 * Without {@code endpoint}, requests go to AWS at {@code https://s3.<region>.amazonaws.com} and
 * {@code region} is required. With {@code endpoint} (http or https, for example MinIO at
 * {@code http://127.0.0.1:9000}), {@code region} defaults to {@code us-east-1}. Requests address
 * the bucket in the host name (virtual-host style) by default when there is no endpoint, and in
 * the path when there is one; {@code enable_virtual_host_style} overrides that choice.
 * <p>
 * Each call is one logical request. The store signs every request with AWS Signature Version 4,
 * including the SHA-256 of the payload, so S3 verifies what it receives. It retries I/O errors,
 * timeouts, credential fetch failures and the statuses 408, 429 and 5xx
 * {@code retryAttempts} times, sleeping {@code retryInterval} before the first retry and doubling
 * the pause up to ten times {@code retryInterval}, and then throws a
 * {@link ObjectStoreException.Kind#RETRYABLE} exception. 409 and 412 are
 * {@link ObjectStoreException.Kind#PRECONDITION_FAILED}. 404 means
 * {@link ObjectStoreException.Kind#NOT_FOUND} for a read, success for a delete and a failed
 * precondition for an {@code If-Match} write, except that a missing bucket ({@code NoSuchBucket})
 * is always {@link ObjectStoreException.Kind#FATAL}. 400, 401, 403 and other unexpected statuses
 * are {@link ObjectStoreException.Kind#FATAL}. Messages carry the S3 error code, never
 * credentials.
 * <p>
 * Timeouts: a request times out after {@code requestBaseTimeout} plus the time its body takes at
 * {@code minThroughput} bytes per second. For an upload the body is the payload. For a download
 * the size becomes known only with the response headers: the request first waits
 * {@code requestBaseTimeout} and, once the {@code Content-Length} of the response is known,
 * extends the deadline to {@code requestBaseTimeout} plus that length at {@code minThroughput},
 * both counted from the start of the attempt. The HTTP client enforces the wait for the response
 * headers itself; the store's own deadline, which also bounds the body, allows 100ms more.
 * <p>
 * The store is thread-safe; it shares one HTTP/1.1 client between all calls and runs at most
 * {@code maxConcurrentRequests} requests at a time.
 */
public class S3ObjectStore implements ObjectStore {
    public static final int DEFAULT_CLEANER_MAX_REQUESTS_PER_SECOND = 1750;
    public static final String DEFAULT_ENDPOINT_REGION = "us-east-1";
    /**
     * Error and listing responses beyond this size are truncated; S3 sends far less.
     */
    public static final int MAX_BUFFERED_RESPONSE_SIZE = 4 * 1024 * 1024;
    // stands in for the body of a response that went into the caller's sink; never written
    private static final ByteSink EMPTY_BODY = new ByteSink(16);
    private static final Log LOG = LogFactory.getLog(S3ObjectStore.class);
    private static final int MAX_BACKOFF_MULTIPLIER = 10;
    private static final long MAX_OBJECT_SIZE = Integer.MAX_VALUE - 8;
    private static final long TIMEOUT_GRACE_MILLIS = 100;
    private final String baseUrl;
    private final String bucketPath;
    private final S3CredentialsProvider credentialsProvider;
    private final String description;
    private final String hostHeader;
    private final HttpClient httpClient;
    private final boolean isHttpClientOwned;
    private final String keyPrefix;
    private final long minThroughput;
    private final long requestBaseTimeout;
    private final int requestBufferSize;
    private final Semaphore requestPermits;
    private final int retryAttempts;
    private final long retryInterval;
    private final S3RequestSigner signer;

    public S3ObjectStore(@NotNull ObjectStoreSettings settings, @NotNull ReplicationConfiguration configuration, @NotNull Map<String, String> env) {
        this(settings, configuration, env, null);
    }

    /**
     * @param env        the process environment, read for credentials
     * @param httpClient client to send requests with, null to create one that {@link #close()}
     *                   shuts down
     */
    public S3ObjectStore(
            @NotNull ObjectStoreSettings settings,
            @NotNull ReplicationConfiguration configuration,
            @NotNull Map<String, String> env,
            @Nullable HttpClient httpClient
    ) {
        if (!ObjectStoreSettings.SCHEME_S3.equals(settings.getScheme())) {
            throw ObjectStoreException.fatal("object store settings are not for the S3 store [scheme=" + settings.getScheme() + ']');
        }
        this.retryAttempts = Math.max(0, configuration.getRetryAttempts());
        this.retryInterval = Math.max(0, configuration.getRetryInterval());
        this.requestBaseTimeout = Math.max(1, configuration.getRequestBaseTimeout());
        this.minThroughput = configuration.getMinThroughput();
        this.requestBufferSize = Math.max(256, configuration.getRequestBufferSize());
        this.requestPermits = new Semaphore(Math.max(1, configuration.getMaxConcurrentRequests()));

        final String bucket = settings.get(ObjectStoreSettings.KEY_BUCKET);
        assert bucket != null : "ObjectStoreSettings.parse() requires the bucket";
        final String root = stripSlashes(settings.get(ObjectStoreSettings.KEY_ROOT));
        this.keyPrefix = root.isEmpty() ? "" : root + '/';
        final String endpoint = settings.get(ObjectStoreSettings.KEY_ENDPOINT);
        final String virtualHostSetting = settings.get(ObjectStoreSettings.KEY_ENABLE_VIRTUAL_HOST_STYLE);
        final boolean isVirtualHost = virtualHostSetting != null ? Boolean.parseBoolean(virtualHostSetting) : endpoint == null;

        String region = settings.get(ObjectStoreSettings.KEY_REGION);
        final String scheme;
        String host;
        int port;
        if (endpoint == null) {
            if (region == null) {
                throw ObjectStoreException.fatal("object store setting region is required when endpoint is not set");
            }
            if (!isDnsName(region)) {
                throw ObjectStoreException.fatal("object store setting region is not a valid AWS region [region=" + region + ']');
            }
            scheme = "https";
            host = "s3." + region + ".amazonaws.com";
            port = -1;
        } else {
            final URI uri = parseEndpoint(endpoint);
            scheme = uri.getScheme().toLowerCase();
            host = uri.getHost();
            port = uri.getPort();
            if (port == ("https".equals(scheme) ? 443 : 80)) {
                port = -1;
            }
            if (region == null) {
                region = DEFAULT_ENDPOINT_REGION;
            }
        }
        if (isVirtualHost) {
            if (!isDnsName(bucket)) {
                throw ObjectStoreException.fatal("bucket name cannot be used in a host name, set enable_virtual_host_style=false [bucket="
                        + bucket + ']');
            }
            host = bucket + '.' + host;
            this.bucketPath = "";
        } else {
            this.bucketPath = '/' + S3RequestSigner.uriEncode(bucket, false);
        }
        // HttpClient sends the port in the Host header only when it is not the scheme's default
        this.hostHeader = port == -1 ? host : host + ':' + port;
        this.baseUrl = scheme + "://" + hostHeader;
        this.signer = new S3RequestSigner(region);
        this.description = "s3 bucket=" + bucket + (root.isEmpty() ? "" : ", root=" + root) + ", region=" + region
                + ", endpoint=" + baseUrl + ", style=" + (isVirtualHost ? "virtual-host" : "path");

        this.isHttpClientOwned = httpClient == null;
        this.httpClient = httpClient != null ? httpClient : newHttpClient(requestBaseTimeout);
        try {
            this.credentialsProvider = S3CredentialsProvider.fromSettings(settings, env, this.httpClient, requestBaseTimeout);
        } catch (ObjectStoreException e) {
            if (isHttpClientOwned) {
                this.httpClient.shutdownNow();
            }
            throw e;
        }
    }

    /**
     * Encodes query parameters as SigV4 requires: names and values URI-encoded, sorted by name.
     */
    public static @NotNull String canonicalQuery(@NotNull TreeMap<String, String> parameters) {
        final StringBuilder b = new StringBuilder();
        for (Map.Entry<String, String> parameter : parameters.entrySet()) {
            if (!b.isEmpty()) {
                b.append('&');
            }
            b.append(S3RequestSigner.uriEncode(parameter.getKey(), false))
                    .append('=')
                    .append(S3RequestSigner.uriEncode(parameter.getValue(), false));
        }
        return b.toString();
    }

    @Override
    public void close() {
        if (isHttpClientOwned) {
            httpClient.shutdownNow();
        }
    }

    @Override
    public void delete(@NotNull String key) {
        ObjectStoreUtils.validateKey(key);
        final S3Request request = new S3Request("DELETE", objectPath(key), "", key);
        final S3Response response = execute(request);
        final int status = response.status;
        if (status == 200 || status == 204 || (status == 404 && !response.isNoSuchBucket())) {
            return;
        }
        throw failure(request, response);
    }

    @Override
    public @Nullable String get(@NotNull String key, @Nullable String ifNoneMatch, @NotNull ByteSink sink) {
        ObjectStoreUtils.validateKey(key);
        final String etag = ObjectStoreUtils.stripQuotes(ifNoneMatch);
        final S3Request request = new S3Request("GET", objectPath(key), "", key);
        request.ifNoneMatch = etag != null ? '"' + etag + '"' : null;
        request.okSink = sink;
        final S3Response response = execute(request);
        return switch (response.status) {
            case 200 -> responseETag(request, response);
            case 304 -> null;
            case 404 -> {
                if (response.isNoSuchBucket()) {
                    throw failure(request, response);
                }
                throw ObjectStoreException.notFound(key);
            }
            default -> throw failure(request, response);
        };
    }

    @Override
    public int getDefaultCleanerMaxRequestsPerSecond() {
        return DEFAULT_CLEANER_MAX_REQUESTS_PER_SECOND;
    }

    @Override
    public @NotNull String getDescription() {
        return description;
    }

    @Override
    public boolean isIntegrityChecked() {
        return true;
    }

    @Override
    public void list(@NotNull String prefix, @Nullable String startAfter, @NotNull ListConsumer consumer) {
        final String fullPrefix = keyPrefix + prefix;
        final String listPath = bucketPath.isEmpty() ? "/" : bucketPath;
        String continuationToken = null;
        while (true) {
            final TreeMap<String, String> parameters = new TreeMap<>();
            parameters.put("list-type", "2");
            if (!fullPrefix.isEmpty()) {
                parameters.put("prefix", fullPrefix);
            }
            if (continuationToken != null) {
                parameters.put("continuation-token", continuationToken);
            } else if (startAfter != null) {
                parameters.put("start-after", keyPrefix + startAfter);
            }
            final S3Request request = new S3Request("GET", listPath, canonicalQuery(parameters), null);
            request.operation = "LIST";
            request.subject = "prefix=" + prefix;
            final S3Response response = execute(request);
            if (response.status != 200) {
                throw failure(request, response);
            }
            final S3ListPage page;
            try {
                page = S3ListPage.parse(response.getBodyAsString());
            } catch (IllegalArgumentException e) {
                throw ObjectStoreException.fatal("malformed ListObjectsV2 response [prefix=" + prefix + ", reason=" + e.getMessage() + ']');
            }
            for (int i = 0, n = page.size(); i < n; i++) {
                final String key = page.getKey(i);
                // S3 lists only keys under the requested prefix; skip anything else a server sends
                if (key.startsWith(fullPrefix) && !consumer.onKey(key.substring(keyPrefix.length()), page.getSize(i))) {
                    return;
                }
            }
            if (!page.isTruncated()) {
                return;
            }
            continuationToken = page.getNextContinuationToken();
        }
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
        ObjectStoreUtils.validateKey(key);
        final String expected = ObjectStoreUtils.stripQuotes(ifMatch);
        final S3Request request = new S3Request("PUT", objectPath(key), "", key);
        request.body = data;
        request.bodyOffset = offset;
        request.bodyLength = length;
        request.payloadSha256 = ObjectStoreUtils.sha256Hex(data, offset, length);
        request.ifMatch = expected != null ? '"' + expected + '"' : null;
        request.ifNoneMatch = isIfNoneMatch ? "*" : null;
        final S3Response response = execute(request);
        final int status = response.status;
        if (status == 200) {
            return responseETag(request, response);
        }
        // S3 answers If-Match on a missing object with 404 NoSuchKey
        if (status == 404 && expected != null && !response.isNoSuchBucket()) {
            throw failure(request, response, true);
        }
        throw failure(request, response);
    }

    private static String codeSuffix(S3Response response) {
        final String code = response.getErrorCode();
        return code != null ? ", code=" + code : "";
    }

    private static ObjectStoreException failure(S3Request request, S3Response response) {
        return failure(request, response, response.status == 409 || response.status == 412);
    }

    private static ObjectStoreException failure(S3Request request, S3Response response, boolean isPreconditionFailed) {
        final String details = " [op=" + request.operation + ", " + request.subject + ", status=" + response.status + codeSuffix(response) + ']';
        return isPreconditionFailed
                ? new ObjectStoreException(ObjectStoreException.Kind.PRECONDITION_FAILED, response.status, "object store precondition failed" + details)
                : new ObjectStoreException(ObjectStoreException.Kind.FATAL, response.status, "object store request failed" + details);
    }

    private static ObjectStoreException invalidEndpoint(String endpoint) {
        return ObjectStoreException.fatal("object store setting endpoint must be http://host[:port] or https://host[:port] [endpoint="
                + endpoint + ']');
    }

    // ObjectStoreSettings.parse() rejects empty values, so the name is never empty
    private static boolean isDnsName(String name) {
        for (int i = 0, n = name.length(); i < n; i++) {
            final char c = name.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.')) {
                return false;
            }
        }
        return true;
    }

    private static boolean isRetryableStatus(int status) {
        return status == 408 || status == 429 || status >= 500;
    }

    private static HttpClient newHttpClient(long connectTimeoutMillis) {
        return HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(connectTimeoutMillis))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    private static URI parseEndpoint(String endpoint) {
        final URI uri;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw invalidEndpoint(endpoint);
        }
        final String scheme = uri.getScheme();
        // an opaque URI has no host and fails before its null path is looked at
        if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                || uri.getHost() == null
                || uri.getRawUserInfo() != null
                || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null) {
            throw invalidEndpoint(endpoint);
        }
        return uri;
    }

    private static String responseETag(S3Request request, S3Response response) {
        final String etag = ObjectStoreUtils.stripQuotes(response.headers.firstValue("ETag").orElse(null));
        if (etag == null || etag.isEmpty()) {
            throw ObjectStoreException.fatal("object store response lacks an ETag [op=" + request.operation + ", " + request.subject + ']');
        }
        return etag;
    }

    private static String stripSlashes(String value) {
        if (value == null) {
            return "";
        }
        int lo = 0;
        int hi = value.length();
        while (lo < hi && value.charAt(lo) == '/') {
            lo++;
        }
        while (hi > lo && value.charAt(hi - 1) == '/') {
            hi--;
        }
        return value.substring(lo, hi);
    }

    // The HttpRequest timeout covers the wait for the response headers and normally fires first;
    // this deadline, a little later, also bounds the response body.
    private HttpResponse<ByteSink> await(CompletableFuture<HttpResponse<ByteSink>> future, Exchange exchange, long timeoutMillis) {
        final long start = System.nanoTime();
        long timeout = timeoutMillis;
        try {
            while (true) {
                try {
                    final long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeout + TIMEOUT_GRACE_MILLIS);
                    return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                } catch (TimeoutException e) {
                    // the response headers may have announced a body that needs more time
                    final long extended = timeoutMillis + transferMillis(exchange.contentLength);
                    if (extended > timeout) {
                        timeout = extended;
                        continue;
                    }
                    throw timedOut(timeout, e);
                }
            }
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ObjectStoreException ose) {
                throw ose;
            }
            if (cause instanceof HttpTimeoutException) {
                throw timedOut(timeoutMillis, cause);
            }
            throw ObjectStoreException.retryable("I/O error [error=" + cause + ']', cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ObjectStoreException.retryable("request interrupted", e);
        } finally {
            // stop a late or failed attempt from writing into the caller's sink, and free its connection
            exchange.abandon();
            future.cancel(true);
        }
    }

    private S3Response execute(S3Request request) {
        long pause = retryInterval;
        for (int attempt = 1; ; attempt++) {
            ObjectStoreException failure;
            try {
                final S3Response response = send(request);
                if (!isRetryableStatus(response.status)) {
                    return response;
                }
                failure = new ObjectStoreException(
                        ObjectStoreException.Kind.RETRYABLE,
                        response.status,
                        "status=" + response.status + codeSuffix(response)
                );
            } catch (ObjectStoreException e) {
                if (!e.isRetryable()) {
                    throw e;
                }
                failure = e;
            }
            if (attempt > retryAttempts) {
                throw new ObjectStoreException(
                        ObjectStoreException.Kind.RETRYABLE,
                        failure.getStatus(),
                        "object store request failed [op=" + request.operation + ", " + request.subject + ", attempts=" + attempt
                                + ", reason=" + failure.getMessage() + ']',
                        failure
                );
            }
            LOG.info().$("S3 request failed, retrying [op=").$(request.operation)
                    .$(", ").$(request.subject)
                    .$(", attempt=").$(attempt)
                    .$(", reason=").$(failure.getMessage())
                    .I$();
            ObjectStoreUtils.pause(pause);
            pause = Math.min(pause * 2, retryInterval * MAX_BACKOFF_MULTIPLIER);
        }
    }

    private String objectPath(String key) {
        return bucketPath + '/' + S3RequestSigner.uriEncode(keyPrefix + key, true);
    }

    private S3Response send(S3Request request) {
        final S3Credentials credentials = credentialsProvider.getCredentials();
        final String amzDate = S3RequestSigner.formatAmzDate(System.currentTimeMillis());
        final String sessionToken = credentials.getSessionToken();
        final TreeMap<String, String> signedHeaders = new TreeMap<>();
        signedHeaders.put("host", hostHeader);
        signedHeaders.put("x-amz-content-sha256", request.payloadSha256);
        signedHeaders.put("x-amz-date", amzDate);
        if (sessionToken != null) {
            signedHeaders.put("x-amz-security-token", sessionToken);
        }
        final String authorization = signer.authorization(
                request.method,
                request.path,
                request.query,
                signedHeaders,
                request.payloadSha256,
                amzDate,
                credentials
        );
        final long timeoutMillis = requestBaseTimeout + transferMillis(request.bodyLength);
        final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(baseUrl + request.path + (request.query.isEmpty() ? "" : "?" + request.query)))
                .timeout(Duration.ofMillis(timeoutMillis))
                .method(
                        request.method,
                        request.body != null
                                ? HttpRequest.BodyPublishers.ofByteArray(request.body, request.bodyOffset, request.bodyLength)
                                : HttpRequest.BodyPublishers.noBody()
                )
                .header("Authorization", authorization)
                .header("x-amz-content-sha256", request.payloadSha256)
                .header("x-amz-date", amzDate);
        if (sessionToken != null) {
            builder.header("x-amz-security-token", sessionToken);
        }
        if (request.ifMatch != null) {
            builder.header("If-Match", request.ifMatch);
        }
        if (request.ifNoneMatch != null) {
            builder.header("If-None-Match", request.ifNoneMatch);
        }
        final HttpRequest httpRequest = builder.build();
        try {
            requestPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ObjectStoreException.retryable("request interrupted", e);
        }
        try {
            final Exchange exchange = new Exchange(request.okSink, requestBufferSize, request.subject);
            final HttpResponse<ByteSink> response = await(httpClient.sendAsync(httpRequest, exchange), exchange, timeoutMillis);
            // the caller's sink holds a successful body; error handling never reads it
            return new S3Response(response.statusCode(), response.headers(), exchange.isOkSinkUsed ? EMPTY_BODY : response.body());
        } finally {
            requestPermits.release();
        }
    }

    private ObjectStoreException timedOut(long timeoutMillis, Throwable cause) {
        return ObjectStoreException.retryable("request timed out [timeout=" + timeoutMillis + "ms]", cause);
    }

    private long transferMillis(long bytes) {
        return minThroughput > 0 && bytes > 0 ? bytes * 1000 / minThroughput : 0;
    }

    /**
     * Collects a response body into a {@link ByteSink}. Writes happen under the exchange's monitor
     * and stop once the exchange is abandoned, so a timed-out attempt cannot touch the caller's
     * sink while the next attempt fills it.
     */
    private static final class BodyCollector implements HttpResponse.BodySubscriber<ByteSink> {
        private final CompletableFuture<ByteSink> body = new CompletableFuture<>();
        private final long contentLength;
        private final Exchange exchange;
        private final boolean isCallerSink;
        private final ByteSink sink;
        private Flow.Subscription subscription;

        BodyCollector(Exchange exchange, ByteSink sink, boolean isCallerSink, long contentLength) {
            this.exchange = exchange;
            this.sink = sink;
            this.isCallerSink = isCallerSink;
            this.contentLength = contentLength;
        }

        @Override
        public CompletableFuture<ByteSink> getBody() {
            return body;
        }

        @Override
        public void onComplete() {
            body.complete(sink);
        }

        @Override
        public void onError(Throwable throwable) {
            body.completeExceptionally(throwable);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            synchronized (exchange) {
                if (exchange.isAbandoned || body.isDone()) {
                    subscription.cancel();
                    return;
                }
                try {
                    for (int i = 0, n = buffers.size(); i < n; i++) {
                        final ByteBuffer buffer = buffers.get(i);
                        int length = buffer.remaining();
                        if (!isCallerSink) {
                            length = Math.min(length, MAX_BUFFERED_RESPONSE_SIZE - sink.size());
                        }
                        if (length > 0) {
                            final byte[] target = sink.ensureCapacity(length);
                            buffer.get(target, sink.size(), length);
                            sink.advance(length);
                        }
                    }
                } catch (ObjectStoreException e) {
                    subscription.cancel();
                    body.completeExceptionally(e);
                }
            }
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (isCallerSink) {
                if (contentLength > MAX_OBJECT_SIZE) {
                    subscription.cancel();
                    body.completeExceptionally(ObjectStoreException.fatal("object too large to buffer [" + exchange.subject + ", size=" + contentLength + ']'));
                    return;
                }
                synchronized (exchange) {
                    if (!exchange.isAbandoned) {
                        sink.clear();
                    }
                }
            }
            subscription.request(Long.MAX_VALUE);
        }
    }

    /**
     * One attempt of a request: picks where the response body goes once the status is known.
     */
    private static final class Exchange implements HttpResponse.BodyHandler<ByteSink> {
        private final int bufferSize;
        private final ByteSink okSink;
        private final String subject;
        private volatile long contentLength = -1;
        // guarded by this
        private boolean isAbandoned;
        private volatile boolean isOkSinkUsed;

        Exchange(ByteSink okSink, int bufferSize, String subject) {
            this.okSink = okSink;
            this.bufferSize = bufferSize;
            this.subject = subject;
        }

        @Override
        public HttpResponse.BodySubscriber<ByteSink> apply(HttpResponse.ResponseInfo info) {
            final long length = info.headers().firstValueAsLong("Content-Length").orElse(-1L);
            contentLength = length;
            if (okSink != null && info.statusCode() == 200) {
                isOkSinkUsed = true;
                return new BodyCollector(this, okSink, true, length);
            }
            final int capacity = length >= 0 ? (int) Math.min(length, MAX_BUFFERED_RESPONSE_SIZE) : bufferSize;
            return new BodyCollector(this, new ByteSink(capacity), false, length);
        }

        synchronized void abandon() {
            isAbandoned = true;
        }
    }

    private static final class S3Request {
        final String method;
        final String path;
        final String query;
        byte[] body;
        int bodyLength;
        int bodyOffset;
        String ifMatch;
        String ifNoneMatch;
        ByteSink okSink;
        String operation;
        String payloadSha256 = S3RequestSigner.EMPTY_PAYLOAD_SHA256;
        String subject;

        S3Request(String method, String path, String query, String key) {
            this.method = method;
            this.path = path;
            this.query = query;
            this.operation = method;
            this.subject = "key=" + key;
        }
    }

    private static final class S3Response {
        final ByteSink body;
        final HttpHeaders headers;
        final int status;
        private String errorCode;
        private boolean isErrorCodeParsed;

        S3Response(int status, HttpHeaders headers, ByteSink body) {
            this.status = status;
            this.headers = headers;
            this.body = body;
        }

        String getBodyAsString() {
            return new String(body.data(), 0, body.size(), StandardCharsets.UTF_8);
        }

        String getErrorCode() {
            if (!isErrorCodeParsed) {
                isErrorCodeParsed = true;
                final String text = getBodyAsString();
                errorCode = text.isEmpty() ? null : S3XmlParser.findErrorCode(text);
            }
            return errorCode;
        }

        boolean isNoSuchBucket() {
            return "NoSuchBucket".equals(getErrorCode());
        }
    }
}
