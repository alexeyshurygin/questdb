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

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory S3 for tests: PutObject (with If-None-Match: * and If-Match), GetObject (with
 * If-None-Match), DeleteObject and ListObjectsV2 with paging. Every request must carry a valid
 * AWS Signature Version 4 Authorization header; the server recomputes the signature from the bytes
 * it received with its own code, independent of the signer under test, and checks the payload
 * hash. Tests script faults with {@link #addFault}.
 */
public class FakeS3Server implements Closeable {
    public static final String ACCESS_KEY_ID = "AKIDFAKES3TEST";
    public static final String BUCKET = "test-bucket";
    public static final String REGION = "us-east-1";
    public static final String SECRET_ACCESS_KEY = "fake/secret+key=";
    private final Map<String, String[]> credentials = new ConcurrentHashMap<>();
    private final Deque<Fault> faults = new ArrayDeque<>();
    private final TestHttpServer http;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();
    private final TreeMap<String, byte[]> objects = new TreeMap<>(FakeS3Server::compareUtf8);
    private final List<RecordedRequest> requests = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger signatureFailures = new AtomicInteger();
    private volatile boolean isNumericEscapes;
    private volatile boolean isVirtualHost;
    private volatile int pageSize = 1000;
    private volatile String region = REGION;

    public FakeS3Server() throws IOException {
        credentials.put(ACCESS_KEY_ID, new String[]{SECRET_ACCESS_KEY, null});
        http = new TestHttpServer(this::handle);
    }

    public static String md5Hex(byte[] data) {
        try {
            return hex(MessageDigest.getInstance("MD5").digest(data));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public static String sha256Hex(byte[] data) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public void addCredentials(String accessKeyId, String secretAccessKey, String sessionToken) {
        credentials.put(accessKeyId, new String[]{secretAccessKey, sessionToken});
    }

    /**
     * Applies {@code fault} to the next request with the given method, or any method when null.
     */
    public void addFault(String method, Fault fault) {
        synchronized (faults) {
            fault.method = method;
            faults.add(fault);
        }
    }

    @Override
    public void close() throws IOException {
        http.close();
    }

    public String getEndpoint() {
        return http.getBaseUrl();
    }

    public byte[] getObject(String key) {
        synchronized (objects) {
            return objects.get(key);
        }
    }

    /**
     * Most requests the server has handled at the same time.
     */
    public int getMaxInFlight() {
        return maxInFlight.get();
    }

    public int getPendingFaultCount() {
        synchronized (faults) {
            return faults.size();
        }
    }

    public int getPort() {
        return http.getPort();
    }

    public List<RecordedRequest> getRequests() {
        synchronized (requests) {
            return new ArrayList<>(requests);
        }
    }

    public int getSignatureFailures() {
        return signatureFailures.get();
    }

    public void putObject(String key, byte[] data) {
        synchronized (objects) {
            objects.put(key, data);
        }
    }

    public void setKeepAlive(boolean isKeepAlive) {
        http.setKeepAlive(isKeepAlive);
    }

    public void setNumericEscapes(boolean isNumericEscapes) {
        this.isNumericEscapes = isNumericEscapes;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setVirtualHost(boolean isVirtualHost) {
        this.isVirtualHost = isVirtualHost;
    }

    // S3 lists keys in UTF-8 binary order
    private static int compareUtf8(String a, String b) {
        return java.util.Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static String errorXml(String code) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" + code + "</Code><Message>fake s3 error</Message>"
                + "<RequestId>REQ1</RequestId></Error>";
    }

    private static String hex(byte[] bytes) {
        final StringBuilder b = new StringBuilder();
        for (byte x : bytes) {
            b.append(String.format("%02x", x & 0xff));
        }
        return b.toString();
    }

    private static byte[] hmac(byte[] key, String data) throws Exception {
        final Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String percentDecode(String s) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < s.length(); i++) {
            final char c = s.charAt(i);
            if (c == '%') {
                out.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 2;
            } else {
                final byte[] bytes = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
                out.write(bytes, 0, bytes.length);
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    // independent RFC 3986 encoder for the server's canonical query
    private static String percentEncode(String s) {
        final StringBuilder b = new StringBuilder();
        for (byte x : s.getBytes(StandardCharsets.UTF_8)) {
            final char c = (char) (x & 0xff);
            if (Character.isLetterOrDigit(c) && c < 128 || c == '-' || c == '_' || c == '.' || c == '~') {
                b.append(c);
            } else {
                b.append('%').append(String.format("%02X", x & 0xff));
            }
        }
        return b.toString();
    }

    private static Map<String, String> queryParameters(String rawQuery) {
        final Map<String, String> parameters = new HashMap<>();
        if (!rawQuery.isEmpty()) {
            for (String pair : rawQuery.split("&")) {
                final int eq = pair.indexOf('=');
                parameters.put(percentDecode(eq < 0 ? pair : pair.substring(0, eq)), eq < 0 ? "" : percentDecode(pair.substring(eq + 1)));
            }
        }
        return parameters;
    }

    private String escapeXml(String s) {
        final StringBuilder b = new StringBuilder();
        s.codePoints().forEach(cp -> {
            switch (cp) {
                case '&' -> b.append("&amp;");
                case '<' -> b.append("&lt;");
                case '>' -> b.append("&gt;");
                case '"' -> b.append("&quot;");
                case '\'' -> b.append("&apos;");
                default -> {
                    if (cp > 0x7e && isNumericEscapes) {
                        b.append(cp % 2 == 0 ? "&#" + cp + ';' : "&#x" + Integer.toHexString(cp) + ';');
                    } else {
                        b.appendCodePoint(cp);
                    }
                }
            }
        });
        return b.toString();
    }

    private void handle(TestHttpServer.Request request, TestHttpServer.Exchange exchange) throws IOException {
        maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        try {
            handle0(request, exchange);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private void handle0(TestHttpServer.Request request, TestHttpServer.Exchange exchange) throws IOException {
        requests.add(new RecordedRequest(request));
        Fault fault = null;
        synchronized (faults) {
            final Fault head = faults.peek();
            if (head != null && (head.method == null || head.method.equals(request.method))) {
                fault = faults.poll();
            }
        }
        if (fault != null) {
            switch (fault.kind) {
                case STATUS -> {
                    respondError(exchange, fault.status, fault.code);
                    return;
                }
                case CLOSE -> {
                    exchange.closeAbruptly();
                    return;
                }
                case DELAY_AND_DROP -> {
                    sleep(fault.millis);
                    exchange.closeAbruptly();
                    return;
                }
                case DELAY_AND_PROCESS -> sleep(fault.millis);
                case RAW -> {
                    exchange.writeRawAndClose(fault.raw);
                    return;
                }
                case RESPONSE -> {
                    exchange.respond(fault.status, fault.headers, fault.body);
                    return;
                }
                default -> {
                    // STALL_BODY and SLOW_BODY shape the response below
                }
            }
        }
        final String authError = verifySignature(request);
        if (authError != null) {
            signatureFailures.incrementAndGet();
            respondError(exchange, authError.equals("XAmzContentSHA256Mismatch") ? 400 : 403, authError);
            return;
        }
        String path = request.path;
        final String host = request.header("host");
        final String bucket;
        if (isVirtualHost) {
            bucket = host.substring(0, host.indexOf('.'));
        } else {
            final int slash = path.indexOf('/', 1);
            bucket = percentDecode(slash < 0 ? path.substring(1) : path.substring(1, slash));
            path = slash < 0 ? "/" : path.substring(slash);
        }
        if (!BUCKET.equals(bucket)) {
            respondError(exchange, 404, "NoSuchBucket");
            return;
        }
        final String key = percentDecode(path.substring(1));
        if (key.isEmpty() && "GET".equals(request.method)) {
            handleList(request, exchange);
        } else if ("PUT".equals(request.method)) {
            handlePut(request, exchange, key);
        } else if ("GET".equals(request.method)) {
            handleGet(request, exchange, key, fault);
        } else if ("DELETE".equals(request.method)) {
            synchronized (objects) {
                objects.remove(key);
            }
            exchange.respond(204, Map.of(), new byte[0]);
        } else {
            respondError(exchange, 405, "MethodNotAllowed");
        }
    }

    private void handleGet(TestHttpServer.Request request, TestHttpServer.Exchange exchange, String key, Fault fault) throws IOException {
        final byte[] data;
        synchronized (objects) {
            data = objects.get(key);
        }
        if (data == null) {
            respondError(exchange, 404, "NoSuchKey");
            return;
        }
        final String etag = '"' + md5Hex(data) + '"';
        if (etag.equals(request.header("if-none-match"))) {
            exchange.respond(304, Map.of("ETag", etag), new byte[0]);
            return;
        }
        if (fault != null && fault.kind == FaultKind.STALL_BODY) {
            exchange.respondSlowly(200, Map.of("ETag", etag), data, data.length / 2, 1, fault.millis);
        } else if (fault != null && fault.kind == FaultKind.SLOW_BODY) {
            exchange.respondSlowly(200, Map.of("ETag", etag), data, data.length, fault.status, fault.millis);
        } else {
            exchange.respond(200, Map.of("ETag", etag), data);
        }
    }

    private void handleList(TestHttpServer.Request request, TestHttpServer.Exchange exchange) throws IOException {
        final Map<String, String> parameters = queryParameters(request.rawQuery);
        if (!"2".equals(parameters.get("list-type"))) {
            respondError(exchange, 400, "InvalidArgument");
            return;
        }
        final String prefix = parameters.getOrDefault("prefix", "");
        final String token = parameters.get("continuation-token");
        final String after = token != null
                ? new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8)
                : parameters.get("start-after");
        final List<Map.Entry<String, byte[]>> page = new ArrayList<>();
        boolean isTruncated = false;
        synchronized (objects) {
            final NavigableMap<String, byte[]> tail = after != null ? objects.tailMap(after, false) : objects;
            for (Map.Entry<String, byte[]> entry : tail.entrySet()) {
                if (!entry.getKey().startsWith(prefix)) {
                    continue;
                }
                if (page.size() == pageSize) {
                    isTruncated = true;
                    break;
                }
                page.add(Map.entry(entry.getKey(), entry.getValue()));
            }
        }
        final StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        xml.append("<Name>").append(BUCKET).append("</Name>");
        xml.append("<Prefix>").append(escapeXml(prefix)).append("</Prefix>");
        xml.append("<KeyCount>").append(page.size()).append("</KeyCount>");
        xml.append("<MaxKeys>").append(pageSize).append("</MaxKeys>");
        xml.append("<IsTruncated>").append(isTruncated).append("</IsTruncated>");
        if (token != null) {
            xml.append("<ContinuationToken>").append(token).append("</ContinuationToken>");
        }
        for (Map.Entry<String, byte[]> entry : page) {
            xml.append("\n  <Contents>")
                    .append("<Key>").append(escapeXml(entry.getKey())).append("</Key>")
                    .append("<LastModified>2026-01-01T00:00:00.000Z</LastModified>")
                    .append("<ETag>&quot;").append(md5Hex(entry.getValue())).append("&quot;</ETag>")
                    .append("<Size>").append(entry.getValue().length).append("</Size>")
                    .append("<StorageClass>STANDARD</StorageClass>")
                    .append("</Contents>");
        }
        if (isTruncated) {
            final String last = page.get(page.size() - 1).getKey();
            xml.append("<NextContinuationToken>")
                    .append(Base64.getEncoder().encodeToString(last.getBytes(StandardCharsets.UTF_8)))
                    .append("</NextContinuationToken>");
        }
        xml.append("</ListBucketResult>");
        exchange.respond(200, Map.of("Content-Type", "application/xml"), xml.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void handlePut(TestHttpServer.Request request, TestHttpServer.Exchange exchange, String key) throws IOException {
        final String ifNoneMatch = request.header("if-none-match");
        final String ifMatch = request.header("if-match");
        synchronized (objects) {
            final byte[] existing = objects.get(key);
            if ("*".equals(ifNoneMatch) && existing != null) {
                respondError(exchange, 412, "PreconditionFailed");
                return;
            }
            if (ifMatch != null) {
                if (existing == null) {
                    respondError(exchange, 404, "NoSuchKey");
                    return;
                }
                if (!ifMatch.equals('"' + md5Hex(existing) + '"')) {
                    respondError(exchange, 412, "PreconditionFailed");
                    return;
                }
            }
            objects.put(key, request.body);
        }
        exchange.respond(200, Map.of("ETag", '"' + md5Hex(request.body) + '"'), new byte[0]);
    }

    private void respondError(TestHttpServer.Exchange exchange, int status, String code) throws IOException {
        exchange.respond(status, Map.of("Content-Type", "application/xml"), errorXml(code).getBytes(StandardCharsets.UTF_8));
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Returns null when the request carries a valid signature, else the S3 error code.
    private String verifySignature(TestHttpServer.Request request) {
        try {
            final String authorization = request.header("authorization");
            if (authorization == null || !authorization.startsWith("AWS4-HMAC-SHA256 ")) {
                return "AccessDenied";
            }
            final Map<String, String> fields = new HashMap<>();
            for (String part : authorization.substring("AWS4-HMAC-SHA256 ".length()).split(",")) {
                final int eq = part.indexOf('=');
                fields.put(part.substring(0, eq).trim(), part.substring(eq + 1).trim());
            }
            final String[] credential = fields.get("Credential").split("/");
            final String[] secret = credentials.get(credential[0]);
            if (secret == null) {
                return "InvalidAccessKeyId";
            }
            final String amzDate = request.header("x-amz-date");
            final String payloadHash = request.header("x-amz-content-sha256");
            if (!sha256Hex(request.body).equals(payloadHash)) {
                return "XAmzContentSHA256Mismatch";
            }
            final List<String> signedHeaders = List.of(fields.get("SignedHeaders").split(";"));
            if (credential.length != 5
                    || !credential[1].equals(amzDate.substring(0, 8))
                    || !credential[2].equals(region)
                    || !credential[3].equals("s3")
                    || !credential[4].equals("aws4_request")
                    || !signedHeaders.contains("host")
                    || !signedHeaders.contains("x-amz-date")
                    || !signedHeaders.contains("x-amz-content-sha256")) {
                return "AuthorizationHeaderMalformed";
            }
            if (secret[1] != null && (!secret[1].equals(request.header("x-amz-security-token")) || !signedHeaders.contains("x-amz-security-token"))) {
                return "InvalidToken";
            }
            final Map<String, String> query = new TreeMap<>();
            if (!request.rawQuery.isEmpty()) {
                for (String pair : request.rawQuery.split("&")) {
                    final int eq = pair.indexOf('=');
                    query.put(
                            percentEncode(percentDecode(eq < 0 ? pair : pair.substring(0, eq))),
                            eq < 0 ? "" : percentEncode(percentDecode(pair.substring(eq + 1)))
                    );
                }
            }
            final StringBuilder canonicalQuery = new StringBuilder();
            for (Map.Entry<String, String> parameter : query.entrySet()) {
                if (canonicalQuery.length() > 0) {
                    canonicalQuery.append('&');
                }
                canonicalQuery.append(parameter.getKey()).append('=').append(parameter.getValue());
            }
            final StringBuilder canonical = new StringBuilder();
            canonical.append(request.method).append('\n').append(request.path).append('\n').append(canonicalQuery).append('\n');
            for (String name : signedHeaders) {
                final String value = request.header(name);
                if (value == null) {
                    return "AuthorizationHeaderMalformed";
                }
                canonical.append(name).append(':').append(value.trim()).append('\n');
            }
            canonical.append('\n').append(fields.get("SignedHeaders")).append('\n').append(payloadHash);
            final String scope = credential[1] + '/' + region + "/s3/aws4_request";
            final String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + '\n' + scope + '\n'
                    + sha256Hex(canonical.toString().getBytes(StandardCharsets.UTF_8));
            byte[] key = hmac(("AWS4" + secret[0]).getBytes(StandardCharsets.UTF_8), credential[1]);
            key = hmac(key, region);
            key = hmac(key, "s3");
            key = hmac(key, "aws4_request");
            final String expected = hex(hmac(key, stringToSign));
            return expected.equals(fields.get("Signature")) ? null : "SignatureDoesNotMatch";
        } catch (Exception e) {
            return "AuthorizationHeaderMalformed";
        }
    }

    public enum FaultKind {
        CLOSE, DELAY_AND_DROP, DELAY_AND_PROCESS, RAW, RESPONSE, SLOW_BODY, STALL_BODY, STATUS
    }

    public static final class Fault {
        final FaultKind kind;
        byte[] body;
        String code;
        Map<String, String> headers;
        String method;
        long millis;
        String raw;
        int status;

        private Fault(FaultKind kind) {
            this.kind = kind;
        }

        /**
         * Resets the connection after reading the request.
         */
        public static Fault close() {
            return new Fault(FaultKind.CLOSE);
        }

        /**
         * Waits, then resets the connection without applying the request.
         */
        public static Fault delayAndDrop(long millis) {
            final Fault fault = new Fault(FaultKind.DELAY_AND_DROP);
            fault.millis = millis;
            return fault;
        }

        /**
         * Waits, then serves the request normally.
         */
        public static Fault delayAndProcess(long millis) {
            final Fault fault = new Fault(FaultKind.DELAY_AND_PROCESS);
            fault.millis = millis;
            return fault;
        }

        /**
         * Writes the given bytes as the whole response and closes the connection.
         */
        public static Fault raw(String raw) {
            final Fault fault = new Fault(FaultKind.RAW);
            fault.raw = raw;
            return fault;
        }

        /**
         * Sends this response without verifying or applying the request.
         */
        public static Fault response(int status, Map<String, String> headers, String body) {
            final Fault fault = new Fault(FaultKind.RESPONSE);
            fault.status = status;
            fault.headers = new LinkedHashMap<>(headers);
            fault.body = body.getBytes(StandardCharsets.UTF_8);
            return fault;
        }

        /**
         * GET only: sends the whole body in {@code chunks} parts with {@code pauseMillis} between them.
         */
        public static Fault slowBody(int chunks, long pauseMillis) {
            final Fault fault = new Fault(FaultKind.SLOW_BODY);
            fault.status = chunks;
            fault.millis = pauseMillis;
            return fault;
        }

        /**
         * GET only: sends half the body, stalls, then resets the connection.
         */
        public static Fault stallBody(long millis) {
            final Fault fault = new Fault(FaultKind.STALL_BODY);
            fault.millis = millis;
            return fault;
        }

        /**
         * Answers with this status and an S3 error document carrying {@code code}.
         */
        public static Fault status(int status, String code) {
            final Fault fault = new Fault(FaultKind.STATUS);
            fault.status = status;
            fault.code = code;
            return fault;
        }
    }

    public static final class RecordedRequest {
        public final Map<String, String> headers;
        public final String method;
        public final String path;
        public final String rawQuery;

        RecordedRequest(TestHttpServer.Request request) {
            this.method = request.method;
            this.path = request.path;
            this.rawQuery = request.rawQuery;
            this.headers = new LinkedHashMap<>(request.headers);
        }

        @Override
        public String toString() {
            return method + ' ' + path + (rawQuery.isEmpty() ? "" : "?" + rawQuery);
        }
    }
}
