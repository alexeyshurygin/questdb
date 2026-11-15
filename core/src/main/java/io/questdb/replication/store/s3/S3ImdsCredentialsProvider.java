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
import io.questdb.replication.store.ObjectStoreException;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/**
 * Credentials of the IAM role attached to the EC2 instance, read from the instance metadata
 * service with the IMDSv2 session token flow:
 * <ol>
 *     <li>{@code PUT /latest/api/token} with {@code X-aws-ec2-metadata-token-ttl-seconds: 21600}
 *     returns a session token;</li>
 *     <li>{@code GET /latest/meta-data/iam/security-credentials/} names the role;</li>
 *     <li>{@code GET /latest/meta-data/iam/security-credentials/<role>} returns JSON with
 *     {@code AccessKeyId}, {@code SecretAccessKey}, {@code Token} and {@code Expiration}.</li>
 * </ol>
 * The provider caches the credentials and fetches new ones 5 minutes before they expire. When a
 * refresh fails while the cached credentials are still valid, it keeps using them and tries again
 * on the next call.
 */
public final class S3ImdsCredentialsProvider implements S3CredentialsProvider {
    public static final String DEFAULT_ENDPOINT = "http://169.254.169.254";
    public static final long REFRESH_MARGIN_MILLIS = 5 * 60 * 1000L;
    public static final String TOKEN_TTL_SECONDS = "21600";
    private static final String CREDENTIALS_PATH = "/latest/meta-data/iam/security-credentials/";
    private static final Log LOG = LogFactory.getLog(S3ImdsCredentialsProvider.class);
    private static final String TOKEN_HEADER = "X-aws-ec2-metadata-token";
    private static final String TOKEN_PATH = "/latest/api/token";
    private static final String TOKEN_TTL_HEADER = "X-aws-ec2-metadata-token-ttl-seconds";
    private final LongSupplier clock;
    private final String endpoint;
    private final HttpClient httpClient;
    private final long timeoutMillis;
    private S3Credentials cached;

    /**
     * @param endpoint      base URL of the metadata service, for example {@code http://169.254.169.254}
     * @param timeoutMillis timeout of each metadata request
     * @param clock         epoch milliseconds, compared with the credentials' expiration
     */
    public S3ImdsCredentialsProvider(@NotNull HttpClient httpClient, @NotNull String endpoint, long timeoutMillis, @NotNull LongSupplier clock) {
        this.httpClient = httpClient;
        this.endpoint = validateEndpoint(endpoint);
        this.timeoutMillis = Math.max(1, timeoutMillis);
        this.clock = clock;
    }

    /**
     * Parses a JSON object whose values are strings, numbers, booleans or null, and returns its
     * string-valued members.
     *
     * @throws IllegalArgumentException when the text is not such an object
     */
    public static @NotNull Map<String, String> parseJsonObject(@NotNull String json) {
        return new JsonReader(json).readObject();
    }

    @Override
    public synchronized @NotNull S3Credentials getCredentials() {
        final long now = clock.getAsLong();
        if (cached != null && now < cached.getExpirationMillis() - REFRESH_MARGIN_MILLIS) {
            return cached;
        }
        try {
            cached = fetch();
        } catch (ObjectStoreException e) {
            if (cached != null && now < cached.getExpirationMillis()) {
                LOG.info().$("could not refresh EC2 instance metadata credentials, using the cached ones until they expire [error=")
                        .$(e.getMessage())
                        .I$();
                return cached;
            }
            throw e;
        }
        return cached;
    }

    private static String firstLine(String text) {
        final String trimmed = text.trim();
        final int eol = trimmed.indexOf('\n');
        return (eol < 0 ? trimmed : trimmed.substring(0, eol)).trim();
    }

    private static String requireField(Map<String, String> fields, String name) {
        final String value = fields.get(name);
        if (value == null || value.isEmpty()) {
            throw ObjectStoreException.fatal("EC2 instance metadata credentials lack a field [field=" + name + ']');
        }
        return value;
    }

    private static String validateEndpoint(String endpoint) {
        final String trimmed = endpoint.trim();
        final URI uri;
        try {
            uri = new URI(trimmed);
        } catch (URISyntaxException e) {
            throw ObjectStoreException.fatal("invalid EC2 instance metadata endpoint [endpoint=" + trimmed + ']');
        }
        if (uri.getHost() == null || !("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))) {
            throw ObjectStoreException.fatal("invalid EC2 instance metadata endpoint [endpoint=" + trimmed + ']');
        }
        int end = trimmed.length();
        while (trimmed.charAt(end - 1) == '/') {
            end--;
        }
        return trimmed.substring(0, end);
    }

    private S3Credentials fetch() {
        final String token = request("PUT", TOKEN_PATH, TOKEN_TTL_HEADER, TOKEN_TTL_SECONDS).trim();
        final String role = firstLine(request("GET", CREDENTIALS_PATH, TOKEN_HEADER, token));
        if (role.isEmpty()) {
            throw ObjectStoreException.fatal("EC2 instance metadata lists no IAM role, attach an instance profile to the instance");
        }
        final String json = request("GET", CREDENTIALS_PATH + S3RequestSigner.uriEncode(role, false), TOKEN_HEADER, token);
        final Map<String, String> fields;
        try {
            fields = parseJsonObject(json);
        } catch (IllegalArgumentException e) {
            throw ObjectStoreException.fatal("malformed EC2 instance metadata credentials [role=" + role + ", reason=" + e.getMessage() + ']');
        }
        final String code = fields.get("Code");
        if (code != null && !"Success".equals(code)) {
            throw ObjectStoreException.fatal("EC2 instance metadata did not return credentials [role=" + role + ", code=" + code + ']');
        }
        final String accessKeyId = requireField(fields, "AccessKeyId");
        final String secretAccessKey = requireField(fields, "SecretAccessKey");
        final String sessionToken = requireField(fields, "Token");
        final String expiration = requireField(fields, "Expiration");
        final long expirationMillis;
        try {
            expirationMillis = Instant.parse(expiration).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw ObjectStoreException.fatal("malformed EC2 instance metadata credentials [role=" + role + ", reason=invalid Expiration]");
        }
        return new S3Credentials(accessKeyId, secretAccessKey, sessionToken, expirationMillis);
    }

    private String request(String method, String path, String headerName, String headerValue) {
        final HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint + path))
                .timeout(Duration.ofMillis(timeoutMillis))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .header(headerName, headerValue)
                .build();
        final CompletableFuture<HttpResponse<String>> future = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        final HttpResponse<String> response;
        try {
            response = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw timedOut(path, e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            // the client's own request timeout races with ours; both mean the same
            if (cause instanceof HttpTimeoutException) {
                throw timedOut(path, cause);
            }
            throw ObjectStoreException.retryable("EC2 instance metadata request failed [path=" + path + ", error=" + cause + ']', cause);
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw ObjectStoreException.retryable("EC2 instance metadata request interrupted [path=" + path + ']', e);
        }
        final int status = response.statusCode();
        if (status == 200) {
            return response.body();
        }
        final ObjectStoreException.Kind kind = status == 408 || status == 429 || status >= 500
                ? ObjectStoreException.Kind.RETRYABLE
                : ObjectStoreException.Kind.FATAL;
        throw new ObjectStoreException(kind, status, "EC2 instance metadata request failed [path=" + path + ", status=" + status + ']');
    }

    private ObjectStoreException timedOut(String path, Throwable cause) {
        return ObjectStoreException.retryable("EC2 instance metadata request timed out [path=" + path + ", timeout=" + timeoutMillis + "ms]", cause);
    }

    private static final class JsonReader {
        private final String json;
        private int pos;

        JsonReader(String json) {
            this.json = json;
        }

        Map<String, String> readObject() {
            final Map<String, String> fields = new HashMap<>();
            skipWhitespace();
            expect('{');
            skipWhitespace();
            if (peek() == '}') {
                pos++;
            } else {
                while (true) {
                    skipWhitespace();
                    final String name = readString();
                    skipWhitespace();
                    expect(':');
                    skipWhitespace();
                    if (peek() == '"') {
                        fields.put(name, readString());
                    } else {
                        readLiteral();
                    }
                    skipWhitespace();
                    final char c = next();
                    if (c == '}') {
                        break;
                    }
                    if (c != ',') {
                        throw malformed("expected ',' or '}'");
                    }
                }
            }
            skipWhitespace();
            if (pos < json.length()) {
                throw malformed("unexpected text after the object");
            }
            return fields;
        }

        private void expect(char expected) {
            if (next() != expected) {
                throw malformed("expected '" + expected + '\'');
            }
        }

        private IllegalArgumentException malformed(String reason) {
            return new IllegalArgumentException("malformed JSON [position=" + pos + ", reason=" + reason + ']');
        }

        private char next() {
            if (pos >= json.length()) {
                throw malformed("unexpected end of text");
            }
            return json.charAt(pos++);
        }

        private char peek() {
            if (pos >= json.length()) {
                throw malformed("unexpected end of text");
            }
            return json.charAt(pos);
        }

        private void readLiteral() {
            // a number, true, false or null; the value itself is not needed
            final int start = pos;
            while (pos < json.length()) {
                final char c = json.charAt(pos);
                if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'E') {
                    pos++;
                } else {
                    break;
                }
            }
            if (pos == start) {
                throw malformed("unsupported value");
            }
        }

        private String readString() {
            expect('"');
            final StringBuilder b = new StringBuilder();
            while (true) {
                final char c = next();
                if (c == '"') {
                    return b.toString();
                }
                if (c < 0x20) {
                    throw malformed("control character in string");
                }
                if (c != '\\') {
                    b.append(c);
                    continue;
                }
                final char escaped = next();
                switch (escaped) {
                    case '"', '\\', '/' -> b.append(escaped);
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'u' -> {
                        if (pos + 4 > json.length()) {
                            throw malformed("truncated unicode escape");
                        }
                        int value = 0;
                        for (int i = 0; i < 4; i++) {
                            final int digit = Character.digit(json.charAt(pos++), 16);
                            if (digit < 0) {
                                throw malformed("invalid unicode escape");
                            }
                            value = value * 16 + digit;
                        }
                        b.append((char) value);
                    }
                    default -> throw malformed("invalid escape");
                }
            }
        }

        private void skipWhitespace() {
            while (pos < json.length()) {
                final char c = json.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }
    }
}
