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

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fake EC2 instance metadata service speaking IMDSv2: a session token from
 * {@code PUT /latest/api/token}, the role name and the role's credentials from
 * {@code /latest/meta-data/iam/security-credentials/}. Each credentials fetch issues a new
 * access key id ({@code IMDSKEY1}, {@code IMDSKEY2}, ...) so tests can tell refreshes apart.
 */
public class FakeImdsServer implements Closeable {
    public static final String CREDENTIALS_PATH = "/latest/meta-data/iam/security-credentials/";
    public static final String SECRET_PREFIX = "imds/secret";
    public static final String SESSION_TOKEN_PREFIX = "imds-session-token-";
    public static final String TOKEN_PATH = "/latest/api/token";
    private final AtomicInteger credentialFetches = new AtomicInteger();
    private final ConcurrentLinkedDeque<Object[]> faults = new ConcurrentLinkedDeque<>();
    private final TestHttpServer http;
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger tokens = new AtomicInteger();
    private volatile String credentialsBody;
    private volatile String expiration = "2100-01-01T00:00:00Z";
    private volatile String role = "test-role";
    private volatile String roleList;
    private volatile FakeS3Server s3;

    public FakeImdsServer() throws IOException {
        http = new TestHttpServer(this::handle);
    }

    public static String accessKeyId(int fetch) {
        return "IMDSKEY" + fetch;
    }

    /**
     * Answers the next request whose path starts with {@code pathPrefix} with {@code status} and
     * an empty body; status 0 resets the connection instead, and a negative status waits that
     * many milliseconds before resetting it.
     */
    public void addFault(String pathPrefix, int status) {
        faults.add(new Object[]{pathPrefix, status});
    }

    /**
     * Answers the next request whose path starts with {@code pathPrefix} with the head of a
     * 100-byte response and one byte of it, then stalls for {@code millis} and resets the connection.
     */
    public void addStallFault(String pathPrefix, long millis) {
        faults.add(new Object[]{pathPrefix, 200, millis});
    }

    @Override
    public void close() throws IOException {
        http.close();
    }

    public int getCredentialFetches() {
        return credentialFetches.get();
    }

    public String getEndpoint() {
        return http.getBaseUrl();
    }

    public List<String> getRequests() {
        synchronized (requests) {
            return new ArrayList<>(requests);
        }
    }

    /**
     * Registers every issued key with {@code s3}, so the fake S3 accepts requests signed with them.
     */
    public void linkS3(FakeS3Server s3) {
        this.s3 = s3;
    }

    /**
     * Replaces the credentials document; null restores the generated one.
     */
    public void setCredentialsBody(String credentialsBody) {
        this.credentialsBody = credentialsBody;
    }

    public void setExpiration(String expiration) {
        this.expiration = expiration;
    }

    public void setRole(String role) {
        this.role = role;
    }

    /**
     * Replaces the role listing; null restores the configured role followed by a new line.
     */
    public void setRoleList(String roleList) {
        this.roleList = roleList;
    }

    private void handle(TestHttpServer.Request request, TestHttpServer.Exchange exchange) throws IOException {
        requests.add(request.method + ' ' + request.path);
        for (Object[] fault : faults) {
            if (request.path.startsWith((String) fault[0]) && faults.remove(fault)) {
                final int status = (Integer) fault[1];
                if (fault.length == 3) {
                    exchange.respondSlowly(status, Map.of(), new byte[100], 1, 1, (Long) fault[2]);
                } else if (status <= 0) {
                    try {
                        Thread.sleep(-status);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.closeAbruptly();
                } else {
                    exchange.respond(status, Map.of(), new byte[0]);
                }
                return;
            }
        }
        final String currentToken = "session-" + tokens.get();
        if ("PUT".equals(request.method) && TOKEN_PATH.equals(request.path)) {
            if (!"21600".equals(request.header("x-aws-ec2-metadata-token-ttl-seconds"))) {
                exchange.respond(400, Map.of(), new byte[0]);
                return;
            }
            final String token = "session-" + tokens.incrementAndGet();
            exchange.respond(200, Map.of(), token.getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (!"GET".equals(request.method) || !currentToken.equals(request.header("x-aws-ec2-metadata-token"))) {
            exchange.respond(401, Map.of(), new byte[0]);
            return;
        }
        if (CREDENTIALS_PATH.equals(request.path)) {
            final String list = roleList;
            exchange.respond(200, Map.of(), (list != null ? list : role + "\n").getBytes(StandardCharsets.UTF_8));
            return;
        }
        if (request.path.equals(CREDENTIALS_PATH + role)) {
            String body = credentialsBody;
            if (body == null) {
                final int fetch = credentialFetches.incrementAndGet();
                final String accessKeyId = accessKeyId(fetch);
                final String secret = SECRET_PREFIX + fetch;
                final String token = SESSION_TOKEN_PREFIX + fetch;
                final FakeS3Server linked = s3;
                if (linked != null) {
                    linked.addCredentials(accessKeyId, secret, token);
                }
                body = "{\n  \"Code\" : \"Success\",\n  \"LastUpdated\" : \"2026-11-14T00:00:00Z\",\n  \"Type\" : \"AWS-HMAC\",\n"
                        + "  \"AccessKeyId\" : \"" + accessKeyId + "\",\n  \"SecretAccessKey\" : \"" + secret.replace("/", "\\/") + "\",\n"
                        + "  \"Token\" : \"" + token + "\",\n  \"Expiration\" : \"" + expiration + "\"\n}";
            }
            exchange.respond(200, Map.of(), body.getBytes(StandardCharsets.UTF_8));
            return;
        }
        exchange.respond(404, Map.of(), new byte[0]);
    }
}
