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

import io.questdb.replication.store.ObjectStoreUtils;
import org.jetbrains.annotations.NotNull;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.SortedMap;

/**
 * AWS Signature Version 4 for S3 requests sent with an {@code Authorization} header, as described
 * in "Signature Calculations for the Authorization Header: Transferring Payload in a Single Chunk"
 * of the Amazon S3 API Reference. The caller chooses which headers to sign and passes the hex
 * SHA-256 of the payload, which also goes into the {@code x-amz-content-sha256} header.
 * Instances are immutable and thread-safe.
 */
public final class S3RequestSigner {
    public static final String ALGORITHM = "AWS4-HMAC-SHA256";
    /**
     * Hex SHA-256 of an empty payload.
     */
    public static final String EMPTY_PAYLOAD_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final DateTimeFormatter AMZ_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final char[] UPPER_HEX_DIGITS = "0123456789ABCDEF".toCharArray();
    private final String region;
    private final String service;

    public S3RequestSigner(@NotNull String region) {
        this(region, "s3");
    }

    public S3RequestSigner(@NotNull String region, @NotNull String service) {
        this.region = region;
        this.service = service;
    }

    /**
     * Formats epoch milliseconds as the {@code x-amz-date} value, for example {@code 20130524T000000Z}.
     */
    public static @NotNull String formatAmzDate(long epochMillis) {
        return AMZ_DATE_FORMAT.format(Instant.ofEpochMilli(epochMillis));
    }

    /**
     * Derives the signing key: HMAC-SHA256 chained over date, region, service and "aws4_request",
     * starting from "AWS4" + secret.
     */
    public static byte @NotNull [] signingKey(@NotNull String secretAccessKey, @NotNull String date, @NotNull String region, @NotNull String service) {
        final byte[] dateKey = hmacSha256(("AWS4" + secretAccessKey).getBytes(StandardCharsets.UTF_8), date);
        final byte[] regionKey = hmacSha256(dateKey, region);
        final byte[] serviceKey = hmacSha256(regionKey, service);
        return hmacSha256(serviceKey, "aws4_request");
    }

    /**
     * URI-encodes a string the way SigV4 requires: every byte of its UTF-8 form except the RFC 3986
     * unreserved characters becomes {@code %XY} with upper-case hex digits.
     *
     * @param isPath when true, '/' stays as is, for object key paths; query parameter names and
     *               values encode it
     */
    public static @NotNull String uriEncode(@NotNull String value, boolean isPath) {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        final StringBuilder b = new StringBuilder(bytes.length + 16);
        for (byte x : bytes) {
            final int c = x & 0xff;
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '-' || c == '.' || c == '_' || c == '~' || (c == '/' && isPath)) {
                b.append((char) c);
            } else {
                b.append('%').append(UPPER_HEX_DIGITS[c >>> 4]).append(UPPER_HEX_DIGITS[c & 0x0f]);
            }
        }
        return b.toString();
    }

    /**
     * Builds the {@code Authorization} header value.
     *
     * @param method         HTTP method
     * @param canonicalUri   the URI-encoded path exactly as sent
     * @param canonicalQuery the query string exactly as sent: URI-encoded names and values sorted by
     *                       name, empty when there is none
     * @param signedHeaders  lower-case names of the headers to sign mapped to their values; a
     *                       sorted map, so the names come out in the order SigV4 requires
     * @param payloadSha256  hex SHA-256 of the payload
     * @param amzDate        the {@code x-amz-date} header value
     */
    public @NotNull String authorization(
            @NotNull String method,
            @NotNull String canonicalUri,
            @NotNull String canonicalQuery,
            @NotNull SortedMap<String, String> signedHeaders,
            @NotNull String payloadSha256,
            @NotNull String amzDate,
            @NotNull S3Credentials credentials
    ) {
        final String stringToSign = stringToSign(canonicalRequest(method, canonicalUri, canonicalQuery, signedHeaders, payloadSha256), amzDate);
        final byte[] key = signingKey(credentials.getSecretAccessKey(), amzDate.substring(0, 8), region, service);
        final byte[] signature = hmacSha256(key, stringToSign);
        return ALGORITHM + " Credential=" + credentials.getAccessKeyId() + '/' + scope(amzDate)
                + ",SignedHeaders=" + signedHeaderNames(signedHeaders)
                + ",Signature=" + ObjectStoreUtils.hex(signature, signature.length);
    }

    /**
     * The SigV4 canonical request: method, URI, query, headers, signed header names and payload
     * hash, separated by new lines.
     */
    public @NotNull String canonicalRequest(
            @NotNull String method,
            @NotNull String canonicalUri,
            @NotNull String canonicalQuery,
            @NotNull SortedMap<String, String> signedHeaders,
            @NotNull String payloadSha256
    ) {
        final StringBuilder b = new StringBuilder(256);
        b.append(method).append('\n')
                .append(canonicalUri).append('\n')
                .append(canonicalQuery).append('\n');
        for (Map.Entry<String, String> header : signedHeaders.entrySet()) {
            b.append(header.getKey()).append(':').append(canonicalHeaderValue(header.getValue())).append('\n');
        }
        b.append('\n')
                .append(signedHeaderNames(signedHeaders)).append('\n')
                .append(payloadSha256);
        return b.toString();
    }

    public @NotNull String getRegion() {
        return region;
    }

    /**
     * The credential scope, for example {@code 20130524/us-east-1/s3/aws4_request}.
     */
    public @NotNull String scope(@NotNull String amzDate) {
        return amzDate.substring(0, 8) + '/' + region + '/' + service + "/aws4_request";
    }

    /**
     * The string to sign: algorithm, request date, scope and the hex SHA-256 of the canonical
     * request, separated by new lines.
     */
    public @NotNull String stringToSign(@NotNull String canonicalRequest, @NotNull String amzDate) {
        final byte[] request = canonicalRequest.getBytes(StandardCharsets.UTF_8);
        return ALGORITHM + '\n' + amzDate + '\n' + scope(amzDate) + '\n' + ObjectStoreUtils.sha256Hex(request, 0, request.length);
    }

    private static String canonicalHeaderValue(String value) {
        // trim and collapse runs of spaces into one, as SigV4 requires
        final String trimmed = value.trim();
        final StringBuilder b = new StringBuilder(trimmed.length());
        boolean isPreviousSpace = false;
        for (int i = 0, n = trimmed.length(); i < n; i++) {
            final char c = trimmed.charAt(i);
            if (c == ' ') {
                if (!isPreviousSpace) {
                    b.append(c);
                }
                isPreviousSpace = true;
            } else {
                b.append(c);
                isPreviousSpace = false;
            }
        }
        return b.toString();
    }

    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            final Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            // every Java runtime must provide HmacSHA256, and it accepts keys of any length
            throw new AssertionError("HmacSHA256 is not available", e);
        }
    }

    private static String signedHeaderNames(SortedMap<String, String> signedHeaders) {
        return String.join(";", signedHeaders.keySet());
    }
}
