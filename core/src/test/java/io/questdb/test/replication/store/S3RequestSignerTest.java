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

import io.questdb.replication.store.ObjectStoreUtils;
import io.questdb.replication.store.s3.S3Credentials;
import io.questdb.replication.store.s3.S3RequestSigner;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.TreeMap;

/**
 * Checks the SigV4 signer against the worked examples that AWS publishes:
 * <ul>
 *     <li>GET Object, PUT Object, GET Bucket Lifecycle and GET Bucket (List Objects) from the
 *     Amazon S3 API Reference, "Signature Calculations for the Authorization Header: Transferring
 *     Payload in a Single Chunk (AWS Signature Version 4)", section "Examples: Signature
 *     Calculations" (access key AKIAIOSFODNN7EXAMPLE, bucket examplebucket, us-east-1,
 *     20130524T000000Z);</li>
 *     <li>the signing key derivation from the AWS General Reference, "Examples of how to derive a
 *     signing key for Signature Version 4" (IAM, 20120215).</li>
 * </ul>
 */
public class S3RequestSignerTest {
    private static final String AMZ_DATE = "20130524T000000Z";
    private static final S3Credentials EXAMPLE_CREDENTIALS = new S3Credentials(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            null,
            S3Credentials.NO_EXPIRATION
    );
    private static final String HOST = "examplebucket.s3.amazonaws.com";
    private final S3RequestSigner signer = new S3RequestSigner("us-east-1");

    @Test
    public void testCanonicalHeaderValueTrimsAndCollapsesSpaces() {
        final TreeMap<String, String> headers = new TreeMap<>();
        headers.put("host", HOST);
        headers.put("x-amz-meta-note", "  a   b  c ");
        Assert.assertEquals(
                "GET\n/k\n\nhost:examplebucket.s3.amazonaws.com\nx-amz-meta-note:a b c\n\nhost;x-amz-meta-note\n"
                        + S3RequestSigner.EMPTY_PAYLOAD_SHA256,
                signer.canonicalRequest("GET", "/k", "", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256)
        );
    }

    @Test
    public void testEmptyPayloadHash() {
        Assert.assertEquals(S3RequestSigner.EMPTY_PAYLOAD_SHA256, ObjectStoreUtils.sha256Hex(new byte[0], 0, 0));
    }

    @Test
    public void testFormatAmzDate() {
        Assert.assertEquals(AMZ_DATE, S3RequestSigner.formatAmzDate(Instant.parse("2013-05-24T00:00:00Z").toEpochMilli()));
        Assert.assertEquals("19700101T000000Z", S3RequestSigner.formatAmzDate(0));
        Assert.assertEquals("20261114T235959Z", S3RequestSigner.formatAmzDate(Instant.parse("2026-11-14T23:59:59.999Z").toEpochMilli()));
    }

    @Test
    public void testGetBucketLifecycleExample() {
        final TreeMap<String, String> headers = new TreeMap<>();
        headers.put("host", HOST);
        headers.put("x-amz-content-sha256", S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        headers.put("x-amz-date", AMZ_DATE);
        final String canonicalRequest = signer.canonicalRequest("GET", "/", "lifecycle=", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        Assert.assertEquals(
                """
                        GET
                        /
                        lifecycle=
                        host:examplebucket.s3.amazonaws.com
                        x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
                        x-amz-date:20130524T000000Z

                        host;x-amz-content-sha256;x-amz-date
                        e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855""",
                canonicalRequest
        );
        Assert.assertEquals(
                """
                        AWS4-HMAC-SHA256
                        20130524T000000Z
                        20130524/us-east-1/s3/aws4_request
                        9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca""",
                signer.stringToSign(canonicalRequest, AMZ_DATE)
        );
        Assert.assertEquals(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                        + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
                        + "Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
                signer.authorization("GET", "/", "lifecycle=", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256, AMZ_DATE, EXAMPLE_CREDENTIALS)
        );
    }

    @Test
    public void testGetObjectExample() {
        final TreeMap<String, String> headers = new TreeMap<>();
        headers.put("host", HOST);
        headers.put("range", "bytes=0-9");
        headers.put("x-amz-content-sha256", S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        headers.put("x-amz-date", AMZ_DATE);
        final String canonicalRequest = signer.canonicalRequest("GET", "/test.txt", "", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        Assert.assertEquals(
                """
                        GET
                        /test.txt

                        host:examplebucket.s3.amazonaws.com
                        range:bytes=0-9
                        x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
                        x-amz-date:20130524T000000Z

                        host;range;x-amz-content-sha256;x-amz-date
                        e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855""",
                canonicalRequest
        );
        Assert.assertEquals(
                """
                        AWS4-HMAC-SHA256
                        20130524T000000Z
                        20130524/us-east-1/s3/aws4_request
                        7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972""",
                signer.stringToSign(canonicalRequest, AMZ_DATE)
        );
        Assert.assertEquals(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                        + "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"
                        + "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
                signer.authorization("GET", "/test.txt", "", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256, AMZ_DATE, EXAMPLE_CREDENTIALS)
        );
    }

    @Test
    public void testListObjectsExample() {
        final TreeMap<String, String> parameters = new TreeMap<>();
        parameters.put("prefix", "J");
        parameters.put("max-keys", "2");
        final String query = io.questdb.replication.store.s3.S3ObjectStore.canonicalQuery(parameters);
        Assert.assertEquals("max-keys=2&prefix=J", query);
        final TreeMap<String, String> headers = new TreeMap<>();
        headers.put("host", HOST);
        headers.put("x-amz-content-sha256", S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        headers.put("x-amz-date", AMZ_DATE);
        final String canonicalRequest = signer.canonicalRequest("GET", "/", query, headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        Assert.assertEquals(
                """
                        AWS4-HMAC-SHA256
                        20130524T000000Z
                        20130524/us-east-1/s3/aws4_request
                        df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7""",
                signer.stringToSign(canonicalRequest, AMZ_DATE)
        );
        Assert.assertEquals(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                        + "SignedHeaders=host;x-amz-content-sha256;x-amz-date,"
                        + "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
                signer.authorization("GET", "/", query, headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256, AMZ_DATE, EXAMPLE_CREDENTIALS)
        );
    }

    @Test
    public void testPutObjectExample() {
        final byte[] payload = "Welcome to Amazon S3.".getBytes(StandardCharsets.UTF_8);
        final String payloadHash = ObjectStoreUtils.sha256Hex(payload, 0, payload.length);
        Assert.assertEquals("44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072", payloadHash);
        final String canonicalUri = S3RequestSigner.uriEncode("/test$file.text", true);
        Assert.assertEquals("/test%24file.text", canonicalUri);
        final TreeMap<String, String> headers = new TreeMap<>();
        headers.put("date", "Fri, 24 May 2013 00:00:00 GMT");
        headers.put("host", HOST);
        headers.put("x-amz-content-sha256", payloadHash);
        headers.put("x-amz-date", AMZ_DATE);
        headers.put("x-amz-storage-class", "REDUCED_REDUNDANCY");
        final String canonicalRequest = signer.canonicalRequest("PUT", canonicalUri, "", headers, payloadHash);
        Assert.assertEquals(
                """
                        PUT
                        /test%24file.text

                        date:Fri, 24 May 2013 00:00:00 GMT
                        host:examplebucket.s3.amazonaws.com
                        x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
                        x-amz-date:20130524T000000Z
                        x-amz-storage-class:REDUCED_REDUNDANCY

                        date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class
                        44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072""",
                canonicalRequest
        );
        Assert.assertEquals(
                """
                        AWS4-HMAC-SHA256
                        20130524T000000Z
                        20130524/us-east-1/s3/aws4_request
                        9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d""",
                signer.stringToSign(canonicalRequest, AMZ_DATE)
        );
        Assert.assertEquals(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                        + "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,"
                        + "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
                signer.authorization("PUT", canonicalUri, "", headers, payloadHash, AMZ_DATE, EXAMPLE_CREDENTIALS)
        );
    }

    @Test
    public void testRegionAndScope() {
        final S3RequestSigner euSigner = new S3RequestSigner("eu-west-1");
        Assert.assertEquals("eu-west-1", euSigner.getRegion());
        Assert.assertEquals("20261114/eu-west-1/s3/aws4_request", euSigner.scope("20261114T101010Z"));
        Assert.assertEquals("20261114/eu-west-1/iam/aws4_request", new S3RequestSigner("eu-west-1", "iam").scope("20261114T101010Z"));
    }

    @Test
    public void testSessionTokenIsSignedLikeAnyHeader() {
        final TreeMap<String, String> headers = new TreeMap<>();
        headers.put("host", HOST);
        headers.put("x-amz-content-sha256", S3RequestSigner.EMPTY_PAYLOAD_SHA256);
        headers.put("x-amz-date", AMZ_DATE);
        headers.put("x-amz-security-token", "token-1");
        final S3Credentials credentials = new S3Credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "token-1", 0);
        final String authorization = signer.authorization("GET", "/test.txt", "", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256, AMZ_DATE, credentials);
        Assert.assertTrue(authorization, authorization.startsWith(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
                        + "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,Signature="
        ));
        // a different token value changes the signature
        headers.put("x-amz-security-token", "token-2");
        Assert.assertNotEquals(
                authorization,
                signer.authorization("GET", "/test.txt", "", headers, S3RequestSigner.EMPTY_PAYLOAD_SHA256, AMZ_DATE, credentials)
        );
    }

    @Test
    public void testSigningKeyDerivationExample() {
        final byte[] key = S3RequestSigner.signingKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20120215", "us-east-1", "iam");
        Assert.assertEquals("f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d", ObjectStoreUtils.hex(key, key.length));
    }

    @Test
    public void testUriEncode() {
        Assert.assertEquals("AZaz09-._~", S3RequestSigner.uriEncode("AZaz09-._~", false));
        Assert.assertEquals("a/b%20c", S3RequestSigner.uriEncode("a/b c", true));
        Assert.assertEquals("a%2Fb%20c", S3RequestSigner.uriEncode("a/b c", false));
        Assert.assertEquals("%2A%2B%3D%26%3F%25%23%3A%40%21%24%27%28%29%2C%3B", S3RequestSigner.uriEncode("*+=&?%#:@!$'(),;", true));
        // U+00E9 is C3 A9 in UTF-8, U+1F600 is F0 9F 98 80
        Assert.assertEquals("%C3%A9/%F0%9F%98%80", S3RequestSigner.uriEncode("\u00e9/\uD83D\uDE00", true));
        Assert.assertEquals("", S3RequestSigner.uriEncode("", true));
    }
}
