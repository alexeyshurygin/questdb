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

import io.questdb.replication.store.s3.S3ListPage;
import io.questdb.replication.store.s3.S3XmlParser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class S3XmlParserTest {

    @Test
    public void testDecodeText() {
        Assert.assertEquals("a&b<c>d\"e'f", S3XmlParser.decodeText("a&amp;b&lt;c&gt;d&quot;e&apos;f"));
        Assert.assertEquals("AB\u00e9\uD83D\uDE00", S3XmlParser.decodeText("&#65;&#x42;&#233;&#x1F600;"));
        Assert.assertEquals("plain > text", S3XmlParser.decodeText("plain > text"));
        Assert.assertEquals("", S3XmlParser.decodeText(""));
        Assert.assertEquals("\uDBFF\uDFFF", S3XmlParser.decodeText("&#x10FFFF;"));
    }

    @Test
    public void testDecodeTextRejectsBadEntities() {
        assertMalformed("malformed XML [position=0, reason=unterminated entity]", () -> S3XmlParser.decodeText("&amp"));
        assertMalformed("malformed XML [position=1, reason=unknown entity]", () -> S3XmlParser.decodeText("a&foo;"));
        assertMalformed("malformed XML [position=0, reason=unknown entity]", () -> S3XmlParser.decodeText("&#;"));
        assertMalformed("malformed XML [position=0, reason=unknown entity]", () -> S3XmlParser.decodeText("&;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#x;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#xZZ;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#X41;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#-5;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#+65;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#1114112;"));
        assertMalformed("malformed XML [position=0, reason=invalid character reference]", () -> S3XmlParser.decodeText("&#99999999999;"));
    }

    @Test
    public void testFindErrorCode() {
        Assert.assertEquals("NoSuchKey", S3XmlParser.findErrorCode(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>The resource you requested does not exist</Message></Error>"));
        Assert.assertEquals("A", S3XmlParser.findErrorCode("<Error><Code>A</Code><Code>B</Code></Error>"));
        Assert.assertNull(S3XmlParser.findErrorCode("<Error><Message>m</Message></Error>"));
        Assert.assertNull(S3XmlParser.findErrorCode("<Other><Code>X</Code></Other>"));
        // a document cut short after the code still yields it
        Assert.assertEquals("SlowDown", S3XmlParser.findErrorCode("<Error><Code>SlowDown</Code><Message>Please red"));
        // damage before the code yields nothing
        Assert.assertNull(S3XmlParser.findErrorCode("<Error><Bad></Error><Code>X</Code>"));
        Assert.assertNull(S3XmlParser.findErrorCode("Service Unavailable"));
    }

    @Test
    public void testListPage() {
        final S3ListPage page = S3ListPage.parse("""
                <?xml version="1.0" encoding="UTF-8"?>
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Name>bucket</Name>
                  <Prefix>db1/</Prefix>
                  <KeyCount>2</KeyCount>
                  <MaxKeys>2</MaxKeys>
                  <IsTruncated> true </IsTruncated>
                  <Contents>
                    <Key>db1/a&amp;b&lt;c&gt;&quot;&apos;</Key>
                    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
                    <ETag>&quot;9e107d9d372bb6826bd81d3542a419d6&quot;</ETag>
                    <Size>12</Size>
                    <Owner><ID>owner</ID><DisplayName>me</DisplayName></Owner>
                    <StorageClass>STANDARD</StorageClass>
                  </Contents>
                  <Contents><Size> 0 </Size><Key>db1/&#233;</Key></Contents>
                  <CommonPrefixes><Prefix>db1/dir/</Prefix></CommonPrefixes>
                  <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
                </ListBucketResult>
                """);
        Assert.assertEquals(2, page.size());
        Assert.assertEquals("db1/a&b<c>\"'", page.getKey(0));
        Assert.assertEquals(12, page.getSize(0));
        Assert.assertEquals("db1/\u00e9", page.getKey(1));
        Assert.assertEquals(0, page.getSize(1));
        Assert.assertTrue(page.isTruncated());
        Assert.assertEquals("1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=", page.getNextContinuationToken());
    }

    @Test
    public void testListPageEmptyForms() {
        for (String xml : new String[]{
                "<ListBucketResult/>",
                "<ListBucketResult></ListBucketResult>",
                "<ListBucketResult xmlns=\"x\">\n</ListBucketResult>",
                "<ListBucketResult><Name>b</Name><IsTruncated>false</IsTruncated></ListBucketResult>"
        }) {
            final S3ListPage page = S3ListPage.parse(xml);
            Assert.assertEquals(xml, 0, page.size());
            Assert.assertFalse(xml, page.isTruncated());
            Assert.assertNull(xml, page.getNextContinuationToken());
        }
    }

    @Test
    public void testListPageRejectsMalformedResponses() {
        assertMalformed("response is not a ListBucketResult", () -> S3ListPage.parse("<Error><Code>InternalError</Code></Error>"));
        assertMalformed("response is not a ListBucketResult", () -> S3ListPage.parse("<html>oops</html>"));
        assertMalformed("ListObjectsV2 entry lacks Key or Size",
                () -> S3ListPage.parse("<ListBucketResult><Contents><Key>a</Key></Contents></ListBucketResult>"));
        assertMalformed("ListObjectsV2 entry lacks Key or Size",
                () -> S3ListPage.parse("<ListBucketResult><Contents><Size>1</Size></Contents></ListBucketResult>"));
        assertMalformed("ListObjectsV2 entry has an invalid Size",
                () -> S3ListPage.parse("<ListBucketResult><Contents><Key>a</Key><Size>big</Size></Contents></ListBucketResult>"));
        assertMalformed("truncated ListObjectsV2 response lacks NextContinuationToken",
                () -> S3ListPage.parse("<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"));
        assertMalformed("truncated ListObjectsV2 response lacks NextContinuationToken",
                () -> S3ListPage.parse("<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken></NextContinuationToken></ListBucketResult>"));
        assertMalformed("malformed XML [position=18, reason=unexpected end of document]", () -> S3ListPage.parse("<ListBucketResult>"));
    }

    @Test
    public void testParseEvents() {
        final List<String> events = parse("""
                <?xml version="1.0"?>
                <!-- comment with <tags> -->
                <Root attr="a>b" other='x/'>
                  <Leaf>a &amp; b</Leaf>
                  <Empty/>
                  <Empty2 a="1" />
                  <Group><Inner>1</Inner><Data><![CDATA[<raw> & ]]>tail</Data></Group>
                  <Spaced >  text  </Spaced >
                </Root>
                """);
        Assert.assertEquals(List.of(
                "leaf Root/Leaf=a & b",
                "leaf Root/Empty=",
                "leaf Root/Empty2=",
                "leaf Group/Inner=1",
                "leaf Group/Data=<raw> & tail",
                "end Group",
                "leaf Root/Spaced=  text  ",
                "end Root"
        ), events);
        Assert.assertEquals(List.of("leaf null/a=t"), parse("<a>t</a>"));
        Assert.assertEquals(List.of("leaf null/a="), parse("  <a/>  "));
    }

    @Test
    public void testParseRejectsMalformedDocuments() {
        assertMalformed("malformed XML [position=0, reason=no root element]", () -> parse(""));
        assertMalformed("malformed XML [position=5, reason=no root element]", () -> parse("<?x?>"));
        assertMalformed("malformed XML [position=0, reason=text outside the root element]", () -> parse("text"));
        assertMalformed("malformed XML [position=8, reason=text outside the root element]", () -> parse("<a>x</a>trailing"));
        assertMalformed("malformed XML [position=3, reason=unexpected end of document]", () -> parse("<a>"));
        assertMalformed("malformed XML [position=3, reason=unexpected end tag]", () -> parse("<a></b>"));
        assertMalformed("malformed XML [position=0, reason=unexpected end tag]", () -> parse("</a>"));
        assertMalformed("malformed XML [position=4, reason=more than one root element]", () -> parse("<a/><b/>"));
        assertMalformed("malformed XML [position=3, reason=unsupported markup]", () -> parse("<a><!DOCTYPE x></a>"));
        assertMalformed("malformed XML [position=3, reason=unterminated CDATA section]", () -> parse("<a><![CDATA[x</a>"));
        assertMalformed("malformed XML [position=0, reason=CDATA outside the root element]", () -> parse("<![CDATA[x]]><a/>"));
        assertMalformed("malformed XML [position=0, reason=unterminated markup]", () -> parse("<?xml"));
        assertMalformed("malformed XML [position=3, reason=unterminated markup]", () -> parse("<a><!-- x</a>"));
        assertMalformed("malformed XML [position=0, reason=unterminated tag]", () -> parse("<a"));
        assertMalformed("malformed XML [position=0, reason=unterminated tag]", () -> parse("<a b='>'"));
        assertMalformed("malformed XML [position=0, reason=missing element name]", () -> parse("<>"));
        assertMalformed("malformed XML [position=0, reason=missing element name]", () -> parse("< a/>"));
        assertMalformed("malformed XML [position=3, reason=unknown entity]", () -> parse("<a>&bad;</a>"));
        assertMalformed("malformed XML [position=3, reason=unterminated entity]", () -> parse("<a>&amp</a>;"));
    }

    private static void assertMalformed(String message, Runnable action) {
        try {
            action.run();
            Assert.fail("expected IllegalArgumentException: " + message);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(message, e.getMessage());
        }
    }

    private static List<String> parse(String xml) {
        final List<String> events = new ArrayList<>();
        S3XmlParser.parse(xml, new S3XmlParser.Handler() {
            @Override
            public void onEndElement(@NotNull String name) {
                events.add("end " + name);
            }

            @Override
            public void onLeafElement(@Nullable String parent, @NotNull String name, @NotNull String text) {
                events.add("leaf " + parent + '/' + name + '=' + text);
            }
        });
        return events;
    }
}
