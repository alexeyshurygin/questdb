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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;

/**
 * Minimal XML reader for S3 responses. It reports the text of elements without child elements and
 * the end of elements with children; it ignores attributes, the XML declaration, processing
 * instructions and comments, reads CDATA sections as text, and decodes the predefined entities
 * ({@code &amp; &lt; &gt; &quot; &apos;}) and numeric character references. It rejects document
 * type declarations, so no entity it does not know can appear.
 */
public final class S3XmlParser {

    private S3XmlParser() {
    }

    /**
     * Decodes the entities and character references of XML text.
     *
     * @throws IllegalArgumentException for an unknown, malformed or unterminated entity
     */
    public static @NotNull String decodeText(@NotNull String text) {
        final StringBuilder b = new StringBuilder(text.length());
        decode(text, 0, text.length(), b);
        return b.toString();
    }

    /**
     * Returns the {@code Code} of an S3 {@code Error} document, or null when there is none. A
     * truncated or malformed document still yields a code that appears before the damage.
     */
    public static @Nullable String findErrorCode(@NotNull String xml) {
        final String[] code = {null};
        try {
            parse(xml, new Handler() {
                @Override
                public void onEndElement(@NotNull String name) {
                }

                @Override
                public void onLeafElement(@Nullable String parent, @NotNull String name, @NotNull String text) {
                    if (code[0] == null && "Error".equals(parent) && "Code".equals(name)) {
                        code[0] = text;
                    }
                }
            });
        } catch (IllegalArgumentException ignore) {
            // keep the code found before the error, if any
        }
        return code[0];
    }

    /**
     * Reads a whole document, reporting its elements to {@code handler}.
     *
     * @throws IllegalArgumentException when the document is not well-formed
     */
    public static void parse(@NotNull String xml, @NotNull Handler handler) {
        final ArrayDeque<String> open = new ArrayDeque<>();
        final StringBuilder text = new StringBuilder();
        boolean isLeaf = false;
        boolean hasRoot = false;
        int i = 0;
        final int n = xml.length();
        while (i < n) {
            final int lt = xml.indexOf('<', i);
            final int textEnd = lt < 0 ? n : lt;
            if (textEnd > i) {
                if (open.isEmpty()) {
                    if (!xml.substring(i, textEnd).isBlank()) {
                        throw malformed(i, "text outside the root element");
                    }
                } else {
                    decode(xml, i, textEnd, text);
                }
            }
            if (lt < 0) {
                break;
            }
            if (xml.startsWith("<?", lt)) {
                i = skipPast(xml, lt, "?>");
            } else if (xml.startsWith("<!--", lt)) {
                i = skipPast(xml, lt, "-->");
            } else if (xml.startsWith("<![CDATA[", lt)) {
                if (open.isEmpty()) {
                    throw malformed(lt, "CDATA outside the root element");
                }
                final int end = xml.indexOf("]]>", lt + 9);
                if (end < 0) {
                    throw malformed(lt, "unterminated CDATA section");
                }
                text.append(xml, lt + 9, end);
                i = end + 3;
            } else if (xml.startsWith("<!", lt)) {
                throw malformed(lt, "unsupported markup");
            } else {
                final int gt = findTagEnd(xml, lt);
                if (xml.charAt(lt + 1) == '/') {
                    final String name = xml.substring(lt + 2, gt).trim();
                    if (open.isEmpty() || !open.peek().equals(name)) {
                        throw malformed(lt, "unexpected end tag");
                    }
                    open.pop();
                    if (isLeaf) {
                        handler.onLeafElement(open.peek(), name, text.toString());
                    } else {
                        handler.onEndElement(name);
                    }
                    isLeaf = false;
                } else {
                    final boolean isEmptyElement = xml.charAt(gt - 1) == '/';
                    final String name = tagName(xml, lt + 1, isEmptyElement ? gt - 1 : gt);
                    if (name.isEmpty()) {
                        throw malformed(lt, "missing element name");
                    }
                    if (open.isEmpty() && hasRoot) {
                        throw malformed(lt, "more than one root element");
                    }
                    hasRoot = true;
                    if (isEmptyElement) {
                        handler.onLeafElement(open.peek(), name, "");
                        isLeaf = false;
                    } else {
                        open.push(name);
                        text.setLength(0);
                        isLeaf = true;
                    }
                }
                i = gt + 1;
            }
        }
        if (!open.isEmpty()) {
            throw malformed(n, "unexpected end of document");
        }
        if (!hasRoot) {
            throw malformed(n, "no root element");
        }
    }

    private static void decode(String xml, int lo, int hi, StringBuilder out) {
        int i = lo;
        while (i < hi) {
            final char c = xml.charAt(i);
            if (c != '&') {
                out.append(c);
                i++;
                continue;
            }
            final int semicolon = xml.indexOf(';', i);
            if (semicolon < 0 || semicolon >= hi) {
                throw malformed(i, "unterminated entity");
            }
            final String entity = xml.substring(i + 1, semicolon);
            switch (entity) {
                case "amp" -> out.append('&');
                case "lt" -> out.append('<');
                case "gt" -> out.append('>');
                case "quot" -> out.append('"');
                case "apos" -> out.append('\'');
                default -> out.appendCodePoint(parseCharacterReference(entity, i));
            }
            i = semicolon + 1;
        }
    }

    private static int findTagEnd(String xml, int lt) {
        char quote = 0;
        for (int j = lt + 1, n = xml.length(); j < n; j++) {
            final char c = xml.charAt(j);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
            } else if (c == '"' || c == '\'') {
                quote = c;
            } else if (c == '>') {
                return j;
            }
        }
        throw malformed(lt, "unterminated tag");
    }

    private static IllegalArgumentException malformed(int position, String reason) {
        return new IllegalArgumentException("malformed XML [position=" + position + ", reason=" + reason + ']');
    }

    private static int parseCharacterReference(String entity, int position) {
        if (entity.length() < 2 || entity.charAt(0) != '#') {
            throw malformed(position, "unknown entity");
        }
        final boolean isHex = entity.charAt(1) == 'x';
        final String digits = entity.substring(isHex ? 2 : 1);
        final int codePoint;
        try {
            codePoint = Integer.parseInt(digits, isHex ? 16 : 10);
        } catch (NumberFormatException e) {
            throw malformed(position, "invalid character reference");
        }
        // parseInt accepts a sign, which a character reference must not have
        if (digits.charAt(0) == '-' || digits.charAt(0) == '+' || !Character.isValidCodePoint(codePoint)) {
            throw malformed(position, "invalid character reference");
        }
        return codePoint;
    }

    private static int skipPast(String xml, int from, String terminator) {
        final int end = xml.indexOf(terminator, from);
        if (end < 0) {
            throw malformed(from, "unterminated markup");
        }
        return end + terminator.length();
    }

    private static String tagName(String xml, int lo, int hi) {
        int end = lo;
        while (end < hi && !Character.isWhitespace(xml.charAt(end))) {
            end++;
        }
        return xml.substring(lo, end);
    }

    /**
     * Receives the elements of a document in order.
     */
    public interface Handler {
        /**
         * Called at the end of an element that has child elements.
         */
        void onEndElement(@NotNull String name);

        /**
         * Called at the end of an element without child elements, with its decoded text.
         *
         * @param parent the enclosing element, null for the root element
         */
        void onLeafElement(@Nullable String parent, @NotNull String name, @NotNull String text);
    }
}
