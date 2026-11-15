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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal HTTP/1.1 server for object store tests. It listens on 127.0.0.1 with an ephemeral port,
 * never a fixed one, serves each connection on its own daemon thread and, unless keep-alive is
 * on, closes the connection after each response. Requests in proxy form
 * ({@code GET http://host/path}) are accepted, so the server can stand in for any host name.
 */
public class TestHttpServer implements Closeable {
    private final Thread acceptor;
    private final Handler handler;
    private final ServerSocket serverSocket;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private volatile boolean isClosed;
    private volatile boolean isKeepAlive;

    public TestHttpServer(Handler handler) throws IOException {
        this.handler = handler;
        this.serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        this.acceptor = new Thread(this::acceptLoop, "test-http-acceptor-" + serverSocket.getLocalPort());
        acceptor.setDaemon(true);
        acceptor.start();
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        serverSocket.close();
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException ignore) {
            }
        }
        try {
            acceptor.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String getBaseUrl() {
        return "http://127.0.0.1:" + getPort();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public void setKeepAlive(boolean isKeepAlive) {
        this.isKeepAlive = isKeepAlive;
    }

    private static String readLine(InputStream in) throws IOException {
        final ByteArrayOutputStream line = new ByteArrayOutputStream();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') {
                break;
            }
            if (c != '\r') {
                line.write(c);
            }
        }
        if (c == -1 && line.size() == 0) {
            return null;
        }
        return line.toString(StandardCharsets.ISO_8859_1);
    }

    private void acceptLoop() {
        while (!isClosed) {
            final Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                return;
            }
            sockets.add(socket);
            final Thread worker = new Thread(() -> serve(socket), "test-http-worker");
            worker.setDaemon(true);
            worker.start();
        }
    }

    private Request readRequest(InputStream in) throws IOException {
        final String requestLine = readLine(in);
        if (requestLine == null || requestLine.isEmpty()) {
            return null;
        }
        final String[] parts = requestLine.split(" ");
        final Request request = new Request();
        request.method = parts[0];
        String target = parts[1];
        if (target.startsWith("http://")) {
            final int slash = target.indexOf('/', 7);
            target = slash < 0 ? "/" : target.substring(slash);
        }
        final int question = target.indexOf('?');
        request.path = question < 0 ? target : target.substring(0, question);
        request.rawQuery = question < 0 ? "" : target.substring(question + 1);
        String line;
        while ((line = readLine(in)) != null && !line.isEmpty()) {
            final int colon = line.indexOf(':');
            final String name = line.substring(0, colon).trim().toLowerCase();
            final String value = line.substring(colon + 1).trim();
            request.headers.merge(name, value, (a, b) -> a + ',' + b);
        }
        final String contentLength = request.headers.get("content-length");
        final int length = contentLength != null ? Integer.parseInt(contentLength) : 0;
        request.body = in.readNBytes(length);
        if (request.body.length != length) {
            return null;
        }
        return request;
    }

    private void serve(Socket socket) {
        try (socket) {
            final InputStream in = new BufferedInputStream(socket.getInputStream());
            final OutputStream out = socket.getOutputStream();
            while (!isClosed) {
                final Request request = readRequest(in);
                if (request == null) {
                    return;
                }
                final Exchange exchange = new Exchange(socket, out, isKeepAlive);
                handler.handle(request, exchange);
                if (exchange.isClosed || !exchange.isKeepAlive) {
                    return;
                }
            }
        } catch (IOException ignore) {
            // the client went away
        } finally {
            sockets.remove(socket);
        }
    }

    @FunctionalInterface
    public interface Handler {
        void handle(Request request, Exchange exchange) throws IOException;
    }

    /**
     * The response side of one request.
     */
    public static final class Exchange {
        private final boolean isKeepAlive;
        private final OutputStream out;
        private final Socket socket;
        private boolean isClosed;

        Exchange(Socket socket, OutputStream out, boolean isKeepAlive) {
            this.socket = socket;
            this.out = out;
            this.isKeepAlive = isKeepAlive;
        }

        /**
         * Resets the connection without sending a response.
         */
        public void closeAbruptly() throws IOException {
            isClosed = true;
            socket.setSoLinger(true, 0);
            socket.close();
        }

        public void respond(int status, Map<String, String> headers, byte[] body) throws IOException {
            writeHead(status, headers, body.length);
            out.write(body);
            out.flush();
        }

        /**
         * Sends the head with the given Content-Length, then the body in {@code chunks} parts with
         * {@code pauseMillis} between them, then closes the connection after {@code bytesToSend}
         * bytes.
         */
        public void respondSlowly(int status, Map<String, String> headers, byte[] body, int bytesToSend, int chunks, long pauseMillis) throws IOException {
            writeHead(status, headers, body.length);
            out.flush();
            final int chunkSize = Math.max(1, bytesToSend / chunks);
            int sent = 0;
            try {
                while (sent < bytesToSend) {
                    final int n = Math.min(chunkSize, bytesToSend - sent);
                    out.write(body, sent, n);
                    out.flush();
                    sent += n;
                    Thread.sleep(pauseMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (sent < body.length) {
                closeAbruptly();
            }
        }

        /**
         * Writes bytes as they are, then closes the connection.
         */
        public void writeRawAndClose(String raw) throws IOException {
            out.write(raw.getBytes(StandardCharsets.UTF_8));
            out.flush();
            isClosed = true;
            socket.close();
        }

        private void writeHead(int status, Map<String, String> headers, long contentLength) throws IOException {
            final StringBuilder head = new StringBuilder();
            head.append("HTTP/1.1 ").append(status).append(" Status").append("\r\n");
            for (Map.Entry<String, String> header : headers.entrySet()) {
                head.append(header.getKey()).append(": ").append(header.getValue()).append("\r\n");
            }
            head.append("Content-Length: ").append(contentLength).append("\r\n");
            if (!isKeepAlive) {
                head.append("Connection: close\r\n");
            }
            head.append("\r\n");
            out.write(head.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static final class Request {
        public final Map<String, String> headers = new LinkedHashMap<>();
        public byte[] body;
        public String method;
        public String path;
        public String rawQuery;

        public String header(String name) {
            return headers.get(name);
        }
    }
}
