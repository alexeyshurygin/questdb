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

package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public final class JavaTlsServerSocket implements Socket {

    private static final long ADDRESS_FIELD_OFFSET;
    private static final long CAPACITY_FIELD_OFFSET;
    private static final int INITIAL_BUFFER_CAPACITY_BYTES = 256 * 1024;
    private static final long LIMIT_FIELD_OFFSET;
    private static final int STATE_CLOSING = 3;
    private static final int STATE_EMPTY = 0;
    private static final int STATE_PLAINTEXT = 1;
    private static final int STATE_TLS = 2;

    static {
        Field addressField;
        Field limitField;
        Field capacityField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            limitField = Buffer.class.getDeclaredField("limit");
            capacityField = Buffer.class.getDeclaredField("capacity");
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
        ADDRESS_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(addressField);
        LIMIT_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(limitField);
        CAPACITY_FIELD_OFFSET = Unsafe.getUnsafe().objectFieldOffset(capacityField);
    }

    private final Socket delegate;
    private final Log log;
    private final SSLContext sslContext;
    private final ByteBuffer unwrapInputBuffer;
    private final ByteBuffer unwrapOutputBuffer;
    private final ByteBuffer wrapInputBuffer;
    private final ByteBuffer wrapOutputBuffer;
    private boolean morePlaintextBuffered;
    private SSLEngine sslEngine;
    private int state = STATE_EMPTY;
    private long unwrapInputBufferPtr;
    private long wrapOutputBufferPtr;

    JavaTlsServerSocket(NetworkFacade nf, Log log, SSLContext sslContext) {
        this.delegate = new PlainSocket(nf, log);
        this.log = log;
        this.sslContext = sslContext;
        this.wrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(0);
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
        final long newAddress = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, capacity);
        return newAddress;
    }

    private static long expandBuffer(ByteBuffer buffer, long oldAddress) {
        final int oldCapacity = buffer.capacity();
        final int newCapacity = oldCapacity * 2;
        final long newAddress = Unsafe.realloc(oldAddress, oldCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, newCapacity);
        return newAddress;
    }

    private static void resetBufferToPointer(ByteBuffer buffer, long ptr, int len) {
        assert buffer.isDirect();
        Unsafe.getUnsafe().putLong(buffer, ADDRESS_FIELD_OFFSET, ptr);
        Unsafe.getUnsafe().putLong(buffer, LIMIT_FIELD_OFFSET, len);
        Unsafe.getUnsafe().putLong(buffer, CAPACITY_FIELD_OFFSET, len);
        buffer.position(0);
    }

    @Override
    public void close() {
        log.debug().$("closing TLS server socket [fd=").$(delegate.getFd()).$(']').$();
        switch (state) {
            case STATE_CLOSING: // intentional fall through
            case STATE_EMPTY:
                return;
            case STATE_TLS: {
                assert sslEngine != null;
                state = STATE_CLOSING;
                sslEngine.closeOutbound();
                try {
                    sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                    while (wantsTlsWrite()) {
                        final int n = tlsIO(Socket.WRITE_FLAG);
                        if (n <= 0) {
                            log.debug().$("could not send TLS close_notify").$();
                            break;
                        }
                    }
                } catch (SSLException e) {
                    log.debug().$("could not send TLS close_notify").$(e).$();
                }
                sslEngine = null;
            } // fall through
            case STATE_PLAINTEXT:
                state = STATE_CLOSING;
                sslEngine = null;
                freeInternalBuffers();
                delegate.close();
                state = STATE_EMPTY;
                break;
        }
    }

    @Override
    public long getFd() {
        return delegate.getFd();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public boolean isMorePlaintextBuffered() {
        return morePlaintextBuffered;
    }

    @Override
    public boolean isTlsSessionStarted() {
        return sslEngine != null;
    }

    @Override
    public void of(long fd) {
        assert state == STATE_EMPTY;
        delegate.of(fd);
        state = STATE_PLAINTEXT;
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        if (sslEngine == null) {
            return delegate.recv(bufferPtr, bufferLen);
        }
        morePlaintextBuffered = false;
        resetBufferToPointer(unwrapOutputBuffer, bufferPtr, bufferLen);
        unwrapOutputBuffer.position(0);
        try {
            int plainBytesReceived = 0;
            for (; ; ) {
                final int n = readFromSocket();
                assert unwrapInputBuffer.position() == 0 : "unwrapInputBuffer is not compacted";
                final int bytesAvailable = unwrapInputBuffer.limit();
                if (n < 0 && bytesAvailable == 0) {
                    if (plainBytesReceived == 0) {
                        return n;
                    }
                    return plainBytesReceived;
                }
                if (bytesAvailable == 0) {
                    return plainBytesReceived;
                }
                final SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                plainBytesReceived += result.bytesProduced();
                final int bytesConsumed = result.bytesConsumed();
                final int bytesRemaining = bytesAvailable - bytesConsumed;
                Vect.memmove(unwrapInputBufferPtr, unwrapInputBufferPtr + bytesConsumed, bytesRemaining);
                unwrapInputBuffer.position(0);
                unwrapInputBuffer.limit(bytesRemaining);
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        return plainBytesReceived;
                    case BUFFER_OVERFLOW:
                        if (unwrapOutputBuffer.position() == 0) {
                            throw new AssertionError("Output buffer too small to fit a single TLS record. This should not happen, please report as a bug.");
                        }
                        if (bytesRemaining > 0) {
                            morePlaintextBuffered = true;
                        }
                        return plainBytesReceived;
                    case OK:
                        if (bytesRemaining > 0 && unwrapOutputBuffer.remaining() == 0) {
                            morePlaintextBuffered = true;
                            return plainBytesReceived;
                        }
                        break;
                    case CLOSED:
                        log.debug().$("SSL engine closed").$();
                        return plainBytesReceived == 0 ? -1 : plainBytesReceived;
                }
            }
        } catch (SSLException e) {
            log.error().$("could not unwrap SSL packet").$(e).$();
            return -1;
        }
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        if (sslEngine == null) {
            return delegate.send(bufferPtr, bufferLen);
        }
        try {
            resetBufferToPointer(wrapInputBuffer, bufferPtr, bufferLen);
            wrapInputBuffer.position(0);
            int plainBytesConsumed = 0;
            for (; ; ) {
                final int bytesToSend = wrapOutputBuffer.position();
                if (bytesToSend > 0) {
                    final int sent = writeToSocket(bytesToSend);
                    if (sent < 0) {
                        return sent;
                    } else if (sent < bytesToSend) {
                        return plainBytesConsumed;
                    }
                }
                if (wrapInputBuffer.remaining() == 0) {
                    return plainBytesConsumed;
                }
                final SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                plainBytesConsumed += result.bytesConsumed();
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        throw new AssertionError("Underflow while reading a plain text. This should not happen, please report as a bug");
                    case BUFFER_OVERFLOW:
                        if (wrapOutputBuffer.position() == 0) {
                            growWrapOutputBuffer();
                        }
                        break;
                    case OK:
                        break;
                    case CLOSED:
                        log.error().$("Attempt to send to a closed SSLEngine").$();
                        return -1;
                }
            }
        } catch (SSLException e) {
            log.error().$("could not wrap SSL packet").$(e).$();
            return -1;
        }
    }

    @Override
    public int shutdown(int how) {
        return delegate.shutdown(how);
    }

    @Override
    public void startTlsSession(CharSequence peerName) throws TlsSessionInitFailedException {
        assert state == STATE_PLAINTEXT;
        prepareInternalBuffers();
        try {
            this.sslEngine = createSslEngine();
            this.sslEngine.beginHandshake();
            int zeroProgressCount = 0;
            SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
            while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED) {
                switch (handshakeStatus) {
                    case NEED_TASK:
                        Runnable task;
                        while ((task = sslEngine.getDelegatedTask()) != null) {
                            task.run();
                        }
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    case NEED_WRAP: {
                        final SSLEngineResult result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                        switch (result.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                throw new AssertionError("Buffer underflow during TLS handshake. This should not happen, please report as a bug");
                            case BUFFER_OVERFLOW:
                                throw new AssertionError("Buffer overflow during TLS handshake. This should not happen, please report as a bug");
                            case OK:
                                int written = 0;
                                final int bufferLimit = wrapOutputBuffer.position();
                                while (written < bufferLimit) {
                                    final int n = delegate.send(wrapOutputBufferPtr + written, bufferLimit - written);
                                    if (n < 0) {
                                        throw TlsSessionInitFailedException.instance("socket write error");
                                    }
                                    if (n == 0) {
                                        zeroProgressCount++;
                                        if (zeroProgressCount > 1000) {
                                            throw TlsSessionInitFailedException.instance("socket not making progress during TLS handshake write");
                                        }
                                        Thread.yield();
                                    } else {
                                        zeroProgressCount = 0;
                                    }
                                    written += n;
                                }
                                wrapOutputBuffer.clear();
                                break;
                            case CLOSED:
                                throw TlsSessionInitFailedException.instance("client closed connection unexpectedly");
                        }
                        break;
                    }
                    case NEED_UNWRAP: {
                        final int n = readFromSocket();
                        if (n < 0) {
                            throw TlsSessionInitFailedException.instance("socket read error");
                        }
                        if (n == 0 && unwrapInputBuffer.limit() == 0) {
                            zeroProgressCount++;
                            if (zeroProgressCount > 1000) {
                                throw TlsSessionInitFailedException.instance("socket not making progress during TLS handshake read");
                            }
                            Thread.yield();
                            break;
                        }
                        zeroProgressCount = 0;
                        final SSLEngineResult result = sslEngine.unwrap(unwrapInputBuffer, unwrapOutputBuffer);
                        handshakeStatus = result.getHandshakeStatus();
                        switch (result.getStatus()) {
                            case BUFFER_UNDERFLOW:
                                break;
                            case BUFFER_OVERFLOW:
                                throw new AssertionError("Buffer overflow during TLS handshake. This should not happen, please report as a bug");
                            case OK:
                                break;
                            case CLOSED:
                                throw TlsSessionInitFailedException.instance("client closed connection unexpectedly");
                        }
                    }
                    break;
                }
            }
            // compact the unwrap input buffer — there may be application data
            // that arrived in the same TCP segment as the final handshake message
            final int consumed = unwrapInputBuffer.position();
            final int remaining = unwrapInputBuffer.limit() - consumed;
            if (remaining > 0) {
                Vect.memmove(unwrapInputBufferPtr, unwrapInputBufferPtr + consumed, remaining);
            }
            unwrapInputBuffer.position(0);
            unwrapInputBuffer.limit(remaining);
            morePlaintextBuffered = remaining > 0;
            unwrapOutputBuffer.clear();
            wrapOutputBuffer.clear();
            state = STATE_TLS;
        } catch (SSLException e) {
            sslEngine = null;
            throw TlsSessionInitFailedException.instance("TLS session creation failed [error=").put(e.getMessage()).put(']');
        } catch (TlsSessionInitFailedException e) {
            sslEngine = null;
            throw e;
        }
    }

    @Override
    public boolean supportsTls() {
        return true;
    }

    @Override
    public int tlsIO(int readinessFlags) {
        if ((readinessFlags & WRITE_FLAG) != 0) {
            final int bytesToSend = wrapOutputBuffer.position();
            if (bytesToSend > 0) {
                final int n = writeToSocket(bytesToSend);
                return Math.min(n, 0);
            }
        }
        return 0;
    }

    @Override
    public boolean wantsTlsRead() {
        return false;
    }

    @Override
    public boolean wantsTlsWrite() {
        return wrapOutputBuffer.position() > 0;
    }

    private SSLEngine createSslEngine() {
        final SSLEngine engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        return engine;
    }

    private void freeInternalBuffers() {
        long ptrToFree = wrapOutputBufferPtr;
        if (ptrToFree != 0) {
            int capacity = wrapOutputBuffer.capacity();
            assert capacity != 0;
            resetBufferToPointer(wrapOutputBuffer, 0, 0);
            wrapOutputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);
            assert unwrapInputBufferPtr != 0;
            capacity = unwrapInputBuffer.capacity();
            assert capacity != 0;
            resetBufferToPointer(unwrapInputBuffer, 0, 0);
            ptrToFree = unwrapInputBufferPtr;
            unwrapInputBufferPtr = 0;
            Unsafe.free(ptrToFree, capacity, MemoryTag.NATIVE_TLS_RSS);
        }
    }

    private void growWrapOutputBuffer() {
        wrapOutputBufferPtr = expandBuffer(wrapOutputBuffer, wrapOutputBufferPtr);
    }

    private void prepareInternalBuffers() {
        final int initialCapacity = Integer.getInteger("questdb.experimental.tls.buffersize", INITIAL_BUFFER_CAPACITY_BYTES);
        this.wrapOutputBufferPtr = allocateMemoryAndResetBuffer(wrapOutputBuffer, initialCapacity);
        this.unwrapInputBufferPtr = allocateMemoryAndResetBuffer(unwrapInputBuffer, initialCapacity);
        unwrapInputBuffer.flip();
    }

    private int readFromSocket() {
        final int writerPos = unwrapInputBuffer.limit();
        final int freeSpace = unwrapInputBuffer.capacity() - writerPos;
        if (freeSpace == 0) {
            return 0;
        }
        assert Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET) == unwrapInputBufferPtr;
        final long adjustedPtr = unwrapInputBufferPtr + writerPos;
        final int n = delegate.recv(adjustedPtr, freeSpace);
        if (n < 0) {
            return n;
        }
        unwrapInputBuffer.limit(writerPos + n);
        return n;
    }

    private int writeToSocket(int bytesToSend) {
        final int n = delegate.send(wrapOutputBufferPtr, bytesToSend);
        if (n < 0) {
            return n;
        }
        final int bytesRemaining = bytesToSend - n;
        Vect.memmove(wrapOutputBufferPtr, wrapOutputBufferPtr + n, bytesRemaining);
        wrapOutputBuffer.position(bytesRemaining);
        return n;
    }
}
