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

package io.questdb.tls;

import io.questdb.log.Log;
import io.questdb.network.NetworkFacade;
import io.questdb.network.PlainSocket;
import io.questdb.network.Socket;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Server-side TLS socket on top of {@link SSLEngine}.
 * <p>
 * The handshake is non-blocking: {@link #startTlsSession(CharSequence)} starts it and makes as much progress as
 * the bytes already available allow, and {@link #recv(long, int)} and {@link #send(long, int)} continue it
 * whenever the dispatcher reports the socket ready. Until the handshake completes, both return 0 (no plaintext),
 * so a client that stalls mid-handshake waits in the IO dispatcher like any idle connection instead of holding
 * a worker thread. Handshake bytes that the socket cannot take at once stay in the output buffer;
 * {@link #wantsTlsWrite()} reports them and {@link #tlsIO(int)} flushes them when the socket becomes writable.
 */
public final class JavaTlsServerSocket implements Socket {

    private static final long ADDRESS_FIELD_OFFSET;
    private static final long CAPACITY_FIELD_OFFSET;
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);
    private static final int HS_DONE = 0;
    private static final int HS_NEED_INPUT = 1;
    private static final int HS_NEED_OUTPUT = 2;
    private static final int INITIAL_BUFFER_CAPACITY_BYTES = 256 * 1024;
    private static final long LIMIT_FIELD_OFFSET;
    // How many records in a row may carry no plaintext (key updates, warning alerts, empty records) before the
    // peer is taken for one that only keeps a worker busy. Well-behaved peers send such records one at a time.
    private static final int MAX_EMPTY_RECORDS_IN_A_ROW = 32;
    private static final int MAX_LOGGED_ERROR_CHARS = 256;
    // The largest input buffer the socket grows to: far above the largest TLS record (16 KiB plus overhead), so
    // only a buffer configured below one record ever grows.
    private static final int MAX_UNWRAP_INPUT_CAPACITY_BYTES = 1024 * 1024;
    private static final int SHUT_RD = 0;
    private static final int STATE_CLOSING = 3;
    private static final int STATE_EMPTY = 0;
    private static final int STATE_HANDSHAKE = 4;
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
    private final StringSink errorSink = new StringSink();
    private final Log log;
    private final SSLContext sslContext;
    private final ByteBuffer plaintextStagingBuffer;
    private final ByteBuffer unwrapInputBuffer;
    private final ByteBuffer unwrapOutputBuffer;
    private final ByteBuffer wrapInputBuffer;
    private final ByteBuffer wrapOutputBuffer;
    private int emptyRecordsInARow;
    private boolean isHandshakeFailed;
    private boolean isHandshakeInputShort;
    private boolean morePlaintextBuffered;
    private int plaintextStagedHi;
    private int plaintextStagedLo;
    private int plaintextStagingCapacity;
    private long plaintextStagingPtr;
    private SSLEngine sslEngine;
    private int state = STATE_EMPTY;
    // A copy of the plaintext that send() has encrypted and not reported as sent yet, because the socket did not
    // take all of its ciphertext: at most one TLS record of it, [unackedPlainLo, unackedPlainHi). See send().
    private int unackedPlainCapacity;
    private int unackedPlainHi;
    private int unackedPlainLo;
    private long unackedPlainPtr;
    private long unwrapInputBufferPtr;
    private long wrapOutputBufferPtr;

    public JavaTlsServerSocket(NetworkFacade nf, Log log, SSLContext sslContext) {
        this.delegate = new PlainSocket(nf, log);
        this.log = log;
        this.sslContext = sslContext;
        this.wrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.wrapOutputBuffer = ByteBuffer.allocateDirect(0);
        this.unwrapInputBuffer = ByteBuffer.allocateDirect(0);
        this.plaintextStagingBuffer = ByteBuffer.allocateDirect(0);
    }

    @Override
    public void close() {
        log.debug().$("closing TLS server socket [fd=").$(delegate.getFd()).$(']').$();
        switch (state) {
            case STATE_CLOSING: // intentional fall through
            case STATE_EMPTY:
                return;
            case STATE_HANDSHAKE: // intentional fall through
            case STATE_TLS: {
                assert sslEngine != null;
                state = STATE_CLOSING;
                try {
                    sslEngine.closeOutbound();
                    wrapClosingRecords();
                    while (wantsTlsWrite()) {
                        final var n = tlsIO(Socket.WRITE_FLAG);
                        if (n < 0 || wantsTlsWrite()) {
                            log.debug().$("could not send TLS close_notify").$();
                            break;
                        }
                    }
                } catch (SSLException | RuntimeException e) {
                    // close() must end with the socket closed and the buffers freed, whatever the engine does
                    log.debug().$("could not send TLS close_notify [error=").$(sanitize(e)).I$();
                }
                sslEngine = null;
            } // fall through
            case STATE_PLAINTEXT:
                state = STATE_CLOSING;
                sslEngine = null;
                isHandshakeFailed = false;
                isHandshakeInputShort = false;
                morePlaintextBuffered = false;
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

    /**
     * @return true while the TLS handshake started by {@link #startTlsSession(CharSequence)} is still in progress.
     */
    public boolean isHandshakeInProgress() {
        return state == STATE_HANDSHAKE;
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
        if (state == STATE_HANDSHAKE) {
            final var progress = continueHandshake();
            if (progress <= 0) {
                return progress;
            }
        }
        morePlaintextBuffered = false;
        var received = drainStagedPlaintext(bufferPtr, bufferLen);
        try {
            while (received < bufferLen) {
                final var n = readFromSocket();
                assert unwrapInputBuffer.position() == 0 : "unwrapInputBuffer is not compacted";
                final var bytesAvailable = unwrapInputBuffer.limit();
                if (bytesAvailable == 0) {
                    return received > 0 || n >= 0 ? received : n;
                }
                // SSLEngine needs room for a whole record in the destination; a smaller caller buffer gets the
                // record through the staging buffer, which keeps what does not fit for the next call
                final var room = bufferLen - received;
                final var isDirect = room >= plaintextStagingCapacity;
                final ByteBuffer destination;
                if (isDirect) {
                    resetBufferToPointer(unwrapOutputBuffer, bufferPtr + received, room);
                    destination = unwrapOutputBuffer;
                } else {
                    resetBufferToPointer(plaintextStagingBuffer, plaintextStagingPtr, plaintextStagingCapacity);
                    destination = plaintextStagingBuffer;
                }
                final var result = sslEngine.unwrap(unwrapInputBuffer, destination);
                final var bytesRemaining = bytesAvailable - result.bytesConsumed();
                Vect.memmove(unwrapInputBufferPtr, unwrapInputBufferPtr + result.bytesConsumed(), bytesRemaining);
                unwrapInputBuffer.position(0);
                unwrapInputBuffer.limit(bytesRemaining);
                if (isDirect) {
                    received += result.bytesProduced();
                } else {
                    plaintextStagedLo = 0;
                    plaintextStagedHi = result.bytesProduced();
                    received += drainStagedPlaintext(bufferPtr + received, room);
                }
                if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP && !wrapOwedRecords()) {
                    return -1;
                }
                if (result.bytesProduced() > 0) {
                    emptyRecordsInARow = 0;
                } else if (result.bytesConsumed() > 0 && ++emptyRecordsInARow > MAX_EMPTY_RECORDS_IN_A_ROW) {
                    // each such record costs a pass of this loop and returns nothing to the caller
                    log.info().$("TLS peer sends records without data in a row, closing [fd=").$(delegate.getFd())
                            .$(", records=").$(emptyRecordsInARow).I$();
                    return -1;
                }
                if (isEngineStuck(result)) {
                    return -1;
                }
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        if (bytesRemaining == unwrapInputBuffer.capacity()) {
                            // the buffer is smaller than the record; make room and read the rest
                            growUnwrapInputBuffer();
                            break;
                        }
                        // a partial record; recv() already read what the socket had
                        if (n < 0 && received == 0) {
                            return n;
                        }
                        return received;
                    case BUFFER_OVERFLOW:
                        // the session's record size grew past the staging buffer
                        growPlaintextStagingBuffer();
                        break;
                    case CLOSED:
                        log.debug().$("SSL engine closed").$();
                        return received == 0 ? -1 : received;
                    default:
                        break;
                }
            }
        } catch (SSLException | RuntimeException e) {
            log.error().$("could not unwrap SSL packet [fd=").$(delegate.getFd()).$(", error=").$(sanitize(e)).I$();
            return -1;
        } catch (TlsSessionInitFailedException e) {
            log.error().$("could not unwrap SSL packet [fd=").$(delegate.getFd()).$(", error=").$safe(e.getFlyweightMessage()).I$();
            return -1;
        }
        // The caller's buffer is full: tell the dispatcher to come back without waiting for the socket.
        morePlaintextBuffered = plaintextStagedHi > plaintextStagedLo || unwrapInputBuffer.limit() > 0;
        return received;
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        if (sslEngine == null) {
            return delegate.send(bufferPtr, bufferLen);
        }
        if (state == STATE_HANDSHAKE) {
            final var progress = continueHandshake();
            if (progress <= 0) {
                return progress;
            }
        }
        if (sslEngine.isOutboundDone()) {
            // shutdown(SHUT_WR) has ended the stream: nothing is sent after close_notify, and what a short
            // write left unreported is not reported either. Bytes still queued get one more try, so a failed
            // transport answers with its own error code.
            final var pending = wrapOutputBuffer.position();
            if (pending > 0) {
                final var sent = writeToSocket(pending);
                if (sent < 0) {
                    return sent;
                }
            }
            return -1;
        }
        try {
            // ciphertext an earlier call left behind goes first
            final var pending = wrapOutputBuffer.position();
            if (pending > 0) {
                final var sent = writeToSocket(pending);
                if (sent < 0) {
                    return sent;
                }
                if (sent < pending) {
                    return 0;
                }
            }
            // The socket has everything encrypted so far. Plaintext that an earlier call encrypted without
            // reporting it is sent now; the caller presents those bytes again, as after any short write.
            var plainBytesSent = 0;
            if (unackedPlainHi > unackedPlainLo) {
                plainBytesSent = Math.min(bufferLen, unackedPlainHi - unackedPlainLo);
                if (!Vect.memeq(bufferPtr, unackedPlainPtr + unackedPlainLo, plainBytesSent)) {
                    // skipping these bytes would put the wrong ones behind what the peer already has
                    log.error().$("send() after a short write must present the same bytes again [fd=").$(delegate.getFd()).I$();
                    return -1;
                }
                unackedPlainLo += plainBytesSent;
                if (unackedPlainLo < unackedPlainHi) {
                    // the caller presented fewer bytes than are encrypted: the rest follows in its next call
                    return plainBytesSent;
                }
                unackedPlainLo = 0;
                unackedPlainHi = 0;
            }
            resetBufferToPointer(wrapInputBuffer, bufferPtr, bufferLen);
            wrapInputBuffer.position(plainBytesSent);
            while (wrapInputBuffer.remaining() > 0) {
                final var result = sslEngine.wrap(wrapInputBuffer, wrapOutputBuffer);
                if (isEngineStuck(result)) {
                    return -1;
                }
                switch (result.getStatus()) {
                    case BUFFER_UNDERFLOW:
                        // an SSLException, not an Error, so that the catch below fails the connection
                        throw new SSLException("Underflow while reading a plain text. This should not happen, please report as a bug");
                    case BUFFER_OVERFLOW:
                        // the buffer was empty before the wrap: the record is larger than the whole buffer
                        growWrapOutputBuffer();
                        continue;
                    case CLOSED:
                        log.error().$("Attempt to send to a closed SSLEngine").$();
                        return -1;
                    default:
                        break;
                }
                final var bytesToSend = wrapOutputBuffer.position();
                final var sent = writeToSocket(bytesToSend);
                if (sent < 0) {
                    return sent;
                }
                if (sent < bytesToSend) {
                    // These bytes are encrypted, so they cannot be taken back, yet they are not sent until the
                    // socket takes the rest of their record. Reporting them now would let a caller that closes
                    // after its last send() lose the tail of what it wrote.
                    keepUnackedPlaintext(bufferPtr + plainBytesSent, result.bytesConsumed());
                    return plainBytesSent;
                }
                plainBytesSent += result.bytesConsumed();
            }
            return plainBytesSent;
        } catch (SSLException | RuntimeException e) {
            log.error().$("could not wrap SSL packet [fd=").$(delegate.getFd()).$(", error=").$(sanitize(e)).I$();
            return -1;
        }
    }

    /**
     * Half-closes the connection. Shutting down the write side of a TLS session first queues close_notify and
     * tries to send it together with any ciphertext still queued, so the FIN does not overtake them. The attempt
     * does not wait for the socket: what the socket does not take at once is lost, as on close().
     */
    @Override
    public int shutdown(int how) {
        if (how != SHUT_RD && state == STATE_TLS) {
            try {
                sslEngine.closeOutbound();
                wrapClosingRecords();
            } catch (SSLException | RuntimeException e) {
                log.debug().$("could not queue TLS close_notify [fd=").$(delegate.getFd()).$(", error=").$(sanitize(e)).I$();
            }
            if (tlsIO(Socket.WRITE_FLAG) < 0 || wantsTlsWrite()) {
                log.debug().$("could not send queued TLS bytes before shutdown [fd=").$(delegate.getFd()).I$();
            }
        }
        return delegate.shutdown(how);
    }

    /**
     * Starts the TLS handshake and advances it as far as the bytes already received allow. The call never waits
     * for the client: the handshake continues in {@link #recv(long, int)}, {@link #send(long, int)} and
     * {@link #tlsIO(int)}.
     *
     * @throws TlsSessionInitFailedException when the handshake fails on the bytes already available, or the
     *                                       client has already disconnected
     */
    @Override
    public void startTlsSession(CharSequence peerName) throws TlsSessionInitFailedException {
        assert state == STATE_PLAINTEXT;
        prepareInternalBuffers();
        try {
            sslEngine = createSslEngine();
            sslEngine.beginHandshake();
            state = STATE_HANDSHAKE;
            isHandshakeFailed = false;
            isHandshakeInputShort = false;
            emptyRecordsInARow = 0;
            unackedPlainLo = 0;
            unackedPlainHi = 0;
            advanceHandshake(true);
        } catch (SSLException | RuntimeException e) {
            sslEngine = null;
            state = STATE_PLAINTEXT;
            throw TlsSessionInitFailedException.instance("TLS session creation failed [error=").put(sanitize(e)).put(']');
        } catch (TlsSessionInitFailedException e) {
            sslEngine = null;
            state = STATE_PLAINTEXT;
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
            final var bytesToSend = wrapOutputBuffer.position();
            if (bytesToSend > 0) {
                final var n = writeToSocket(bytesToSend);
                if (n < 0) {
                    return n;
                }
            }
            if (state == STATE_HANDSHAKE && !isHandshakeFailed && wrapOutputBuffer.position() == 0) {
                // The flushed bytes may have been only part of a handshake flight: produce the rest. Unwrapping
                // stays with recv() on a worker thread, as it can run CPU-heavy delegated tasks.
                try {
                    advanceHandshake(false);
                } catch (TlsSessionInitFailedException | SSLException | RuntimeException e) {
                    logHandshakeFailure(e);
                    isHandshakeFailed = true;
                    return -1;
                }
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

    /**
     * Appends {@code message} to {@code sink} as printable ASCII: other characters, the backslash included, become
     * {@code \\uXXXX} escapes, and the text is cut at {@value #MAX_LOGGED_ERROR_CHARS} characters. Engine error
     * messages can carry client-controlled bytes, such as the server name from a ClientHello; written raw, a CR
     * or LF in them would forge log lines.
     */
    public static void appendSanitized(StringSink sink, @Nullable CharSequence message) {
        if (message == null) {
            sink.put("null");
            return;
        }
        final var n = Math.min(message.length(), MAX_LOGGED_ERROR_CHARS);
        for (var i = 0; i < n; i++) {
            final var c = message.charAt(i);
            if (c >= 0x20 && c <= 0x7e && c != '\\') {
                sink.put(c);
            } else {
                sink.put("\\u");
                for (var shift = 12; shift >= 0; shift -= 4) {
                    sink.put(Character.forDigit((c >> shift) & 0xf, 16));
                }
            }
        }
        if (message.length() > n) {
            sink.put("...");
        }
    }

    private static long allocateMemoryAndResetBuffer(ByteBuffer buffer, int capacity) {
        final var newAddress = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        resetBufferToPointer(buffer, newAddress, capacity);
        return newAddress;
    }

    private static long expandBuffer(ByteBuffer buffer, long oldAddress) {
        final var oldCapacity = buffer.capacity();
        final var newCapacity = oldCapacity * 2;
        final var newAddress = Unsafe.realloc(oldAddress, oldCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
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

    /**
     * Advances the handshake without waiting on the socket.
     *
     * @param isUnwrapAllowed false keeps the handshake from reading and unwrapping client bytes
     * @return {@link #HS_DONE} once the handshake has completed, {@link #HS_NEED_INPUT} when it waits for client
     * bytes, {@link #HS_NEED_OUTPUT} when the socket did not take all handshake bytes
     */
    private int advanceHandshake(boolean isUnwrapAllowed) throws SSLException, TlsSessionInitFailedException {
        for (; ; ) {
            switch (sslEngine.getHandshakeStatus()) {
                case NEED_TASK: {
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    break;
                }
                case NEED_WRAP: {
                    if (!flushHandshakeBytes()) {
                        return HS_NEED_OUTPUT;
                    }
                    final var result = sslEngine.wrap(EMPTY_BUFFER, wrapOutputBuffer);
                    switch (result.getStatus()) {
                        case OK:
                            final var isFlushed = flushHandshakeBytes();
                            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
                                    || sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
                                // That was the last handshake output (the server Finished in TLS 1.2, the session
                                // ticket in TLS 1.3): the handshake is complete even if the socket took only part
                                // of it. send() and tlsIO() flush the rest. Completing here, on the thread that
                                // called recv(), lets recv() go on to client data already buffered behind the
                                // client's Finished; completing later inside tlsIO() would leave that data waiting
                                // for a READ event that never comes, because the socket has nothing left to read.
                                completeHandshake();
                                return HS_DONE;
                            }
                            if (!isFlushed) {
                                return HS_NEED_OUTPUT;
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            // flushHandshakeBytes() emptied the buffer before the wrap, so the record is larger
                            // than the whole buffer
                            growWrapOutputBuffer();
                            break;
                        case CLOSED:
                            // the engine produced a fatal alert; send it on a best-effort basis
                            flushHandshakeBytes();
                            throw TlsSessionInitFailedException.instance("TLS handshake failed: engine closed");
                        default:
                            // a handshake failure, not an Error, so that the callers fail the connection
                            throw TlsSessionInitFailedException.instance("Buffer underflow during TLS handshake wrap. This should not happen, please report as a bug");
                    }
                    break;
                }
                case NEED_UNWRAP:
                case NEED_UNWRAP_AGAIN: {
                    if (!isUnwrapAllowed) {
                        return HS_NEED_INPUT;
                    }
                    if (unwrapInputBuffer.limit() == 0 || isHandshakeInputShort) {
                        final var n = readFromSocket();
                        if (n < 0) {
                            throw TlsSessionInitFailedException.instance("socket read error");
                        }
                        if (n == 0) {
                            return HS_NEED_INPUT;
                        }
                        isHandshakeInputShort = false;
                    }
                    final var bytesAvailable = unwrapInputBuffer.limit();
                    final var result = sslEngine.unwrap(unwrapInputBuffer, EMPTY_BUFFER);
                    final var bytesRemaining = bytesAvailable - result.bytesConsumed();
                    Vect.memmove(unwrapInputBufferPtr, unwrapInputBufferPtr + result.bytesConsumed(), bytesRemaining);
                    unwrapInputBuffer.position(0);
                    unwrapInputBuffer.limit(bytesRemaining);
                    switch (result.getStatus()) {
                        case BUFFER_UNDERFLOW:
                            if (bytesRemaining == unwrapInputBuffer.capacity()) {
                                growUnwrapInputBuffer();
                            }
                            isHandshakeInputShort = true;
                            break;
                        case BUFFER_OVERFLOW:
                            // Only application data needs room in the destination, so the handshake has completed
                            // and the next record is already application data: recv() unwraps it into the
                            // caller's buffer.
                            completeHandshake();
                            return HS_DONE;
                        case CLOSED:
                            throw TlsSessionInitFailedException.instance("client closed connection unexpectedly");
                        default:
                            break;
                    }
                    break;
                }
                default: // FINISHED and NOT_HANDSHAKING
                    completeHandshake();
                    return HS_DONE;
            }
        }
    }

    private void completeHandshake() {
        // a handshake completes at most once per connection, and close() frees the staging buffer
        assert plaintextStagingPtr == 0;
        final var capacity = sslEngine.getSession().getApplicationBufferSize();
        // allocate before changing state, so a failed allocation leaves the socket in the handshake state
        plaintextStagingPtr = Unsafe.malloc(capacity, MemoryTag.NATIVE_TLS_RSS);
        plaintextStagingCapacity = capacity;
        plaintextStagedLo = 0;
        plaintextStagedHi = 0;
        state = STATE_TLS;
        isHandshakeInputShort = false;
        morePlaintextBuffered = unwrapInputBuffer.limit() > 0;
    }

    /**
     * @return 1 once the handshake has completed, 0 while it waits for the socket, -1 when it has failed
     */
    private int continueHandshake() {
        if (isHandshakeFailed) {
            return -1;
        }
        try {
            return advanceHandshake(true) == HS_DONE ? 1 : 0;
        } catch (TlsSessionInitFailedException | SSLException | RuntimeException e) {
            logHandshakeFailure(e);
            isHandshakeFailed = true;
            return -1;
        }
    }

    private SSLEngine createSslEngine() {
        final var engine = sslContext.createSSLEngine();
        engine.setUseClientMode(false);
        return engine;
    }

    private int drainStagedPlaintext(long bufferPtr, int bufferLen) {
        final var staged = plaintextStagedHi - plaintextStagedLo;
        if (staged == 0) {
            return 0;
        }
        final var n = Math.min(staged, bufferLen);
        Vect.memcpy(bufferPtr, plaintextStagingPtr + plaintextStagedLo, n);
        plaintextStagedLo += n;
        if (plaintextStagedLo == plaintextStagedHi) {
            plaintextStagedLo = 0;
            plaintextStagedHi = 0;
        }
        return n;
    }

    /**
     * @return true when no handshake bytes remain to send
     */
    private boolean flushHandshakeBytes() throws TlsSessionInitFailedException {
        final var bytesToSend = wrapOutputBuffer.position();
        if (bytesToSend > 0 && writeToSocket(bytesToSend) < 0) {
            throw TlsSessionInitFailedException.instance("socket write error");
        }
        return wrapOutputBuffer.position() == 0;
    }

    private void freeInternalBuffers() {
        // each buffer on its own: a failed allocation can leave any subset of them allocated
        if (plaintextStagingPtr != 0) {
            resetBufferToPointer(plaintextStagingBuffer, 0, 0);
            Unsafe.free(plaintextStagingPtr, plaintextStagingCapacity, MemoryTag.NATIVE_TLS_RSS);
            plaintextStagingPtr = 0;
            plaintextStagingCapacity = 0;
            plaintextStagedLo = 0;
            plaintextStagedHi = 0;
        }
        if (unackedPlainPtr != 0) {
            Unsafe.free(unackedPlainPtr, unackedPlainCapacity, MemoryTag.NATIVE_TLS_RSS);
            unackedPlainPtr = 0;
            unackedPlainCapacity = 0;
        }
        unackedPlainLo = 0;
        unackedPlainHi = 0;
        if (wrapOutputBufferPtr != 0) {
            final var capacity = wrapOutputBuffer.capacity();
            resetBufferToPointer(wrapOutputBuffer, 0, 0);
            Unsafe.free(wrapOutputBufferPtr, capacity, MemoryTag.NATIVE_TLS_RSS);
            wrapOutputBufferPtr = 0;
        }
        if (unwrapInputBufferPtr != 0) {
            final var capacity = unwrapInputBuffer.capacity();
            resetBufferToPointer(unwrapInputBuffer, 0, 0);
            Unsafe.free(unwrapInputBufferPtr, capacity, MemoryTag.NATIVE_TLS_RSS);
            unwrapInputBufferPtr = 0;
        }
    }

    /**
     * Detects an engine that can no longer make progress on application data. This server does not support
     * renegotiation or any other handshake after the first one: a client that starts one (a ClientHello after the
     * handshake) leaves the engine waiting for a delegated task that nobody runs, and every later wrap or unwrap
     * returns OK without consuming or producing a byte, so the caller's loop would spin forever.
     */
    private boolean isEngineStuck(SSLEngineResult result) {
        final var handshakeStatus = result.getHandshakeStatus();
        final var isStuck = handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_TASK
                || handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP
                || handshakeStatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN
                || (result.getStatus() == SSLEngineResult.Status.OK && result.bytesConsumed() == 0 && result.bytesProduced() == 0);
        if (isStuck) {
            log.info().$("TLS peer started a handshake after the handshake; renegotiation is not supported, closing [fd=")
                    .$(delegate.getFd()).$(", handshakeStatus=").$(handshakeStatus.name()).I$();
        }
        return isStuck;
    }

    private void growPlaintextStagingBuffer() {
        final var newCapacity = plaintextStagingCapacity * 2;
        plaintextStagingPtr = Unsafe.realloc(plaintextStagingPtr, plaintextStagingCapacity, newCapacity, MemoryTag.NATIVE_TLS_RSS);
        plaintextStagingCapacity = newCapacity;
    }

    private void growUnwrapInputBuffer() throws TlsSessionInitFailedException {
        if (unwrapInputBuffer.capacity() >= MAX_UNWRAP_INPUT_CAPACITY_BYTES) {
            throw TlsSessionInitFailedException.instance("TLS record does not fit into the input buffer");
        }
        final var limit = unwrapInputBuffer.limit();
        unwrapInputBufferPtr = expandBuffer(unwrapInputBuffer, unwrapInputBufferPtr);
        unwrapInputBuffer.position(0);
        unwrapInputBuffer.limit(limit);
    }

    private void growWrapOutputBuffer() {
        wrapOutputBufferPtr = expandBuffer(wrapOutputBuffer, wrapOutputBufferPtr);
    }

    // one wrap() consumes at most one record of plaintext, so the copy stays small
    private void keepUnackedPlaintext(long ptr, int len) {
        if (len > unackedPlainCapacity) {
            unackedPlainPtr = Unsafe.realloc(unackedPlainPtr, unackedPlainCapacity, len, MemoryTag.NATIVE_TLS_RSS);
            unackedPlainCapacity = len;
        }
        if (len > 0) {
            Vect.memcpy(unackedPlainPtr, ptr, len);
        }
        unackedPlainLo = 0;
        unackedPlainHi = len;
    }

    private void logHandshakeFailure(Exception e) {
        final var line = log.info().$("TLS handshake failed [fd=").$(delegate.getFd()).$(", error=");
        if (e instanceof TlsSessionInitFailedException tlsException) {
            // messages this class writes itself: constants, or text that sanitize() has already escaped
            line.$safe(tlsException.getFlyweightMessage());
        } else {
            line.$(sanitize(e));
        }
        line.I$();
    }

    private CharSequence sanitize(Exception e) {
        errorSink.clear();
        errorSink.put(e.getClass().getSimpleName()).put(": ");
        appendSanitized(errorSink, e.getMessage());
        return errorSink;
    }

    private void prepareInternalBuffers() {
        // a previous startTlsSession() on this connection may have failed and left its buffers behind
        final var initialCapacity = Integer.getInteger("questdb.experimental.tls.buffersize", INITIAL_BUFFER_CAPACITY_BYTES);
        if (wrapOutputBufferPtr == 0) {
            wrapOutputBufferPtr = allocateMemoryAndResetBuffer(wrapOutputBuffer, initialCapacity);
        }
        wrapOutputBuffer.position(0);
        if (unwrapInputBufferPtr == 0) {
            unwrapInputBufferPtr = allocateMemoryAndResetBuffer(unwrapInputBuffer, initialCapacity);
        }
        unwrapInputBuffer.position(0);
        unwrapInputBuffer.limit(0);
    }

    private int readFromSocket() {
        final var writerPos = unwrapInputBuffer.limit();
        final var freeSpace = unwrapInputBuffer.capacity() - writerPos;
        if (freeSpace == 0) {
            return 0;
        }
        assert Unsafe.getUnsafe().getLong(unwrapInputBuffer, ADDRESS_FIELD_OFFSET) == unwrapInputBufferPtr;
        final var adjustedPtr = unwrapInputBufferPtr + writerPos;
        final var n = delegate.recv(adjustedPtr, freeSpace);
        if (n < 0) {
            return n;
        }
        unwrapInputBuffer.limit(writerPos + n);
        return n;
    }

    /**
     * Encrypts what the engine still owes the peer once its outbound side is closed: the answers to
     * post-handshake requests first, close_notify last, one record per wrap. What the socket does not take is
     * left in the buffer for the caller's single flush attempt.
     */
    private void wrapClosingRecords() throws SSLException {
        while (!sslEngine.isOutboundDone()) {
            final var before = wrapOutputBuffer.position();
            final var result = sslEngine.wrap(EMPTY_BUFFER, wrapOutputBuffer);
            if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                if (before == 0) {
                    growWrapOutputBuffer();
                } else if (writeToSocket(before) <= 0) {
                    if (wrapOutputBuffer.capacity() >= INITIAL_BUFFER_CAPACITY_BYTES) {
                        // the socket takes nothing: the rest is lost, as on any close with unsent bytes
                        return;
                    }
                    // a buffer configured small: make room, so the caller's flush can carry close_notify too
                    final var pending = wrapOutputBuffer.position();
                    growWrapOutputBuffer();
                    wrapOutputBuffer.position(pending);
                }
            } else if (result.bytesProduced() == 0) {
                return;
            }
        }
    }

    /**
     * Encrypts the answers the engine owes after a post-handshake request, such as a TLS 1.3 key update, so
     * that {@link #wantsTlsWrite()} reports them and the dispatcher sends them. Left inside the engine they
     * would pile up unseen for as long as the server only reads, one record per request the client cares to
     * send.
     *
     * @return false when the peer asks faster than it reads the answers: the output buffer is full of them and
     * the socket takes none
     */
    private boolean wrapOwedRecords() throws SSLException {
        while (sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
            final var before = wrapOutputBuffer.position();
            final var result = sslEngine.wrap(EMPTY_BUFFER, wrapOutputBuffer);
            switch (result.getStatus()) {
                case BUFFER_OVERFLOW:
                    if (before == 0) {
                        growWrapOutputBuffer();
                    } else if (writeToSocket(before) <= 0) {
                        // The socket takes nothing right now. A buffer configured below the default grows up to
                        // it, so that a slow reader's occasional key update still fits; past that the answers
                        // only pile up, and the connection goes.
                        if (wrapOutputBuffer.capacity() >= INITIAL_BUFFER_CAPACITY_BYTES) {
                            log.info().$("TLS peer requests key updates faster than it reads the answers, closing [fd=")
                                    .$(delegate.getFd()).I$();
                            return false;
                        }
                        final var pending = wrapOutputBuffer.position();
                        growWrapOutputBuffer();
                        wrapOutputBuffer.position(pending);
                    }
                    break;
                case CLOSED:
                    return true;
                default:
                    if (result.bytesProduced() == 0) {
                        return true;
                    }
                    break;
            }
        }
        return true;
    }

    private int writeToSocket(int bytesToSend) {
        final var n = delegate.send(wrapOutputBufferPtr, bytesToSend);
        if (n < 0) {
            return n;
        }
        final var bytesRemaining = bytesToSend - n;
        Vect.memmove(wrapOutputBufferPtr, wrapOutputBufferPtr + n, bytesRemaining);
        wrapOutputBuffer.position(bytesRemaining);
        return n;
    }
}
