package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.util.EmptyStatement.ignore;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;

public class SSLChannel extends NioChannel {

    private final ILogger logger = Logger.getLogger(SSLChannel.class);

    private ByteBuffer appBuffer;
    private final Object lock = new Object();
    private final ByteBuffer emptyBuffer;
    private ByteBuffer netOutBuffer;
    private ByteBuffer netInBuffer;
    private final SSLEngine sslEngine;
    private volatile boolean handshakeCompleted;

    // doesn't need to be volatile since only used in synchronized handshake
    private SSLEngineResult handshakeResult;

    public SSLChannel(SSLEngine sslEngine, SocketChannel socketChannel, String mutualAuthentication, boolean directBuffer,
                      boolean clientMode) throws Exception {
        super(socketChannel, clientMode);
        this.sslEngine = sslEngine;

        // In case of the OpenSSL based SSLEngine implementation, the below calls are ignored.
        // For configuration see the OpenSSLEngineFactory
        if ("REQUIRED".equals(mutualAuthentication)) {
            sslEngine.setNeedClientAuth(true);
        } else if ("OPTIONAL".equals(mutualAuthentication)) {
            sslEngine.setWantClientAuth(true);
        }
        SSLSession session = sslEngine.getSession();
        this.appBuffer = newByteBuffer(session.getApplicationBufferSize(), directBuffer);
        this.emptyBuffer = ByteBuffer.allocate(0);
        int netBufferMax = session.getPacketBufferSize();
        this.netOutBuffer = newByteBuffer(netBufferMax, directBuffer);
        this.netInBuffer = newByteBuffer(netBufferMax, directBuffer);
        sslEngine.beginHandshake();
    }

    @SuppressWarnings({
            "checkstyle:cyclomaticcomplexity",
            "checkstyle:npathcomplexity",
            "checkstyle:methodlength",
            "checkstyle:magicnumber"
    })
    private void handshake() throws IOException {
        synchronized (lock) {
            if (handshakeCompleted) {
                return;
            }
            int counter = 0;
            writeToSocket(emptyBuffer);
            while (counter++ < 250 && handshakeResult.getHandshakeStatus() != FINISHED) {
                if (handshakeResult.getHandshakeStatus() == NEED_UNWRAP) {
                    netInBuffer.clear();
                    for (; ; ) {
                        int read = socketChannel.read(netInBuffer);
                        if (read == -1) {
                            throw new SSLException("Remote socket closed during SSL/TLS handshake. This is probably caused by a "
                                    + "SSL/TLS authentication problem resulting in the remote side closing the socket.");
                        } else if (read == 0) {
                            // no data; lets wait a bit and retry
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException e) {
                                throw new IOException(e);
                            }
                        } else {
                            // there is data; lets end the loop
                            break;
                        }
                    }
                    netInBuffer.flip();
                    unwrap();
                    if (handshakeResult.getHandshakeStatus() != FINISHED) {
                        emptyBuffer.clear();
                        writeToSocket(emptyBuffer);
                    }
                } else if (handshakeResult.getHandshakeStatus() == NEED_WRAP) {
                    emptyBuffer.clear();
                    writeToSocket(emptyBuffer);
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
            }
            if (handshakeResult.getHandshakeStatus() != FINISHED) {
                throw new SSLHandshakeException("SSL handshake failed after " + counter
                        + " trials! -> " + handshakeResult.getHandshakeStatus());
            }

            if (logger.isFineEnabled()) {
                SSLSession sslSession = sslEngine.getSession();
                logger.fine("handshake finished, channel=" + this + " protocol=" + sslSession.getProtocol() + ", cipherSuite="
                    + sslSession.getCipherSuite());
            }

            appBuffer.clear();
            appBuffer.flip();
            handshakeCompleted = true;
        }
    }


    private void handleTasks() {
        Runnable task;
        while ((task = sslEngine.getDelegatedTask()) != null) {
            task.run();
        }
    }

    /**
     * Expands the original ByteBuffer. This method expects the original buffer to be in write mode and the resulting
     * ByteBuffer will also be in write mode.
     */
    static ByteBuffer expandBufferInReadMode(ByteBuffer original, int newCapacity) {
        ByteBuffer expanded = newByteBuffer(newCapacity, original.isDirect());
        expanded.put(original);
        // we need to put the buffer back into reading mode
        expanded.flip();
        return expanded;
    }

    static ByteBuffer expandBufferInWriteMode(ByteBuffer original, int newCapacity) {
        ByteBuffer expanded = newByteBuffer(newCapacity, original.isDirect());
        original.flip();
        expanded.put(original);
        return expanded;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!handshakeCompleted) {
            handshake();
        }
        return writeToSocket(src);
    }

    private int writeToSocket(ByteBuffer src) throws IOException {
        wrap(src);

        netOutBuffer.flip();

        int bytesWrittenToSocket = socketChannel.write(netOutBuffer);
        compactOrClear(netOutBuffer);
        return bytesWrittenToSocket;
    }

    @Override
    @SuppressWarnings({
            "checkstyle:cyclomaticcomplexity",
            "checkstyle:npathcomplexity",
            "checkstyle:methodlength"
    })
    public int read(ByteBuffer dst) throws IOException {
        if (!handshakeCompleted) {
            handshake();
        }

        compactOrClear(appBuffer);
        compactOrClear(netInBuffer);

        boolean socketReadFailure = socketChannel.read(netInBuffer) == -1;

        netInBuffer.flip();

        unwrap();

        return socketReadFailure ? -1 : readFromAppBuffer(dst);
    }

    private static void compactOrClear(ByteBuffer bb) {
        if (bb.hasRemaining()) {
            bb.compact();
        } else {
            bb.clear();
        }
    }

    /**
     * Assumes the appBuffer is in writing mode.
     */
    private int readFromAppBuffer(ByteBuffer dst) {
        appBuffer.flip();

        int appBufferRemaining = appBuffer.remaining();
        int outputRemaining = dst.remaining();

        if (outputRemaining < appBufferRemaining) {
            // there is not enough space to all data, so lets copy what we can copy.
            for (int i = 0; i < outputRemaining; i++) {
                dst.put(appBuffer.get());
            }
            return outputRemaining;
        } else {
            // there is enough space to copy all data
            dst.put(appBuffer);
            return appBufferRemaining;
        }
    }

    private void wrap(ByteBuffer src) throws SSLException {
        for (; ; ) {
            SSLEngineResult wrapResult = sslEngine.wrap(src, netOutBuffer);
            this.handshakeResult = wrapResult;
            switch (wrapResult.getStatus()) {
                case OK:
                    // wrap was a success; we done
                    return;
                case CLOSED:
                    // do nothing; sslEngine is closed.
                    return;
                case BUFFER_OVERFLOW:
                    // the netOutBuffer doesn't contain enough data for decoding. So we need to expand it.
                    if (sslEngine.getSession().getPacketBufferSize() > netOutBuffer.capacity()) {
                        netOutBuffer = expandBufferInWriteMode(netOutBuffer, sslEngine.getSession().getPacketBufferSize());
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private String toDebug(String name, ByteBuffer bb) {
        return name + "{pos=" + bb.position() + " limit=" + bb.limit() + " cap:" + bb.capacity() + "}";
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private void unwrap() throws SSLException {
        SSLEngineResult unwrapResult;
        boolean atLeastOneUnwrapped = false;
        while (netInBuffer.hasRemaining()) {
            unwrapResult = sslEngine.unwrap(netInBuffer, appBuffer);
            this.handshakeResult = unwrapResult;

            switch (unwrapResult.getStatus()) {
                case OK:
                    atLeastOneUnwrapped = true;
                    break;
                case BUFFER_OVERFLOW:
                    if (atLeastOneUnwrapped) {
                        return;
                    }

                    // the appBuffer wasn't big enough, so lets expand it and try again
                    // for OpenSSL you can't rely on getApplicationBufferSize; it sometimes gives a too low value
                    // and this can lead to an infinite loop
                    this.appBuffer = expandBufferInWriteMode(appBuffer, appBuffer.capacity() * 2);
                    break;
                case BUFFER_UNDERFLOW:
                    if (atLeastOneUnwrapped) {
                        return;
                    }

                    // Resize buffer if needed.
                    if (netInBuffer.limit() == netInBuffer.capacity()) {
                        // the buffer is full; no more bytes can be added to it.
                        // also the wrap has not consumed any bytes from the netInBuffer since it is still full.
                        int capacityOld = netInBuffer.capacity();
                        int packetBufferSize = sslEngine.getSession().getPacketBufferSize();

                        int capacityNew = capacityOld < packetBufferSize ? packetBufferSize : capacityOld * 2;
                        this.netInBuffer = expandBufferInReadMode(netInBuffer, capacityNew);
                    } //else {
                    // the buffer isn't full; additional bytes can be read from the socket so that the unwrap can complete.
                    // we don't need to increase the buffer in this case.
                    //}
                    return;
                case CLOSED:
                    return;
                default:
                    throw new IllegalStateException();
            }

            HandshakeStatus handshakeStatus = unwrapResult.getHandshakeStatus();
            if (handshakeStatus == NEED_TASK) {
                handleTasks();
            } else if (handshakeStatus == FINISHED || handshakeStatus == NEED_WRAP) {
                break;
            }
        }
    }

    @Override
    public void closeOutbound() throws IOException {
        super.closeOutbound();

        sslEngine.closeOutbound();
        try {
            writeToSocket(emptyBuffer);
        } catch (Exception ignored) {
            ignore(ignored);
        }
    }

    @Override
    public String toString() {
        return "SSLChannel{" + getLocalSocketAddress() + "->" + getRemoteSocketAddress() + '}';
    }

// Useful toString implemention for debugging the handshake logic. It automatically indents so you can easily see the
// server/client
//    @Override
//    public String toString() {
//        if (isClientMode()) {
//            return "SSLChannel{" + getLocalSocketAddress() + "->" + getRemoteSocketAddress() + '}';
//
//        } else {
//            return "       SSLChannel{" + getLocalSocketAddress() + "->" + getRemoteSocketAddress() + '}';
//        }
//    }
}
