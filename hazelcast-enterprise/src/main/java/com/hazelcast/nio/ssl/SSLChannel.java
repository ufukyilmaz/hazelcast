package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
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
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;

public class SSLChannel extends NioChannel {

    static final int EXPAND_FACTOR = 2;

    private final ILogger logger = Logger.getLogger(SSLChannel.class);

    private ByteBuffer appBuffer;
    private final Object lock = new Object();
    private final ByteBuffer emptyBuffer;
    private final ByteBuffer netOutBuffer;
    private final ByteBuffer netInBuffer;
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
            write0(emptyBuffer);
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
                        write0(emptyBuffer);
                    }
                } else if (handshakeResult.getHandshakeStatus() == NEED_WRAP) {
                    emptyBuffer.clear();
                    write0(emptyBuffer);
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
    static ByteBuffer expandBuffer(ByteBuffer original) {
        int oldCapacity = original.capacity();

        int newCapacity = oldCapacity * EXPAND_FACTOR;

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
        return write0(src);
    }

    private int write0(ByteBuffer src) throws IOException {
        wrap(src);

        netOutBuffer.flip();

        int written = socketChannel.write(netOutBuffer);

        if (netOutBuffer.hasRemaining()) {
            netOutBuffer.compact();
        } else {
            netOutBuffer.clear();
        }
        return written;
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

        int readBytesCount = 0;
        if (appBuffer.hasRemaining()) {
            readBytesCount += readFromAppBuffer(dst);
            return readBytesCount;
        }

        if (netInBuffer.hasRemaining()) {
            unwrap();
            appBuffer.flip();
            readBytesCount += readFromAppBuffer(dst);
        }

        if (appBuffer.hasRemaining()) {
            // there is still something left in the appBuffer
            // -> the only explanation is the output buffer is full
            assert !dst.hasRemaining();

            // we cannot do anything sensible when the output buffer is full.
            return readBytesCount;
        }

        if (netInBuffer.hasRemaining()) {
            netInBuffer.compact();
        } else {
            netInBuffer.clear();
        }

        if (socketChannel.read(netInBuffer) == -1) {
            netInBuffer.clear();
            netInBuffer.flip();
            return -1;
        }

        netInBuffer.flip();
        unwrap();
        appBuffer.flip();
        readBytesCount += readFromAppBuffer(dst);
        return readBytesCount;
    }

    private int readFromAppBuffer(ByteBuffer dst) {

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
        SSLEngineResult wrapStatus = sslEngine.wrap(src, netOutBuffer);
        this.handshakeResult = wrapStatus;
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private void unwrap() throws SSLException {
        appBuffer.clear();

        SSLEngineResult unwrapStatus;
        while (netInBuffer.hasRemaining()) {
            unwrapStatus = sslEngine.unwrap(netInBuffer, appBuffer);
            this.handshakeResult = unwrapStatus;

            HandshakeStatus handshakeStatus = unwrapStatus.getHandshakeStatus();
            Status status = unwrapStatus.getStatus();
            if (status == BUFFER_OVERFLOW) {
                // the appBuffer wasn't big enough, so lets expand it.
                appBuffer = expandBuffer(appBuffer);
            }

            if (handshakeStatus == NEED_TASK) {
                handleTasks();
            } else if (handshakeStatus == FINISHED
                    || handshakeStatus == NEED_WRAP
                    || status == BUFFER_UNDERFLOW) {
                break;
            }
        }
    }

    @Override
    public void closeOutbound() throws IOException {
        super.closeOutbound();

        sslEngine.closeOutbound();
        try {
            write0(emptyBuffer);
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
