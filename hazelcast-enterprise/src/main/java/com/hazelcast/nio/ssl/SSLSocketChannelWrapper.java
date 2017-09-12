package com.hazelcast.nio.ssl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.tcp.DefaultSocketChannelWrapper;
import com.hazelcast.util.EmptyStatement;

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

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;

public class SSLSocketChannelWrapper extends DefaultSocketChannelWrapper {

    static final int EXPAND_FACTOR = 2;
    private final ILogger logger = Logger.getLogger(SSLSocketChannelWrapper.class);

    private ByteBuffer applicationBuffer;
    private final Object lock = new Object();
    private final ByteBuffer emptyBuffer;
    private final ByteBuffer netOutBuffer;
    // "reliable" write transport
    private final ByteBuffer netInBuffer;
    // "reliable" read transport
    private final SSLEngine sslEngine;
    private volatile boolean handshakeCompleted;
    private SSLEngineResult sslEngineResult;

    public SSLSocketChannelWrapper(SSLEngine sslEngine, SocketChannel sc, boolean clientMode,
                                   String mutualAuthentication) throws Exception {
        super(sc);
        this.sslEngine = sslEngine;

        // In case of the OpenSSL based SSLEngine implementation, the below calls are ignored.
        // For configuration see the OpenSSLEngineFactory
        if ("REQUIRED".equals(mutualAuthentication)) {
            sslEngine.setNeedClientAuth(true);
        } else if ("OPTIONAL".equals(mutualAuthentication)) {
            sslEngine.setWantClientAuth(true);
        }
        SSLSession session = sslEngine.getSession();
        applicationBuffer = ByteBuffer.allocate(session.getApplicationBufferSize());
        emptyBuffer = ByteBuffer.allocate(0);
        int netBufferMax = session.getPacketBufferSize();
        netOutBuffer = ByteBuffer.allocate(netBufferMax);
        netInBuffer = ByteBuffer.allocate(netBufferMax);
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
            writeInternal(emptyBuffer);
            while (counter++ < 250 && sslEngineResult.getHandshakeStatus() != FINISHED) {
                if (sslEngineResult.getHandshakeStatus() == NEED_UNWRAP) {
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
                    unwrap(netInBuffer);
                    if (sslEngineResult.getHandshakeStatus() != FINISHED) {
                        emptyBuffer.clear();
                        writeInternal(emptyBuffer);
                    }
                } else if (sslEngineResult.getHandshakeStatus() == NEED_WRAP) {
                    emptyBuffer.clear();
                    writeInternal(emptyBuffer);
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
            }
            if (sslEngineResult.getHandshakeStatus() != FINISHED) {
                throw new SSLHandshakeException("SSL handshake failed after " + counter
                        + " trials! -> " + sslEngineResult.getHandshakeStatus());
            }

            if (logger.isFineEnabled()) {
                SSLSession sslSession = sslEngine.getSession();
                logger.fine("handshake finished, channel=" + this + " protocol=" + sslSession.getProtocol() + ", cipherSuite="
                        + sslSession.getCipherSuite());
            }

            applicationBuffer.clear();
            applicationBuffer.flip();
            handshakeCompleted = true;
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private ByteBuffer unwrap(ByteBuffer b) throws SSLException {
        applicationBuffer.clear();

        while (b.hasRemaining()) {
            sslEngineResult = sslEngine.unwrap(b, applicationBuffer);

            HandshakeStatus handshakeStatus = sslEngineResult.getHandshakeStatus();
            Status status = sslEngineResult.getStatus();
            if (status == BUFFER_OVERFLOW) {
                // the appBuffer wasn't big enough, so lets expand it.
                applicationBuffer = expandBuffer(applicationBuffer);
            }

            if (handshakeStatus == NEED_TASK) {
                handleTasks();
            } else if (handshakeStatus == FINISHED
                    || handshakeStatus == NEED_WRAP
                    || status == BUFFER_UNDERFLOW) {
                break;
            }
        }
        return applicationBuffer;
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

        ByteBuffer expanded = ByteBuffer.allocate(newCapacity);

        original.flip();

        expanded.put(original);

        return expanded;
    }

    @Override
    public int write(ByteBuffer input) throws IOException {
        if (!handshakeCompleted) {
            handshake();
        }
        return writeInternal(input);
    }

    private int writeInternal(ByteBuffer input) throws IOException {
        sslEngineResult = sslEngine.wrap(input, netOutBuffer);
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
    public int read(ByteBuffer output) throws IOException {
        if (!handshakeCompleted) {
            handshake();
        }

        int readBytesCount = 0;
        if (applicationBuffer.hasRemaining()) {
            readBytesCount += readFromApplicationBuffer(output);
            return readBytesCount;
        }

        if (netInBuffer.hasRemaining()) {
            unwrap(netInBuffer);
            applicationBuffer.flip();
            readBytesCount += readFromApplicationBuffer(output);
        }

        if (applicationBuffer.hasRemaining()) {
            // there is still something left in the applicationBuffer
            // -> the only explanation is the output buffer is full
            assert !output.hasRemaining();

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
        unwrap(netInBuffer);
        applicationBuffer.flip();
        readBytesCount += readFromApplicationBuffer(output);
        return readBytesCount;
    }

    private int readFromApplicationBuffer(ByteBuffer output) {
        int appBufferRemaining = applicationBuffer.remaining();
        int outputRemaining = output.remaining();

        if (outputRemaining < appBufferRemaining) {
            // there is not enough space to all data, so lets copy what we can copy.
            for (int i = 0; i < outputRemaining; i++) {
                output.put(applicationBuffer.get());
            }
            return outputRemaining;
        } else {
            // there is enough space to copy all data
            output.put(applicationBuffer);
            return appBufferRemaining;
        }
    }

    @Override
    public void closeOutbound() throws IOException {
        super.closeOutbound();

        sslEngine.closeOutbound();
        try {
            writeInternal(emptyBuffer);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    @Override
    public String toString() {
        return "SSLSocketChannelWrapper{" + "socketChannel=" + socketChannel + '}';
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
