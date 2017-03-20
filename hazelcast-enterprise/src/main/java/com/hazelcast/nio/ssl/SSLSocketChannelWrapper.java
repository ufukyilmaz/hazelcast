package com.hazelcast.nio.ssl;

import com.hazelcast.nio.tcp.DefaultSocketChannelWrapper;
import com.hazelcast.util.EmptyStatement;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
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

    public SSLSocketChannelWrapper(SSLContext sslContext, SocketChannel sc, boolean client,
                                   String mutualAuthentication) throws Exception {
        super(sc);
        sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(client);
        sslEngine.setEnableSessionCreation(true);
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
    }

    @SuppressWarnings({
            "checkstyle:cyclomaticcomplexity",
            "checkstyle:npathcomplexity",
            "checkstyle:methodlength",
            "checkstyle:magicnumber"
    })
    private void handshake() throws IOException {
        if (handshakeCompleted) {
            return;
        }
        synchronized (lock) {
            if (handshakeCompleted) {
                return;
            }
            int counter = 0;
            sslEngine.beginHandshake();
            writeInternal(emptyBuffer);
            while (counter++ < 250 && sslEngineResult.getHandshakeStatus() != FINISHED) {
                if (sslEngineResult.getHandshakeStatus() == NEED_UNWRAP) {
                    netInBuffer.clear();
                    while (socketChannel.read(netInBuffer) == 0) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            throw new IOException(e);
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
            applicationBuffer.clear();
            applicationBuffer.flip();
            handshakeCompleted = true;
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    private ByteBuffer unwrap(ByteBuffer b) throws SSLException {
        applicationBuffer.clear();

        while (b.hasRemaining()) {
            synchronized (lock) {
                sslEngineResult = sslEngine.unwrap(b, applicationBuffer);
            }

            if (sslEngineResult.getStatus() == BUFFER_OVERFLOW) {
                // the appBuffer wasn't big enough, so lets expand it.
                applicationBuffer = expandBuffer(applicationBuffer);
            }

            if (sslEngineResult.getHandshakeStatus() == NEED_TASK) {
                handleTasks();
            } else if (sslEngineResult.getHandshakeStatus() == FINISHED || sslEngineResult.getStatus() == BUFFER_UNDERFLOW) {
                return applicationBuffer;
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
        synchronized (lock) {
            sslEngineResult = sslEngine.wrap(input, netOutBuffer);
        }
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
        int limit;
        if (applicationBuffer.hasRemaining()) {
            limit = Math.min(applicationBuffer.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(applicationBuffer.get());
                readBytesCount++;
            }
            return readBytesCount;
        }
        if (netInBuffer.hasRemaining()) {
            unwrap(netInBuffer);
            applicationBuffer.flip();
            limit = Math.min(applicationBuffer.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(applicationBuffer.get());
                readBytesCount++;
            }
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
        limit = Math.min(applicationBuffer.remaining(), output.remaining());
        for (int i = 0; i < limit; i++) {
            output.put(applicationBuffer.get());
            readBytesCount++;
        }
        return readBytesCount;
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
}
