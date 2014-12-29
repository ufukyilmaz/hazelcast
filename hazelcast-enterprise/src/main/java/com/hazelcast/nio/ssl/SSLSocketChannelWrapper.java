package com.hazelcast.nio.ssl;

import com.hazelcast.nio.tcp.DefaultSocketChannelWrapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

public class SSLSocketChannelWrapper extends DefaultSocketChannelWrapper {

    private static final boolean DEBUG = false;

    private final ByteBuffer applicationBuffer;
    private final Object lock = new Object();
    private final ByteBuffer emptyBuffer;
    private final ByteBuffer netOutBuffer;
    // "reliable" write transport
    private final ByteBuffer netInBuffer;
    // "reliable" read transport
    private final SSLEngine sslEngine;
    private volatile boolean handshakeCompleted;
    private SSLEngineResult sslEngineResult;

    public SSLSocketChannelWrapper(SSLContext sslContext, SocketChannel sc, boolean client) throws Exception {
        super(sc);
        sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(client);
        sslEngine.setEnableSessionCreation(true);
        SSLSession session = sslEngine.getSession();
        applicationBuffer = ByteBuffer.allocate(session.getApplicationBufferSize());
        emptyBuffer = ByteBuffer.allocate(0);
        int netBufferMax = session.getPacketBufferSize();
        netOutBuffer = ByteBuffer.allocate(netBufferMax);
        netInBuffer = ByteBuffer.allocate(netBufferMax);
    }

    /**
     * TODO sleep in sync block
     */
    private void handshake() throws IOException {
        if (handshakeCompleted) {
            return;
        }
        log("Starting handshake...");
        synchronized (lock) {
            if (handshakeCompleted) {
                log("Handshake already completed...");
                return;
            }
            int counter = 0;
            log("Begin handshake");
            sslEngine.beginHandshake();
            writeInternal(emptyBuffer);
            while (counter++ < 250 && sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                log("Handshake status: " + sslEngineResult.getHandshakeStatus());
                if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    log("Begin UNWRAP");
                    netInBuffer.clear();
                    while (socketChannel.read(netInBuffer) < 1) {
                        try {
                            log("Spinning on channel read...");
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                    }
                    netInBuffer.flip();
                    unwrap(netInBuffer);
                    log("Done UNWRAP: " + sslEngineResult.getHandshakeStatus());
                    if (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                        emptyBuffer.clear();
                        writeInternal(emptyBuffer);
                        log("Done WRAP after UNWRAP: " + sslEngineResult.getHandshakeStatus());
                    }
                } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                    log("Begin WRAP");
                    emptyBuffer.clear();
                    writeInternal(emptyBuffer);
                    log("Done WRAP: " + sslEngineResult.getHandshakeStatus());
                } else {
                    try {
                        log("Sleeping... Status: " + sslEngineResult.getHandshakeStatus());
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
            }
            if (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                throw new SSLHandshakeException("SSL handshake failed after " + counter
                        + " trials! -> " + sslEngineResult.getHandshakeStatus());
            }
            log("Handshake completed!");
            applicationBuffer.clear();
            applicationBuffer.flip();
            handshakeCompleted = true;
        }
    }

    private void log(String log) {
        if (DEBUG) {
            System.err.println(getClass().getSimpleName() + "[" + socketChannel.socket().getLocalSocketAddress() + "]: " + log);
        }
    }

    private ByteBuffer unwrap(ByteBuffer b) throws SSLException {
        log(" ----------- unwrap enter ---------------- ");
        applicationBuffer.clear();
        log("==============");
        log("net buffer = " + netInBuffer);
        log("application buffer = " + applicationBuffer);
        log("==============");

        while (b.hasRemaining()) {
            synchronized (lock) {
                sslEngineResult = sslEngine.unwrap(b, applicationBuffer);
            }
            log("==============");
            log("SSL Engine Status : " + sslEngineResult.getStatus());
            log("net buffer  = " + netInBuffer);
            log("application buffer = " + applicationBuffer);
            log("==============");
            if (sslEngineResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                log(" ----------- unwrap exit ----------------");
                return applicationBuffer;
            }
            if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                log("Handshake NEED TASK");
                Runnable task;
                while ((task = sslEngine.getDelegatedTask()) != null) {
                    log("Running task: " + task);
                    task.run();
                }
            } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
                    || sslEngineResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                log(" ----------- unwrap exit ----------------");
                return applicationBuffer;
            }
        }
        log(" ----------- unwrap exit ---------------- ");
        return applicationBuffer;
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

    public int read(ByteBuffer output) throws IOException {
        log("######  read enter #########");
        log("net buffer = " + netInBuffer);
        log("application buffer = " + applicationBuffer);
        if (!handshakeCompleted) {
            handshake();
        }
        int readBytesCount = 0;
        int limit;
        if (applicationBuffer.hasRemaining()) {
            log("Read data from applicationBuffer");
            limit = Math.min(applicationBuffer.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(applicationBuffer.get());
                readBytesCount++;
            }
            log("######  read exit #########");
            return readBytesCount;
        }
        if (netInBuffer.hasRemaining()) {
            log("There are some data in the netInBuffer, try to unwrap more data ");
            unwrap(netInBuffer);
            applicationBuffer.flip();
            limit = Math.min(applicationBuffer.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(applicationBuffer.get());
                readBytesCount++;
            }
        }
        if (netInBuffer.hasRemaining()) {
            log("There is some data in the netInBuffer, compacting the buffer");
            netInBuffer.compact();
        } else {
            log("There is no data in the netInBuffer, so clear it for the next read");
            netInBuffer.clear();
        }
        if (socketChannel.read(netInBuffer) == -1) {
            netInBuffer.clear();
            netInBuffer.flip();
            log("######  read exit #########");
            return -1;
        }

        log("Try to unwrap and read again. ");
        netInBuffer.flip();
        unwrap(netInBuffer);
        applicationBuffer.flip();
        limit = Math.min(applicationBuffer.remaining(), output.remaining());
        for (int i = 0; i < limit; i++) {
            output.put(applicationBuffer.get());
            readBytesCount++;
        }
        log("######  read exit #########");
        return readBytesCount;
    }

    @Override
    public void close() throws IOException {
        sslEngine.closeOutbound();
        try {
            writeInternal(emptyBuffer);
        } catch (Exception ignored) {
        }
        socketChannel.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SSLSocketChannelWrapper{");
        sb.append("socketChannel=").append(socketChannel);
        sb.append('}');
        return sb.toString();
    }
}
