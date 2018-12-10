package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.EOFException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.BLOCKED;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static com.hazelcast.nio.ssl.TLSHandshakeDecoder.newSSLException;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_OVERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static javax.net.ssl.SSLEngineResult.Status.OK;

/**
 * A {@link OutboundHandler} that takes care of outgoing TLS handshake
 * traffic.
 *
 * Once the handshake is complete, this handler will remove itself from the
 * pipeline.
 *
 * The TLSHandshakeDecoder will not consume anything from its source.
 *
 * @see TLSHandshakeDecoder
 */
public class TLSHandshakeEncoder extends OutboundHandler<Void, ByteBuffer> {

    private final SSLEngine sslEngine;
    private final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
    private final TLSExecutor tlsExecutor;

    TLSHandshakeEncoder(SSLEngine sslEngine,
                        TLSExecutor tlsExecutor) {
        this.sslEngine = sslEngine;
        this.tlsExecutor = tlsExecutor;
    }

    @Override
    public void handlerAdded() {
        // we need to make sure that the buffer for incoming data contains enough space.
        // getPacketBufferSize tells how much space is needed for the encrypted data (outgoing) data.
        initDstBuffer(sslEngine.getSession().getPacketBufferSize());
    }

    @Override
    public void interceptError(Throwable t) throws Throwable {
        if (t instanceof EOFException) {
            throw newSSLException(t);
        }
    }

    @Override
    public HandlerStatus onWrite() throws Exception {
        compactOrClear(dst);

        try {
            for (; ; ) {
                SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
                switch (handshakeStatus) {
                    case FINISHED:
                        break;
                    case NEED_TASK:
                        tlsExecutor.executeHandshakeTasks(sslEngine, channel);
                        return BLOCKED;
                    case NEED_WRAP:
                        SSLEngineResult wrapResult = sslEngine.wrap(emptyBuffer, dst);
                        SSLEngineResult.Status wrapResultStatus = wrapResult.getStatus();
                        if (wrapResultStatus == OK) {
                            // the wrap was a success, return to the loop to check the
                            // handshake status again.
                            continue;
                        } else if (wrapResultStatus == BUFFER_OVERFLOW) {
                            // not enough space available to write the content, so lets
                            // return DIRTY to indicate that we want to be notified if
                            // more space on the socket is available.
                            return DIRTY;
                        } else if (wrapResultStatus == CLOSED) {
                            // the wrap was not a success; the SSLEngine got closed.
                            // lets return because there is nothing that can be done,
                            // eventually the channel will get closed.
                            return CLEAN;
                        } else {
                            throw new IllegalStateException("Unexpected wrapResult:" + wrapResult);
                        }
                    case NEED_UNWRAP:
                        // the SSLEngine wants to read data; so lets trigger the inbound pipeline to start reading
                        channel.inboundPipeline().wakeup();
                        // and indicate we are blocked since the handshake isn't complete
                        return BLOCKED;
                    case NOT_HANDSHAKING:
                        if (!isTlsHandshakeBufferDrained()) {
                            // we need to wait till the buffer is drained before replacing the handlers
                            return DIRTY;
                        }

                        // The handshake is complete, therefor the TLSHandshakeEncoder is removed
                        channel.outboundPipeline().replace(this, new TLSEncoder(sslEngine));
                        // Inbound traffic was blocked; so the inbound pipeline is woken up.
                        // The TLSHandshakeDecoder will then remove itself from the pipeline
                        // and regular TLS traffic is started.
                        channel.inboundPipeline().wakeup();
                        return CLEAN;
                    default:
                        throw new IllegalStateException();
                }
            }
        } finally {
            dst.flip();
        }
    }

    private boolean isTlsHandshakeBufferDrained() {
        return dst.position() == 0;
    }
}
