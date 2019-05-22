package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.networking.HandlerStatus.BLOCKED;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.IOUtil.toDebugString;
import static com.hazelcast.nio.ssl.TLSUtil.publishRemoteCertificates;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static javax.net.ssl.SSLEngineResult.Status.BUFFER_UNDERFLOW;
import static javax.net.ssl.SSLEngineResult.Status.CLOSED;
import static javax.net.ssl.SSLEngineResult.Status.OK;

/**
 * A {@link InboundHandler} that takes care of incoming TLS handshake
 * traffic.
 *
 * Once the handshake is complete, this handler will remove itself from the
 * pipeline and the TLSDecoder takes over. The {@link TLSHandshakeDecoder}
 * will not push any data to its dst. Once the handshake is complete, the
 * TLSDecoder will start pushing data.
 *
 * @see TLSHandshakeEncoder
 */
public class TLSHandshakeDecoder extends InboundHandler<ByteBuffer, Void> {

    private final SSLEngine sslEngine;
    private final TLSExecutor tlsExecutor;
    private final ByteBuffer appBuffer;
    private final ConcurrentMap attributeMap;

    public TLSHandshakeDecoder(SSLEngine sslEngine,
                               TLSExecutor tlsExecutor, ConcurrentMap attributeMap) {
        this.sslEngine = sslEngine;
        this.tlsExecutor = tlsExecutor;
        // direct buffer isn't needed since it is just a handshake.
        this.appBuffer = newByteBuffer(sslEngine.getSession().getApplicationBufferSize(), false);
        this.attributeMap = attributeMap;
    }

    @Override
    public void handlerAdded() {
        // we need to make sure that the buffer for incoming data contains enough space.
        // getPacketBufferSize tells how much space is needed for the encrypted data (incoming) data.
        initSrcBuffer(sslEngine.getSession().getPacketBufferSize());
    }

    @Override
    public void interceptError(Throwable t) throws Throwable {
        if (t instanceof EOFException) {
            throw newSSLException(t);
        }
    }

    static SSLException newSSLException(Throwable t) {
        return new SSLException("Remote socket closed during SSL/TLS handshake. "
                + " This is probably caused by a SSL/TLS authentication problem"
                + " resulting in the remote side closing the socket.", t);
    }

    @Override
    public HandlerStatus onRead() throws Exception {
        src.flip();
        try {
            for (; ; ) {
                SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
                switch (handshakeStatus) {
                    case FINISHED:
                        // the handshake is finished; lets retry the loop again and
                        // we end up at NOT_HANDSHAKING
                        break;
                    case NEED_TASK:
                        tlsExecutor.executeHandshakeTasks(sslEngine, channel);
                        return BLOCKED;
                    case NEED_WRAP:
                        // the SSLEngine has something to write, so lets wakeup the outbound pipeline.
                        channel.outboundPipeline().wakeup();
                        return BLOCKED;
                    case NEED_UNWRAP:
                        SSLEngineResult unwrapResult = sslEngine.unwrap(src, appBuffer);
                        SSLEngineResult.Status unwrapStatus = unwrapResult.getStatus();
                        if (unwrapStatus == OK) {
                            // unwrapping was a success.
                            // go back to the for loop and check handshake status again
                            continue;
                        } else if (unwrapStatus == CLOSED) {
                            // the SSLEngine is closed. So lets return, since nothing
                            // can be done here. Eventually the channel will get closed
                            return CLEAN;
                        } else if (unwrapStatus == BUFFER_UNDERFLOW) {
                            if (sslEngine.getHandshakeStatus() == NOT_HANDSHAKING) {
                                // needed because of OpenSSL. OpenSSL can indicate buffer underflow,
                                // but handshake can complete at the same time. Check causes the loop
                                // to be retried.
                                continue;
                            }

                            // not enough data is available to decode the content, so lets return
                            // and wait for more data to be received.
                            return CLEAN;
                        } else {
                            throw new IllegalStateException("Unexpected " + unwrapResult);
                        }
                    case NOT_HANDSHAKING:
                        if (appBuffer.position() != 0) {
                            throw new IllegalStateException("Unexpected data in the appBuffer, it should be empty "
                                    + toDebugString("appBuffer", appBuffer));
                        }
                        publishRemoteCertificates(sslEngine, attributeMap);
                        TLSDecoder tlsDecoder = new TLSDecoder(sslEngine);
                        channel.inboundPipeline().replace(this, tlsDecoder);
                        // the src buffer could contain unencrypted data not needed for the handshake
                        // this data needs to be pushed to the TLSDecoder.
                        tlsDecoder.src().put(src);
                        // wakeup the outbound pipeline to complete the handshake.
                        channel.outboundPipeline().wakeup();
                        return DIRTY;
                    default:
                        throw new IllegalStateException();
                }
            }
        } finally {
            compactOrClear(src);
        }
    }
}
