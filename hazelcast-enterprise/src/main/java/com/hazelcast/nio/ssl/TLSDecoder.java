package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.IOUtil.toDebugString;
import static java.lang.Math.max;

/**
 * A {@link com.hazelcast.internal.networking.OutboundHandler} responsible
 * for decoding TLS traffic.
 *
 * This TLSDecoder will decode to the application buffer and then it will get copied
 * to the dst buffer. Although this requires a useless copy, directly decoding to the
 * dst buffer has some serious implications. There is a very significant performance
 * drop for the SSLEngine if this is done.

 * Normally the dst ByteBuffer will be the buffer used for decoding, so no
 * intermediate copy of the data is needed as was the case in HZ 3.10 and older.
 *
 * But if the dst ByteBuffer is too small to decode, an intermediate 'appBuffer'
 * will be used for decoding purposes. So src->appBuffer->dst. This is a very
 * concrete problem with the ProtocolDecoder that only has a buffer of 3 bytes,
 * and will not be accepted by the SSLEngine for unwrapping even though there
 * might only 3 bytes to decode.
 *
 * The TLS decoder should have preferable a large src buffer; if this buffer is
 * used to read data from the socket, it should read as much data as possible without
 * needing to go to the socket again.
 *
 * However the dst buffer should not be a lot bigger than the packetBuffer size; because
 * we want to push the decoded bytes through the pipeline as fast as possible.
 *
 * Ideally the TLSDecoder would not be creating the src buffer; but the destination
 * buffer.
 */
public class TLSDecoder extends InboundHandler<ByteBuffer, ByteBuffer> {

    private final SSLEngine sslEngine;
    private final SSLSession sslSession;
    private ByteBuffer appBuffer;

    public TLSDecoder(SSLEngine sslEngine) {
        this.sslEngine = sslEngine;
        this.sslSession = sslEngine.getSession();
    }

    @Override
    public void handlerAdded() {
        int socketReceiveBuffer = channel.options().getOption(SO_RCVBUF);
        int packetBufferSize = sslEngine.getSession().getPacketBufferSize();
        initSrcBuffer(max(socketReceiveBuffer, packetBufferSize));

        this.appBuffer = newByteBuffer(sslSession.getApplicationBufferSize(), channel.options().getOption(DIRECT_BUF));
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public HandlerStatus onRead() throws Exception {
        if (!drainAppBuffer()) {
            return DIRTY;
        }

        src.flip();
        try {
            for (;;) {
                SSLEngineResult unwrapResult;
                try {
                    unwrapResult = sslEngine.unwrap(src, appBuffer);
                } catch (SSLException e) {
                    throw new SSLException(
                            toDebugString("src", src) + " " + toDebugString("app", appBuffer) + " " + toDebugString("dst", dst),
                            e);
                }
                switch (unwrapResult.getStatus()) {
                    case BUFFER_OVERFLOW:
                        // The SSLEngine was not able to process the operation because there are not enough bytes available
                        // in the destination buffer to hold the result.
                        if (appBuffer.capacity() >= sslSession.getApplicationBufferSize()) {
                            // the appBuffer has enough capacity, so lets return DIRTY so that trailing handlers can drain
                            // the dst and then this handler is tried again.
                            return DIRTY;
                        }
                        // the dst has insufficient capacity
                        appBuffer = newAppBuffer();
                        continue;
                    case BUFFER_UNDERFLOW:
                        // The SSLEngine was not able to unwrap the incoming data
                        // because there were not enough source bytes available to
                        // complete an unwrap.
                        return CLEAN;
                    case OK:
                        // unwrapping was a success.

                        if (!drainAppBuffer()) {
                            // not all data got moved to the dst buffer; so
                            // return DIRTY to try again
                            return DIRTY;
                        }

                        if (src.remaining() == 0) {
                            // no data is remaining to be decoded, therefor this handler is done.
                            return CLEAN;
                        }

                        // data is remaining to be decoded, lets try the unwrap again.
                        // escape early; sslengine likes this
                        // return DIRTY;

                        // breaking is what openssl likes
                        break;
                    case CLOSED:
                         return CLEAN;
                    default:
                        throw new IllegalStateException();
                }
            }
        } finally {
            compactOrClear(src);
        }
    }

    private ByteBuffer newAppBuffer() {
        return newByteBuffer(sslSession.getApplicationBufferSize(), channel.options().getOption(DIRECT_BUF));
    }

    private boolean drainAppBuffer() {
        // it is expected to be in writing mode, so we set it to reading mode since we are going to drain it.
        appBuffer.flip();

        int available = appBuffer.remaining();

        if (dst.remaining() < available) {
            int oldLimit = appBuffer.limit();
            appBuffer.limit(appBuffer.position() + dst.remaining());
            dst.put(appBuffer);
            appBuffer.limit(oldLimit);
        } else {
            dst.put(appBuffer);
        }

        if (appBuffer.hasRemaining()) {
            // if not drained, then we need to return false.
            // the appBuffer will remain in reading mode
            compactOrClear(appBuffer);
            return false;
        } else {
            // all bytes have been written
            appBuffer.clear();
        }
        return true;
    }
}
