package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.HandlerStatus;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static java.lang.Math.max;

/**
 * A {@link ChannelOutboundHandler} that takes care of TLS encryption.
 *
 * @see TLSDecoder
 * @see TLSHandshakeEncoder
 */
public class TLSEncoder extends ChannelOutboundHandler<ByteBuffer, ByteBuffer> {

    private final SSLEngine sslEngine;

    public TLSEncoder(SSLEngine sslEngine) {
        this.sslEngine = sslEngine;
    }

    @Override
    public void handlerAdded() {
        int sendBufferSize = channel.config().getOption(SO_SNDBUF);
        int packetBufferSize = sslEngine.getSession().getPacketBufferSize();
        // whatever is configured on the socket level, a minimal size of a dst buffer is needed
        // for encoding.
        // todo: perhaps it would be better to throw an error telling buffer sizes insufficient
        // instead of silently upgrading.
        initDstBuffer(max(sendBufferSize, packetBufferSize));
    }

    @Override
    public void requestClose() {
        sslEngine.closeOutbound();
    }

    /**
     * The logic to encode needs to be executed in a loop. wrap returns OK, even if not all src data is encrypted and even if
     * there is space in the dst buffer.
     */
    @Override
    public HandlerStatus onWrite() throws Exception {
        compactOrClear(dst);
        try {
            for (;;) {
                SSLEngineResult wrapResult = sslEngine.wrap(src, dst);
//                if (wrapResult.bytesProduced() > 32000 || wrapResult.bytesConsumed() > 32000) {
//                    System.out.println("TLSEncoder Bingo: " + wrapResult);
//                }

                switch (wrapResult.getStatus()) {
                    case BUFFER_OVERFLOW:
                        // not enough space in dst
                        return DIRTY;
                    case OK:
                        if (src.remaining() > 0) {
                            // do another wrap since there is more data to encrypt.
                            return DIRTY;
                        }
                        // everything got written
                        return CLEAN;
                    case CLOSED:
                        return CLEAN;
                    default:
                        throw new IllegalStateException("Unexpected " + wrapResult);
                }
            }
        } finally {
            dst.flip();
        }
    }
}
