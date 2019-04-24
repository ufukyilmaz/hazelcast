package com.hazelcast.nio.tcp;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketIOHelper;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.CipherHelper.createSymmetricWriterCipher;
import static com.hazelcast.nio.IOUtil.compactOrClear;

/**
 * En encoder that encoded packets using a symmetric cipher.
 */
@SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
public class SymmetricCipherPacketEncoder extends OutboundHandler<Supplier<Packet>, ByteBuffer> {

    private final Cipher cipher;
    private final PacketIOHelper packetWriter = new PacketIOHelper();
    private Packet packet;
    private ByteBuffer packetBuffer;
    private boolean packetWritten;

    public SymmetricCipherPacketEncoder(TcpIpConnection connection, SymmetricEncryptionConfig config) {
        this.cipher = createSymmetricWriterCipher(config, connection);
    }

    @Override
    public void handlerAdded() {
        initDstBuffer();
        this.packetBuffer = ByteBuffer.allocate(channel.options().getOption(SO_SNDBUF));
    }

    @Override
    public HandlerStatus onWrite() throws Exception {
        // the buffer is in reading mode

        compactOrClear(dst);
        try {
            for (; ; ) {
                if (packet == null) {
                    packet = src.get();

                    if (packet == null) {
                        // everything is processed, so we are done
                        return CLEAN;
                    }
                }

                if (!packetWritten) {
                    if (dst.remaining() < INT_SIZE_IN_BYTES) {
                        return DIRTY;
                    }
                    int size = cipher.getOutputSize(packet.getFrameLength());
                    dst.putInt(size);

                    if (packetBuffer.capacity() < packet.getFrameLength()) {
                        packetBuffer = ByteBuffer.allocate(packet.getFrameLength());
                    }
                    if (!packetWriter.writeTo(packet, packetBuffer)) {
                        throw new HazelcastException("Packet didn't fit into the buffer! " + packet.getFrameLength()
                                + " VS " + packetBuffer);
                    }
                    packetBuffer.flip();
                    packetWritten = true;
                }

                if (dst.hasRemaining()) {
                    int outputSize = cipher.getOutputSize(packetBuffer.remaining());
                    if (outputSize <= dst.remaining()) {
                        cipher.update(packetBuffer, dst);
                    } else {
                        int len = packetBuffer.remaining() / 2;
                        while (len > 0 && cipher.getOutputSize(len) > dst.remaining()) {
                            len = len / 2;
                        }
                        if (len > 0) {
                            int limitOld = packetBuffer.limit();
                            packetBuffer.limit(packetBuffer.position() + len);
                            cipher.update(packetBuffer, dst);
                            packetBuffer.limit(limitOld);
                        }
                    }

                    if (!packetBuffer.hasRemaining()) {
                        if (dst.remaining() >= cipher.getOutputSize(0)) {
                            dst.put(cipher.doFinal());
                            packetWritten = false;
                            packet = null;
                            packetBuffer.clear();
                        }
                    }
                }
            }
        } finally {
            dst.flip();
        }
    }
}
