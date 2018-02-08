package com.hazelcast.nio.tcp;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketIOHelper;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.CipherHelper.createSymmetricWriterCipher;

public class SymmetricCipherPacketEncoder implements ChannelOutboundHandler<Packet> {

    private final Cipher cipher;
    private final PacketIOHelper packetWriter = new PacketIOHelper();
    private ByteBuffer packetBuffer;
    private boolean packetWritten;

    public SymmetricCipherPacketEncoder(TcpIpConnection connection, IOService ioService) {
        this.packetBuffer = ByteBuffer.allocate(ioService.getSocketSendBufferSize() * IOService.KILO_BYTE);
        this.cipher = createSymmetricWriterCipher(ioService.getSymmetricEncryptionConfig(), connection);
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public boolean onWrite(Packet packet, ByteBuffer dst) throws Exception {
        if (!packetWritten) {
            if (dst.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }
            int size = cipher.getOutputSize(packet.packetSize());
            dst.putInt(size);

            if (packetBuffer.capacity() < packet.packetSize()) {
                packetBuffer = ByteBuffer.allocate(packet.packetSize());
            }
            if (!packetWriter.writeTo(packet, packetBuffer)) {
                throw new HazelcastException("Packet didn't fit into the buffer! " + packet.packetSize()
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
                    packetBuffer.clear();
                    return true;
                }
            }
        }
        return false;
    }
}
