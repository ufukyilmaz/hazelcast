package com.hazelcast.nio.tcp;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.CipherHelper;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.util.ExceptionUtil;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;

public class SymmetricCipherPacketWriter implements PacketWriter {

    private final IOService ioService;
    private final TcpIpConnection connection;
    private final Cipher cipher;
    private final ILogger logger;
    private ByteBuffer packetBuffer;
//    private boolean sizeWritten;
    private boolean packetWritten;

    public SymmetricCipherPacketWriter(TcpIpConnection connection, IOService ioService) {
        this.connection = connection;
        this.ioService = ioService;
        logger = ioService.getLogger(getClass().getName());
        packetBuffer = ByteBuffer.allocate(ioService.getSocketSendBufferSize() * IOService.KILO_BYTE);
        cipher = init();
    }

    private Cipher init() {
        try {
            return CipherHelper.createSymmetricWriterCipher(ioService.getSymmetricEncryptionConfig());
        } catch (Exception e) {
            logger.severe("Symmetric Cipher for WriteHandler cannot be initialized.", e);
            CipherHelper.handleCipherException(e, connection);
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean write(Packet packet, ByteBuffer dst) throws Exception {
        if (!packetWritten) {
            if (dst.remaining() < Bits.INT_SIZE_IN_BYTES) {
                return false;
            }
            int size = cipher.getOutputSize(packet.packetSize());
            dst.putInt(size);

            if (packetBuffer.capacity() < packet.packetSize()) {
                packetBuffer = ByteBuffer.allocate(packet.packetSize());
            }
            if (!packet.writeTo(packetBuffer)) {
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
