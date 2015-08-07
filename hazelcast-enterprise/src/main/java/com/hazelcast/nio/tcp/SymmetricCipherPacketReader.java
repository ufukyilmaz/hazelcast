package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.CipherHelper;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packettransceiver.PacketTransceiver;
import com.hazelcast.util.ExceptionUtil;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;

public class SymmetricCipherPacketReader extends DefaultPacketReader {

    private final Cipher cipher;
    private final ILogger logger;
    private final IOService ioService;
    private int size = -1;
    private ByteBuffer cipherBuffer;

    public SymmetricCipherPacketReader(TcpIpConnection connection, IOService ioService, PacketTransceiver packetTransceiver) {
        super(connection, packetTransceiver);
        this.ioService = ioService;
        this.cipherBuffer = ByteBuffer.allocate(ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE);
        this.logger = ioService.getLogger(getClass().getName());
        this.cipher = init();
    }

    Cipher init() {
        try {
            return CipherHelper.createSymmetricReaderCipher(ioService.getSymmetricEncryptionConfig());
        } catch (Exception e) {
            logger.severe("Symmetric Cipher for ReadHandler cannot be initialized.", e);
            CipherHelper.handleCipherException(e, connection);
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public void readPacket(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            try {
                if (size == -1) {
                    if (inBuffer.remaining() < Bits.INT_SIZE_IN_BYTES) {
                        return;
                    }
                    size = inBuffer.getInt();
                    if (cipherBuffer.capacity() < size) {
                        cipherBuffer = ByteBuffer.allocate(size);
                    }
                }
                int remaining = inBuffer.remaining();
                if (remaining < size) {
                    cipher.update(inBuffer, cipherBuffer);
                    size -= remaining;
                } else if (remaining == size) {
                    cipher.doFinal(inBuffer, cipherBuffer);
                    size = -1;
                } else {
                    int oldLimit = inBuffer.limit();
                    int newLimit = inBuffer.position() + size;
                    inBuffer.limit(newLimit);
                    cipher.doFinal(inBuffer, cipherBuffer);
                    inBuffer.limit(oldLimit);
                    size = -1;
                }
            } catch (ShortBufferException e) {
                logger.warning(e);
            }
            cipherBuffer.flip();
            while (cipherBuffer.hasRemaining()) {
                if (packet == null) {
                    packet = new Packet();
                }
                boolean complete = packet.readFrom(cipherBuffer);
                if (complete) {
                    handlePacket(packet);
                    packet = null;
                } else {
                    break;
                }
            }
            if (cipherBuffer.hasRemaining()) {
                cipherBuffer.compact();
            } else {
                cipherBuffer.clear();
            }
        }
    }
}
