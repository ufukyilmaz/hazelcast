package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.CipherHelper.createSymmetricReaderCipher;
import static com.hazelcast.nio.IOService.KILO_BYTE;

public class SymmetricCipherMemberChannelInboundHandler extends MemberChannelInboundHandler {

    private final ILogger logger;
    private final Cipher cipher;

    private ByteBuffer cipherBuffer;
    private int size = -1;

    public SymmetricCipherMemberChannelInboundHandler(TcpIpConnection connection, IOService ioService,
                                                      PacketHandler packetHandler) {
        super(connection, packetHandler);
        this.logger = ioService.getLoggingService().getLogger(getClass());
        this.cipher = createSymmetricReaderCipher(ioService.getSymmetricEncryptionConfig(), connection);
        this.cipherBuffer = ByteBuffer.allocate(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public void onRead(ByteBuffer src) throws Exception {
        while (src.hasRemaining()) {
            try {
                if (size == -1) {
                    if (src.remaining() < Bits.INT_SIZE_IN_BYTES) {
                        return;
                    }
                    size = src.getInt();
                    if (cipherBuffer.capacity() < size) {
                        cipherBuffer = ByteBuffer.allocate(size);
                    }
                }
                int remaining = src.remaining();
                if (remaining < size) {
                    cipher.update(src, cipherBuffer);
                    size -= remaining;
                } else if (remaining == size) {
                    cipher.doFinal(src, cipherBuffer);
                    size = -1;
                } else {
                    int oldLimit = src.limit();
                    int newLimit = src.position() + size;
                    src.limit(newLimit);
                    cipher.doFinal(src, cipherBuffer);
                    src.limit(oldLimit);
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
