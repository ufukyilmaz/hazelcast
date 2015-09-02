package com.hazelcast.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.CipherHelper;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packetdispatcher.PacketDispatcher;
import com.hazelcast.util.ExceptionUtil;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.IOService.KILO_BYTE;

public class SymmetricCipherMemberReadHandler extends MemberReadHandler {

    private final Cipher cipher;
    private final ILogger logger;
    private final IOService ioService;
    private ByteBuffer cipherBuffer;
    private int size = -1;

    public SymmetricCipherMemberReadHandler(TcpIpConnection connection, IOService ioService, PacketDispatcher packetDispatcher) {
        super(connection, packetDispatcher);
        this.ioService = ioService;
        this.cipherBuffer = ByteBuffer.allocate(ioService.getSocketReceiveBufferSize() * KILO_BYTE);
        this.logger = ioService.getLogger(getClass().getName());
        this.cipher = initCipher();
    }

    Cipher initCipher() {
        try {
            return CipherHelper.createSymmetricReaderCipher(ioService.getSymmetricEncryptionConfig());
        } catch (Exception e) {
            logger.severe("Symmetric Cipher for ReadHandler cannot be initialized.", e);
            CipherHelper.handleCipherException(e, connection);
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
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
