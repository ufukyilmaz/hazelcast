package com.hazelcast.enterprise.nio.tcp;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.CipherHelper;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.tcp.DefaultPacketReader;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.util.ExceptionUtil;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;

public class SymmetricCipherPacketReader extends DefaultPacketReader {

    private static final int CONST_BUFFER_NO = 4;

    int size = -1;
    final Cipher cipher;
    final ILogger logger;
    ByteBuffer cipherBuffer = ByteBuffer.allocate(ioService.getSocketReceiveBufferSize() * IOService.KILO_BYTE);

    public SymmetricCipherPacketReader(TcpIpConnection connection, IOService ioService) {
        super(connection, ioService);
        logger = ioService.getLogger(getClass().getName());
        cipher = init();
    }

    Cipher init() {
        Cipher c;
        try {
            c = CipherHelper.createSymmetricReaderCipher(ioService.getSymmetricEncryptionConfig());
        } catch (Exception e) {
            logger.severe("Symmetric Cipher for ReadHandler cannot be initialized.", e);
            CipherHelper.handleCipherException(e, connection);
            throw ExceptionUtil.rethrow(e);
        }
        return c;
    }

    @Override
    public void readPacket(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            try {
                if (size == -1) {
                    if (inBuffer.remaining() < CONST_BUFFER_NO) {
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
                    packet = obtainPacket();
                }
                boolean complete = packet.readFrom(cipherBuffer);
                if (complete) {
                    packet.setConn(connection);
                    ioService.handleMemberPacket(packet);
                    packet = null;
                }
            }
            cipherBuffer.clear();
        }
    }
}
