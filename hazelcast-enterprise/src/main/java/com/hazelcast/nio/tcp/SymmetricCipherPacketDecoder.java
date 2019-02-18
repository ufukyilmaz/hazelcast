package com.hazelcast.nio.tcp;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketIOHelper;
import com.hazelcast.util.function.Consumer;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.nio.CipherHelper.createSymmetricReaderCipher;
import static com.hazelcast.nio.IOUtil.compactOrClear;

@SuppressWarnings("checkstyle:npathcomplexity")
public class SymmetricCipherPacketDecoder extends PacketDecoder {

    private final ILogger logger;
    private final Cipher cipher;
    private final PacketIOHelper packetReader = new PacketIOHelper();
    private ByteBuffer cipherBuffer;
    private int size = -1;

    public SymmetricCipherPacketDecoder(SymmetricEncryptionConfig sic, TcpIpConnection connection,
                                        IOService ioService, Consumer<Packet> dst) {
        super(connection, dst);
        this.logger = ioService.getLoggingService().getLogger(getClass());
        this.cipher = createSymmetricReaderCipher(sic, connection);
    }

    @Override
    public void handlerAdded() {
        super.handlerAdded();
        this.cipherBuffer = ByteBuffer.allocate(channel.options().getOption(SO_RCVBUF));
    }

    @Override
    public HandlerStatus onRead() throws Exception {
        src.flip();
        //System.out.println(channel + "      in PacketDecoder:" + IOUtil.toDebugString("src", src));

        try {
            // System.out.println(channel + "      in PacketDecoder after flip:" + IOUtil.toDebugString("src", src));
            while (src.hasRemaining()) {
                try {
                    if (size == -1) {
                        if (src.remaining() < Bits.INT_SIZE_IN_BYTES) {
                            return CLEAN;
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
                    Packet packet = packetReader.readFrom(cipherBuffer);
                    if (packet == null) {
                        break;
                    }
                    onPacketComplete(packet);
                }

                if (cipherBuffer.hasRemaining()) {
                    cipherBuffer.compact();
                } else {
                    cipherBuffer.clear();
                }
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

//    @Override
//    @SuppressWarnings({"checkstyle:npathcomplexity"})
//    public void onRead(ByteBuffer src) throws Exception {

//    }
}
