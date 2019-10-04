package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.hazelcast.internal.hotrestart.impl.HotRestarter.BUFFER_SIZE;

/**
 * Encapsulates encrypted chunk file writing code.
 */
public class EncryptedChunkFileOut extends ChunkFileOut {

    private static final int CIPHER_OVERHEAD = 128;
    // we use BUFFER_SIZE (+ some more for padding etc.) to make sure that the encrypted
    // contents of ChunkFileOut.buf always fit
    private byte[] outBuffer = new byte[BUFFER_SIZE + CIPHER_OVERHEAD];
    private ByteBuffer outByteBuffer = ByteBuffer.wrap(outBuffer);

    private final Cipher cipher;
    private boolean closed;

    public EncryptedChunkFileOut(File file, MutatorCatchup mc, Cipher cipher) throws IOException {
        super(file, mc);
        this.cipher = cipher;
    }

    @Override
    protected int doWrite(FileChannel chan, ByteBuffer from) throws IOException {
        assert cipher.getOutputSize(from.remaining()) <= outBuffer.length;
        try {
            outByteBuffer.clear();
            cipher.update(from, outByteBuffer);
            assert from.remaining() == 0;
            outByteBuffer.flip();
            return chan.write(outByteBuffer);
        } catch (ShortBufferException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected int doWrite(FileOutputStream fos, byte[] b, int off, int len) throws IOException {
        assert cipher.getOutputSize(len) <= outBuffer.length;
        try {
            int written = cipher.update(b, off, len, outBuffer);
            if (written > 0) {
                fos.write(outBuffer, 0, written);
            }
            return written;
        } catch (ShortBufferException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void prepareClose(FileOutputStream fos) throws IOException {
        if (!closed) {
            closed = true;
            try {
                int written = cipher.doFinal(outBuffer, 0);
                if (written > 0) {
                    fos.write(outBuffer, 0, written);
                }
            } catch (ShortBufferException | BadPaddingException | IllegalBlockSizeException e) {
                throw new IOException(e);
            }
            super.prepareClose(fos);
        }
    }

}
