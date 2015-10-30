/*
 * Original work Copyright (c) 2015 Adrien Grand
 * Modified work Copyright (c) 2015 Hazelcast, Inc.
 */
package com.hazelcast.spi.hotrestart.impl.lz4;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.COMPRESSION_LEVEL_BASE;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.COMPRESSION_METHOD_LZ4;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.COMPRESSION_METHOD_RAW;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.HEADER_LENGTH;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.MAGIC;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.MAGIC_LENGTH;

/**
 * LZ4 input stream using the native LZ4 decompressor and a direct byte buffer.
 */
public final class NativeLZ4BlockInputStream extends InputStream {
    private final FileChannel in;
    private final LZ4FastDecompressor decompressor;
    private final XXHash32 xxHash = XXHashFactory.nativeInstance().hash32();
    private ByteBuffer buffer;
    private ByteBuffer compressedBuffer;
    private boolean finished;

    public NativeLZ4BlockInputStream(FileChannel in, ByteBuffer decompressedBuffer, ByteBuffer compressedBuffer) {
        this.in = in;
        this.decompressor = LZ4Factory.nativeInstance().fastDecompressor();
        decompressedBuffer.clear().limit(0);
        compressedBuffer.clear();
        this.buffer = decompressedBuffer;
        this.compressedBuffer = compressedBuffer;
    }

    @Override
    public int available() throws IOException {
        return buffer.remaining();
    }

    @Override
    public int read() throws IOException {
        if (finished) {
            return -1;
        }
        if (!buffer.hasRemaining()) {
            refill();
        }
        if (finished) {
            return -1;
        }
        return buffer.get();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (finished) {
            return -1;
        }
        if (!buffer.hasRemaining()) {
            refill();
        }
        if (finished) {
            return -1;
        }
        len = Math.min(len, buffer.remaining());
        buffer.get(b, off, len);
        return len;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException {
        if (finished) {
            return -1;
        }
        if (!buffer.hasRemaining()) {
            refill();
        }
        if (finished) {
            return -1;
        }
        final int skipped = (int) Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + skipped);
        return skipped;
    }

    @Override public void close() throws IOException {
        in.close();
        this.buffer = null;
        this.compressedBuffer = null;
    }

    private void refill() throws IOException {
        readFully(compressedBuffer, HEADER_LENGTH);
        for (int i = 0; i < MAGIC_LENGTH; ++i) {
            if (compressedBuffer.get() != MAGIC[i]) {
                throw new IOException("Stream is corrupted");
            }
        }
        final int token = compressedBuffer.get();
        final int compressionMethod = token & 0xF0;
        final int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
        if (compressionMethod != COMPRESSION_METHOD_RAW && compressionMethod != COMPRESSION_METHOD_LZ4) {
            throw new IOException("Stream is corrupted");
        }
        final int compressedLen = compressedBuffer.getInt();
        final int originalLen = compressedBuffer.getInt();
        final int check = compressedBuffer.getInt();
        assert HEADER_LENGTH == MAGIC_LENGTH + 13;
        if (originalLen > 1 << compressionLevel
                || originalLen < 0
                || compressedLen < 0
                || (originalLen == 0 && compressedLen != 0)
                || (originalLen != 0 && compressedLen == 0)
                || (compressionMethod == COMPRESSION_METHOD_RAW && originalLen != compressedLen)) {
            throw new IOException("Stream is corrupted");
        }
        if (originalLen == 0) {
            if (check != 0) {
                throw new IOException("Stream is corrupted");
            }
            finished = true;
            return;
        }
        if (buffer.capacity() < originalLen) {
            throw new LZ4Exception(
                    "Decompressed buffer too small. Required " + originalLen + ", actual " + buffer.capacity());
//            destroyDirectBuffer(buffer);
//            buffer = directBuffer(Math.max(originalLen, buffer.capacity() * 3 / 2));
        }
        switch (compressionMethod) {
            case COMPRESSION_METHOD_RAW:
                readFully(buffer, originalLen);
                break;
            case COMPRESSION_METHOD_LZ4:
                if (compressedBuffer.capacity() < compressedLen) {
                    throw new LZ4Exception(
                            "Compressed buffer too small. Required " + compressedLen + ", actual "
                                    + compressedBuffer.capacity());
//                    destroyDirectBuffer(compressedBuffer);
//                    compressedBuffer =
//                            directBuffer(Math.max(compressedLen, compressedBuffer.capacity() * 3 / 2));
                }
                readFully(compressedBuffer, compressedLen);
                try {
                    buffer.clear();
                    buffer.limit(originalLen);
                    decompressor.decompress(compressedBuffer, buffer);
                    buffer.flip();
                } catch (LZ4Exception e) {
                    throw new IOException("Stream is corrupted", e);
                }
                break;
            default:
                throw new AssertionError();
        }
//        if (xxHash.hash(buffer, 0, originalLen, DEFAULT_SEED) != check) {
//            throw new IOException("Stream is corrupted (checksum mismatch)");
//        }
    }

    private void readFully(ByteBuffer b, int len) throws IOException {
        b.clear();
        b.limit(len);
        do {
            if (in.read(b) == -1) {
                throw new EOFException();
            }
        } while (b.hasRemaining());
        b.flip();
    }

    @Override public boolean markSupported() {
        return false;
    }

    @Override public void mark(int readlimit) {
        // unsupported
    }

    @Override public void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override public String toString() {
        return getClass().getSimpleName() + "(in=" + in + ')';
    }

}
