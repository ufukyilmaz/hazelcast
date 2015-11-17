/*
 * Original work Copyright (c) 2015 Adrien Grand
 * Modified work Copyright (c) 2015 Hazelcast, Inc.
 */
package com.hazelcast.spi.hotrestart.impl.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * LZ4 output stream using native LZ4 compressor and a direct byte buffer.
 */
public final class NativeLZ4BlockOutputStream extends OutputStream {
    /** Magic bytes that identify a block as compressed by this output stream. */
    static final byte[] MAGIC = new byte[] {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
    static final int MAGIC_LENGTH = MAGIC.length;

    /** Length of the LZ4 block header. */
    @SuppressWarnings({ "checkstyle:declarationorder", "checkstyle:trailingcomment" })
    public static final int HEADER_LENGTH =
            MAGIC_LENGTH // magic bytes
                    + 1          // token
                    + 4          // compressed length
                    + 4          // decompressed length
                    + 4;         // checksum

    static final int COMPRESSION_LEVEL_BASE = 10;
    static final int MIN_BLOCK_SIZE = 64;
    static final int MAX_BLOCK_SIZE = 1 << (COMPRESSION_LEVEL_BASE + 0x0F);

    static final int COMPRESSION_METHOD_RAW = 0x10;
    static final int COMPRESSION_METHOD_LZ4 = 0x20;

    private static final int DEFAULT_SEED = 0x9747b28c;

    private final int compressionLevel;
    private final LZ4Compressor compressor;
    private final XXHash32 xxHash = XXHashFactory.nativeInstance().hash32();
    private final ByteBuffer buffer;
    private final ByteBuffer compressedBuffer;
    private FileChannel out;
    private boolean finished;

    /**
     * Create a new {@link OutputStream} with configurable block size. Large
     * blocks require more memory at compression and decompression time but
     * should improve the compression ratio.
     *
     * @param out         the {@link OutputStream} to feed
     * @param blockSize   the maximum number of bytes to try to compress at once,
     *                    must be >= 64 and <= 32 M
     */
    public NativeLZ4BlockOutputStream(FileChannel out, int blockSize,
                                      ByteBuffer decompressedBuffer, ByteBuffer compressedBuffer) {
        this.out = out;
        this.compressor = LZ4Factory.nativeInstance().fastCompressor();
        this.compressionLevel = compressionLevel(blockSize);
        this.buffer = decompressedBuffer;
        this.compressedBuffer = compressedBuffer.put(MAGIC);
    }

    private void ensureNotFinished() {
        if (finished) {
            throw new IllegalStateException("This stream is already closed");
        }
    }

    @Override
    public void write(int b) throws IOException {
        ensureNotFinished();
        if (!buffer.hasRemaining()) {
            flushBufferedData();
        }
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureNotFinished();
        int transferredCount;
        while (len > (transferredCount = buffer.remaining())) {
            buffer.put(b, off, transferredCount);
            flushBufferedData();
            off += transferredCount;
            len -= transferredCount;
        }
        buffer.put(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void close() throws IOException {
        if (!finished) {
            finish();
        }
        if (out != null) {
            out.close();
            out = null;
        }
    }

    private void flushBufferedData() throws IOException {
        final int uncompressedLength = buffer.position();
        if (uncompressedLength == 0) {
            return;
        }
        final int check = xxHash.hash(buffer, 0, uncompressedLength, DEFAULT_SEED);
        buffer.flip().mark();
        compressedBuffer.clear().position(HEADER_LENGTH).mark();
        compressor.compress(buffer, compressedBuffer);
        int compressedLength = compressedBuffer.position() - HEADER_LENGTH;
        final int compressMethod;
        if (compressedLength >= uncompressedLength) {
            compressMethod = COMPRESSION_METHOD_RAW;
            compressedLength = uncompressedLength;
            buffer.reset();
            compressedBuffer.reset();
            compressedBuffer.put(buffer);
        } else {
            compressMethod = COMPRESSION_METHOD_LZ4;
        }
        final int mark = compressedBuffer.position();
        compressedBuffer.position(MAGIC_LENGTH);
        compressedBuffer.put((byte) (compressMethod | compressionLevel))
                .putInt(compressedLength)
                .putInt(uncompressedLength)
                .putInt(check);
        assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        compressedBuffer.position(0).limit(mark);
        do {
            out.write(compressedBuffer);
        } while (compressedBuffer.hasRemaining());
        buffer.clear();
    }

    /**
     * Flush this compressed {@link OutputStream}.
     *
     * If the stream has been created with <code>syncFlush=true</code>, pending
     * data will be compressed and appended to the underlying {@link OutputStream}
     * before calling {@link OutputStream#flush()} on the underlying stream.
     * Otherwise, this method just flushes the underlying stream, so pending
     * data might not be available for reading until {@link #finish()} or
     * {@link #close()} is called.
     */
    @Override
    public void flush() throws IOException {
        if (out != null) {
            flushBufferedData();
        }
    }

    /**
     * Same as {@link #close()} except that it doesn't close the underlying stream.
     * This can be useful if you want to keep on using the underlying stream.
     */
    public void finish() throws IOException {
        ensureNotFinished();
        flushBufferedData();
        compressedBuffer.position(MAGIC_LENGTH);
        compressedBuffer.put((byte) (COMPRESSION_METHOD_RAW | compressionLevel)).putInt(0).putInt(0).putInt(0);
        assert MAGIC_LENGTH + 13 == HEADER_LENGTH;
        compressedBuffer.flip();
        do {
            out.write(compressedBuffer);
        } while (compressedBuffer.hasRemaining());
        finished = true;
    }

    private static int compressionLevel(int blockSize) {
        if (blockSize < MIN_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be >= " + MIN_BLOCK_SIZE + ", got " + blockSize);
        } else if (blockSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException("blockSize must be <= " + MAX_BLOCK_SIZE + ", got " + blockSize);
        }
        int compressionLevel = 32 - Integer.numberOfLeadingZeros(blockSize - 1);
        assert (1 << compressionLevel) >= blockSize;
        assert blockSize * 2 > (1 << compressionLevel);
        compressionLevel = Math.max(0, compressionLevel - COMPRESSION_LEVEL_BASE);
        assert compressionLevel >= 0 && compressionLevel <= 0x0F;
        return compressionLevel;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(out=" + out + ", blockSize=" + compressedBuffer.capacity()
                + ", compressor=" + compressor + ')';
    }

}
