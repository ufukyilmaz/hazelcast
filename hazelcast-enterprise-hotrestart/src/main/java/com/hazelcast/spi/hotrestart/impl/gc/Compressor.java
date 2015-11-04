package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;
import com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockInputStream;
import com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.closeIgnoringFailure;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.HEADER_LENGTH;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Compresses stable chunk files.
 */
public final class Compressor implements Disposable {
    /** Suffix for compressed files. */
    public static final String COMPRESSED_SUFFIX = ".lz4";
    /** LZ4 block size */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int BLOCK_SIZE = 1 << 18;
    private final ByteBuffer decompressedBuffer = directBuffer(BLOCK_SIZE);
    private final ByteBuffer compressedBuffer =
            directBuffer(HEADER_LENGTH + LZ4Factory.nativeInstance().fastCompressor().maxCompressedLength(BLOCK_SIZE));
    private final byte[] buf = new byte[BLOCK_SIZE];

    public static LZ4Factory lz4Factory() {
        return LZ4Factory.unsafeInstance();
    }

    public OutputStream compressedOutputStream(FileOutputStream out) {
        return new NativeLZ4BlockOutputStream(out.getChannel(), BLOCK_SIZE, decompressedBuffer, compressedBuffer);
    }

    public InputStream compressedInputStream(FileInputStream in) {
        return new NativeLZ4BlockInputStream(in.getChannel(), decompressedBuffer, compressedBuffer);
    }

    @SuppressWarnings({ "checkstyle:innerassignment", "checkstyle:npathcomplexity" })
    public boolean lz4Compress(StableChunk chunk, GcHelper gcHelper, MutatorCatchup mc, GcLogger logger) {
        final long start = System.nanoTime();
        if (chunk == null) {
            return false;
        }
        logger.fine("LZ4 compressing chunk %03x", chunk.seq);
        final File outFile = gcHelper.chunkFile(chunk.seq, Chunk.FNAME_SUFFIX + COMPRESSED_SUFFIX, true);
        final FileOutputStream fileOut = gcHelper.createFileOutputStream(outFile);
        if (fileOut == null) {
            // happens only if IO is disabled by configuration
            chunk.compressed = true;
            return false;
        }
        FileInputStream in = null;
        OutputStream out = null;
        boolean didCatchUp = false;
        try {
            final File inFile = gcHelper.chunkFile(chunk.seq, Chunk.FNAME_SUFFIX, false);
            in = gcHelper.createFileInputStream(inFile);
            out = compressedOutputStream(fileOut);
            //noinspection ConstantConditions (fileOut != null already implies in != null)
            for (int count; (count = in.read(buf)) != -1;) {
                out.write(buf, 0, count);
                didCatchUp |= mc.catchupNow() > 0;
            }
            out.flush();
            fileOut.getChannel().force(true);
            out.close();
            chunk.compressed = true;
            if (!inFile.delete()) {
                throw new IOException("failed to delete " + inFile);
            }
        } catch (Exception e) {
            e.printStackTrace();
            closeIgnoringFailure(out);
            if (!outFile.delete()) {
                logger.severe("failed to delete " + outFile);
            }
        } finally {
            closeIgnoringFailure(out);
            closeIgnoringFailure(in);
            logger.fine("Compression took %,d ms", NANOSECONDS.toMillis(System.nanoTime() - start));
        }
        return didCatchUp;
    }

    @Override public void dispose() {
        destroyDirectBuffer(decompressedBuffer);
        destroyDirectBuffer(compressedBuffer);
    }

    public static ByteBuffer directBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    public static void destroyDirectBuffer(ByteBuffer buf) {
        if (buf == null || !buf.isDirect()) {
            return;
        }
        ((sun.nio.ch.DirectBuffer) buf).cleaner().clean();
    }
}
