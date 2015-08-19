/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;
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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.createFileInputStream;
import static com.hazelcast.spi.hotrestart.impl.lz4.NativeLZ4BlockOutputStream.HEADER_LENGTH;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Gzips stable chunk files.
 */
public final class Compression {
    /** Suffix for compressed files. */
    public static final String COMPRESSED_SUFFIX = ".lz4";
    /** LZ4 block size */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int BLOCK_SIZE = 1 << 18;
    private static final byte[] BUF = new byte[BLOCK_SIZE];

    private static final ByteBuffer DECOMPRESSED_BUFFER = directBuffer(BLOCK_SIZE);
    private static final ByteBuffer COMPRESSED_BUFFER = directBuffer(HEADER_LENGTH
            + LZ4Factory.nativeInstance().fastCompressor().maxCompressedLength(BLOCK_SIZE));
    private static final Method METHOD_GET_CLEANER = methodGetCleaner(COMPRESSED_BUFFER);
    private static final Method METHOD_CLEAN = methodClean(METHOD_GET_CLEANER, COMPRESSED_BUFFER);

    private Compression() { }

    public static LZ4Factory lz4Factory() {
        return LZ4Factory.unsafeInstance();
    }

    public static OutputStream compressedOutputStream(FileOutputStream out) {
        return new NativeLZ4BlockOutputStream(out.getChannel(), BLOCK_SIZE);
    }

    public static InputStream compressedInputStream(FileInputStream in) {
//        return new LZ4BlockInputStream(in);
        return new NativeLZ4BlockInputStream(in.getChannel(), DECOMPRESSED_BUFFER, COMPRESSED_BUFFER);
    }

    @SuppressWarnings({ "checkstyle:innerassignment", "checkstyle:npathcomplexity" })
    static boolean lz4Compress(StableChunk chunk, GcHelper chunkFactory, MutatorCatchup mc) {
        final long start = System.nanoTime();
        if (chunk == null) {
            return false;
        }
        final File outFile = chunkFactory.chunkFile(chunk.seq, Chunk.FNAME_SUFFIX + COMPRESSED_SUFFIX, true);
        final FileOutputStream fileOut = GcHelper.createFileOutputStream(outFile);
        if (fileOut == null) {
            return false;
        }
        System.err.format("LZ4 compressing chunk %016x", chunk.seq);
        System.err.flush();
        final File inFile = chunkFactory.chunkFile(chunk.seq, Chunk.FNAME_SUFFIX, false);
        final FileInputStream in = createFileInputStream(inFile);
        if (in == null) {
            System.err.println();
            chunk.compressed = true;
            return false;
        }
        boolean didCatchUp = false;
        try {
            final OutputStream out = compressedOutputStream(fileOut);
            for (int count; (count = in.read(BUF)) != -1;) {
                out.write(BUF, 0, count);
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
            if (!outFile.delete()) {
                System.err.println("failed to delete " + outFile);
            }
            try {
                fileOut.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } finally {
            System.err.format(" took %,d ms%n", NANOSECONDS.toMillis(System.nanoTime() - start));
        }
        return didCatchUp;
    }

    public static ByteBuffer directBuffer(int capacity) {
        final ByteBuffer b = ByteBuffer.allocateDirect(capacity);
        b.order(ByteOrder.LITTLE_ENDIAN);
        return b;
    }

    public static void destroyDirectBuffer(ByteBuffer directByteBuf) {
        if (!directByteBuf.isDirect()) {
            return;
        }
        try {
            METHOD_CLEAN.invoke(METHOD_GET_CLEANER.invoke(directByteBuf));
        } catch (Exception e) {
            throw new HotRestartException(e);
        }
    }

    private static Method methodGetCleaner(ByteBuffer directBuf) {
        try {
            final Method getCleaner = directBuf.getClass().getMethod("cleaner");
            getCleaner.setAccessible(true);
            return getCleaner;
        } catch (NoSuchMethodException e) {
            throw new HotRestartException(e);
        }
    }

    private static Method methodClean(Method getCleaner, ByteBuffer directBuf) {
        try {
            final Object cleaner = getCleaner.invoke(directBuf);
            final Method cleanMethod = cleaner.getClass().getMethod("clean");
            cleanMethod.setAccessible(true);
            return cleanMethod;
        } catch (Exception e) {
            throw new HotRestartException(e);
        }
    }
}
