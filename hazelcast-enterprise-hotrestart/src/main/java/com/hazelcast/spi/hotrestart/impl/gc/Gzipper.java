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

import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.createFileInputStream;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Gzips stable chunk files. This class is currently unused because perforamnce is
 * insufficient. LZ4 is being considered instead.
 */
public final class Gzipper {
    /** Gzipped file suffix. */
    public static final String GIP_SUFFIX = ".gz";
    /** Minimum file size to gzip. */
    public static final int MIN_SIZE_TO_GZIP = 32 * 1024;
    private static final byte[] BUF = new byte[BufferedGzipOutputStream.BUFFER_SIZE];

    private Gzipper() { }

    @SuppressWarnings("checkstyle:npathcomplexity")
    static boolean gzip(Set<StableChunk> chunks, GcHelper chunkFactory, MutatorCatchup mc) {
        final long start = System.nanoTime();
        final StableChunk chunk = selectChunkToGzip(chunks);
        if (chunk == null) {
            return false;
        }
        final File outFile = chunkFactory.chunkFile(chunk.seq, Chunk.FNAME_SUFFIX + GIP_SUFFIX, true);
        final FileOutputStream fileOut = GcHelper.createFileOutputStream(outFile);
        if (fileOut == null) {
            return false;
        }
        System.err.print("Gzipping chunk #" + chunk.seq);
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
            final GZIPOutputStream out = new GZIPOutputStream(fileOut);
            for (int count; (count = in.read(BUF)) != -1;) {
                out.write(BUF, 0, count);
                fileOut.getChannel().force(true);
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
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        } finally {
            System.err.format(" took %,d ms%n", NANOSECONDS.toMillis(System.nanoTime() - start));
        }
        return didCatchUp;
    }

    private static StableChunk selectChunkToGzip(Set<StableChunk> chunks) {
        double lowestCb = Double.MAX_VALUE;
        StableChunk mostStableChunk = null;
        for (StableChunk c : chunks) {
            if (c.compressed || c.size() < MIN_SIZE_TO_GZIP) {
                continue;
            }
            final double cb = c.cachedCostBenefit();
            if (cb < lowestCb) {
                mostStableChunk = c;
                lowestCb = cb;
            }
        }
        return mostStableChunk;
    }
}
