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

package com.hazelcast.spi.hotrestart.impl.lz4;

import com.hazelcast.nio.Bits;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.hotrestart.impl.gc.Compression.BLOCK_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.Compression.directBuffer;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

//CHECKSTYLE:OFF
@SuppressFBWarnings
class TestPerformance {
    static final String SOURCE_FILE_NAME = "test.dat";
    public static final int BUF_SIZE = 1 << 16;
    static final long
            WARMUP_TIME = SECONDS.toNanos(1),
            MEASUREMENT_TIME = SECONDS.toNanos(1);
    static final byte[]
            INPUT_BUF = new byte[BUF_SIZE],
            COMPRESSED_BUF = new byte[BUF_SIZE],
            DECOMPRESSED_BUF = new byte[BUF_SIZE];

    public static void amain(String[] args) throws Exception {
        for (int period = 1; period < 33; period++) {
            final OutputStream out =
                    new BufferedOutputStream(new FileOutputStream(String.format("cyclicdata-%02d.bin", period)));
            final int fileSize = 10 * 1000 * 1000;
            out.write("123".getBytes(Bits.ISO_8859_1));
            for (int i = 0; i < fileSize; i++) {
                out.write('A' + i % period);
            }
            out.close();
        }
    }

    private static void readCompressed() throws IOException {
        final InputStream cin = new NativeLZ4BlockInputStream(
                new FileInputStream("silesia.lz4").getChannel(),
                directBuffer(BLOCK_SIZE),
                directBuffer(LZ4Factory.nativeInstance().fastCompressor().maxCompressedLength(BLOCK_SIZE)));
        final long start = System.nanoTime();
        int sum = 0;
        for (int count; (count = cin.read(DECOMPRESSED_BUF)) != -1; ) {
            sum += count;
        }
        final long tookMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.format("%,f MB/s\n", (double) sum / tookMicros);
        cin.close();
    }

    public static void xmain(String[] args) throws IOException {
        fillInputBufWithSequenceData();
//        measure(new CompressionTask(LZ4Factory.unsafeInstance().fastCompressor()), "Unsafe compressor");
//        measure(new DecompressionTask(LZ4Factory.unsafeInstance().fastDecompressor()), "Unsafe decompressor");
//        verifyResult();

        measure(new CompressionTask(LZ4Factory.unsafeInstance().fastCompressor()), "Unsafe compressor");
        verifyResult();

//        measure(new CompressionTask(LZ4Factory.safeInstance().fastCompressor()), "Safe compressor");
//        measure(new DecompressionTask(LZ4Factory.safeInstance().fastDecompressor()), "Safe decompressor");
//        verifyResult();
//
//        measure(new CompressionTask(LZ4Factory.nativeInstance().fastCompressor()), "Native compressor");
//        measure(new DecompressionTask(LZ4Factory.nativeInstance().fastDecompressor()), "Native decompressor");
//        verifyResult();
    }

    private static void stopHere() {
        throw new RuntimeException("Stop here");
    }

    static class CompressionTask implements Runnable {
        final LZ4Compressor compressor;
        CompressionTask(LZ4Compressor compressor) {
            this.compressor = compressor;
        }
        @Override public void run() {
            compressor.compress(INPUT_BUF, COMPRESSED_BUF);
        }
    }

    static class DecompressionTask implements Runnable {
        final LZ4FastDecompressor decompressor;
        DecompressionTask(LZ4FastDecompressor decompressor) {
            this.decompressor = decompressor;
        }
        @Override public void run() {
            decompressor.decompress(COMPRESSED_BUF, DECOMPRESSED_BUF);
        }
    }

    private static void measure(Runnable task, String name) {
        long start = System.nanoTime();
        System.out.println(name+" warmup");
        do {
            task.run();
        } while (System.nanoTime() - start < WARMUP_TIME);
        System.out.println(name+" measurement");
        start = System.nanoTime();
        long byteCount = 0;
        long took;
        do {
            task.run();
            byteCount += BUF_SIZE;
        } while ((took = System.nanoTime() - start) < MEASUREMENT_TIME);
        System.out.format("%s %,d bytes/second\n", name, (byteCount * SECONDS.toMillis(1)) / NANOSECONDS.toMillis(took));
    }

    private static void verifyResult() {
        int mismatchAt = -1;
        for (int i = 0; i < BUF_SIZE; i++) {
            if (INPUT_BUF[i] != DECOMPRESSED_BUF[i]) {
                mismatchAt = i;
                break;
            }
        }
        if (mismatchAt != -1) {
            for (int i = max(0, mismatchAt - 16); i < BUF_SIZE; i++) {
                final boolean mismatch = INPUT_BUF[i] != DECOMPRESSED_BUF[i];
                System.out.format("%6x: %2X %2X%s\n", i, INPUT_BUF[i],
                        DECOMPRESSED_BUF[i], mismatch? "   <---- MISMATCH" : "");
            }
        }
        Arrays.fill(COMPRESSED_BUF, (byte) 0);
        Arrays.fill(DECOMPRESSED_BUF, (byte) 0);
    }

    private static void fillInputBufWithRandomData() {
        new Random().nextBytes(INPUT_BUF);
        System.arraycopy(INPUT_BUF, 0, INPUT_BUF, BUF_SIZE / 2, BUF_SIZE / 2);
    }

    private static void fillInputBufWithSourceData() throws IOException {
        final InputStream sourceIn = new FileInputStream(SOURCE_FILE_NAME);
        readFully(sourceIn, INPUT_BUF);
        sourceIn.close();
    }

    static void fillInputBufWithSequenceData() {
        for (int i = 0; i < BUF_SIZE; i++) {
            INPUT_BUF[i] = (byte) (i % (i < BUF_SIZE / 2 ? 9 : 9));
        }
        INPUT_BUF[3] = 127;
    }

    private static void readFully(InputStream in, byte[] b) throws IOException {
        int bytesRead = 0;
        do {
            int count = in.read(b, bytesRead, b.length - bytesRead);
            if (count < 0) {
                throw new EOFException();
            }
            bytesRead += count;
        } while (bytesRead < b.length);
    }
}
