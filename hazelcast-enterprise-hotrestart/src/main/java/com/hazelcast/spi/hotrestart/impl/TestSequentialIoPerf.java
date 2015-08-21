/*
 *
 *  * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

//Throwaway class, used for research and development
//CHECKSTYLE:OFF

package com.hazelcast.spi.hotrestart.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.out;
import static java.lang.Thread.currentThread;

@SuppressFBWarnings
public final class TestSequentialIoPerf {
    static final long TEST_FILE_SIZE = 1 << 30;
    public static final int TEST_BUF_SIZE = 1 << 12;
    static byte[] TEST_BUF = new byte[TEST_BUF_SIZE];
    static final String SOURCE_FILE_NAME = "mockdata.bin";
    static final String TEST_FILE_NAME = "test.dat";
    static File homeDir;
    static CyclicBarrier barrier;

    static final AtomicLong sum = new AtomicLong();

    public static void main(final String[] args) throws Exception {
        final int threads = args.length > 0? Integer.parseInt(args[0]) : 1;
        homeDir = new File(args.length > 1 ? args[1] : ".");
//        fillTestBufWithSourceData();
        barrier = new CyclicBarrier(threads + 1);
        for (int i = 0; i < threads; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    testingTask("" + currentThread().getId());
                }
            }).start();
        }
        while (true) {
            barrier.await();
            System.out.format("                                                 SUM %,d\n", sum.getAndSet(0));
        }
    }

    private static void testingTask(String id) {
        try {
            final String fileName = new File(homeDir, TEST_FILE_NAME + '_' +id).getCanonicalPath();
            for (final PerfTestCase testCase : testCases) {
                System.out.format(id + " Using file size of %,d\n", TEST_FILE_SIZE);
                for (int i = 0; i < 5; i++) {
                    System.gc();
                    long writeDurationMs = testCase.test(PerfTestCase.Type.WRITE, fileName);
                    long bytesWrittenPerSec = (TEST_FILE_SIZE * 1000L) / writeDurationMs;
                    sum.addAndGet(bytesWrittenPerSec);
                    barrier.await();
                    out.format("%s %s\twrite=%,d bytes/sec\n", id, testCase.getName(), bytesWrittenPerSec);
                    for (int j = 0; j < 5; j++) {
                        purgeCache();
                        System.gc();
                        long readDurationMs = testCase.test(PerfTestCase.Type.READ, fileName);
                        long bytesReadPerSec = (TEST_FILE_SIZE * 1000L) / readDurationMs;
                        out.format("%s %s\tread=%,d bytes/sec\n", id, testCase.getName(), bytesReadPerSec);
                        sum.addAndGet(bytesReadPerSec);
                        barrier.await();
                    }
                }
            }
            deleteFile(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void purgeCache() throws IOException, InterruptedException {
        if (System.getProperty("os.name").startsWith("Mac")) {
            new ProcessBuilder("sudo", "purge")
//                    .inheritIO()
                    .start().waitFor();
        } else {
            new ProcessBuilder("sudo", "su", "-c", "echo 3 > /proc/sys/vm/drop_caches")
//                    .inheritIO()
                    .start().waitFor();
        }
    }

    private static void deleteFile(final String testFileName) throws Exception {
        File file = new File(testFileName);
        if (!file.delete()) {
            out.println("Failed to delete test file=" + testFileName);
            out.println("Windows does not allow mapped files to be deleted.");
        }
    }

    abstract static class PerfTestCase {
        enum Type {READ, WRITE}

        private final String name;
        private int checkSum;

        PerfTestCase(final String name) { this.name = name; }
        String getName() { return name; }

        long test(final Type type, final String fileName) {
            long start = System.currentTimeMillis();
            try {
                switch (type) {
                    case WRITE:
                        checkSum = testWrite(fileName);
                        break;
                    case READ:
                        final int checkSum = testRead(fileName);
                        if (checkSum != this.checkSum) {
                            throw new IllegalStateException(
                                    getName() + " expected=" + this.checkSum + " got=" + checkSum);
                        }
                        break;
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            return System.currentTimeMillis() - start;
        }
        abstract int testWrite(final String fileName) throws Exception;
        abstract int testRead(final String fileName) throws Exception;
    }
    private static int writeStreamWithBuffer(OutputStream out) throws Exception {
        int checkSum = 0;
        for (long i = 0; i < TEST_FILE_SIZE;) {
            for (int j = 0; j < TEST_BUF_SIZE; j++) {
                checkSum += TEST_BUF[j];
            }
            out.write(TEST_BUF, 0, TEST_BUF_SIZE);
            i += TEST_BUF_SIZE;
        }
        return checkSum;
    }

    private static int readStreamWithBuffer(InputStream in) throws Exception {
        final byte[] buffer = new byte[TEST_BUF_SIZE];
        int checkSum = 0;
        int bytesRead;
        while (-1 != (bytesRead = in.read(buffer))) {
            for (int i = 0; i < bytesRead; i++) {
                checkSum += buffer[i];
            }
        }
        return checkSum;
    }

    private static PerfTestCase[] testCases = {
            new PerfTestCase("StreamFile") {
                @Override int testWrite(final String fileName) throws Exception {
                    final FileOutputStream out = new FileOutputStream(fileName);
                    try {
                        return writeStreamWithBuffer(out);
                    } finally {
                        out.getChannel().force(true);
                        out.close();
                    }
                }
                @Override int testRead(final String fileName) throws Exception {
                    final FileInputStream file = new FileInputStream(fileName);
                    try {
                        return readStreamWithBuffer(file);
                    } finally {
                        file.close();
                    }
                }
            },
            new PerfTestCase("BufferedChannelFile") {
                @Override int testWrite(final String fileName) throws Exception {
                    final FileChannel channel = new RandomAccessFile(fileName, "rw").getChannel();
                    final ByteBuffer buffer = ByteBuffer.allocate(TEST_BUF_SIZE);
                    int checkSum = 0;
                    for (long i = 0; i < TEST_FILE_SIZE; i++) {
                        byte b = (byte) i;
                        checkSum += b;
                        buffer.put(b);
                        if (!buffer.hasRemaining()) {
                            buffer.flip();
                            channel.write(buffer);
                            buffer.clear();
                        }
                    }
                    channel.force(true);
                    channel.close();
                    return checkSum;
                }

                @Override int testRead(final String fileName) throws Exception {
                    final FileChannel channel = new RandomAccessFile(fileName, "rw").getChannel();
                    final ByteBuffer buffer = ByteBuffer.allocate(TEST_BUF_SIZE);
                    int checkSum = 0;
                    while (-1 != (channel.read(buffer))) {
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            checkSum += buffer.get();
                        }
                        buffer.clear();
                    }
                    return checkSum;
                }
            },
    };

    private static void fillTestBufWithSourceData() throws IOException {
        final InputStream sourceIn = new FileInputStream(new File(homeDir, SOURCE_FILE_NAME));
        readFully(sourceIn, TEST_BUF);
        sourceIn.close();
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
