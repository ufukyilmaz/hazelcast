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
package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.InMemoryStoreRegistry;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.Chunk;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;
import com.hazelcast.spi.hotrestart.impl.gc.Record;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Pattern;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.newRecord;
import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.MAX_BUCKET_DIRS;
import static com.hazelcast.spi.hotrestart.impl.gc.Compression.COMPRESSED_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.Compression.compressedInputStream;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.TOMBSTONE_VALUE;

/**
 * Reads the persistent state and:
 * <ol>
 *     <li>refills the in-memory stores</li>
 *     <li>rebuilds the Hot Restart Store's metadata</li>
 * </ol>
 */
class HotRestarter {
    private static final int HEX_RADIX = 16;
    private static final int CHUNK_FNAME_LENGTH; static {
        final int bitsPerHexDigit = 4;
        CHUNK_FNAME_LENGTH = Long.SIZE / bitsPerHexDigit;
    }
    private static final Comparator<File> BY_SEQ = new Comparator<File>() {
        public int compare(File left, File right) {
            final long leftSeq = seq(left);
            final long rightSeq = seq(right);
            return leftSeq == rightSeq ? 0 : leftSeq < rightSeq ? -1 : 1;
        }
    };
    private static final Pattern RX_BUCKET_DIR = Pattern.compile("[0-9a-f]{3}");
    private static final FileFilter BUCKET_DIRS_ONLY = new FileFilter() {
        public boolean accept(File f) {
            return f.isDirectory() && RX_BUCKET_DIR.matcher(f.getName()).matches();
        }
    };
    private static final FileFilter CHUNK_FILES_ONLY = new FileFilter() {
        public boolean accept(File f) {
            return f.isFile() && (f.getName().endsWith(Chunk.FNAME_SUFFIX)
                                  || f.getName().endsWith(Chunk.FNAME_SUFFIX + COMPRESSED_SUFFIX));
        }
    };

    private final ByteBuffer headerBuf = ByteBuffer.allocate(Record.HEADER_SIZE);
    private final GcHelper chunkFactory;
    private Rebuilder rebuilder;
    private ArrayList<File> chunkFiles;
    private InputStream in;
    private byte[] key;
    private byte[] value;
    private long seq;
    private long prefix;

    public HotRestarter(GcHelper chunkFactory, Rebuilder rebuilder) {
        this.chunkFactory = chunkFactory;
        this.rebuilder = rebuilder;
    }

    public void restart() {
        final InMemoryStoreRegistry reg = chunkFactory.inMemoryStoreRegistry;
        final long highestChunkSeq = init();
        chunkFactory.initChunkSeq(highestChunkSeq);
        while (advance()) {
            final KeyHandle kh = reg.provisionalInMemoryStoreForPrefix(prefix).accept(key, value);
            rebuilder.accept(newRecord(seq, kh, key, value));
        }
        rebuilder.done();
    }

    public boolean advance() {
        try {
            headerBuf.clear();
            if (!tryFill(headerBuf)) {
                return false;
            }
            seq = headerBuf.getLong();
            prefix = headerBuf.getLong();
            key = readBlock(headerBuf.getInt());
            final int valueLen = headerBuf.getInt();
            value = (valueLen < 0) ? TOMBSTONE_VALUE : readBlock(valueLen);
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    /**
     * Initializes the cursor's chunk file queue.
     * @return sequence number of the youngest existing chunk
     */
    @SuppressWarnings("checkstyle:npathcomplexity")
    private long init() {
        final File[] bucketDirs = chunkFactory.homeDir.listFiles(BUCKET_DIRS_ONLY);
        if (bucketDirs == null) {
            throw new HotRestartException("Failed to list directory contents: " + chunkFactory.homeDir);
        }
        for (File d : bucketDirs) {
            final File[] chunksInBucket = d.listFiles(CHUNK_FILES_ONLY);
            if (chunksInBucket == null) {
                throw new HotRestartException("Failed to list directory contents: " + d);
            }
            if (chunksInBucket.length == 0) {
                continue;
            }
            if (chunkFiles == null) {
                chunkFiles = new ArrayList<File>(chunksInBucket.length * MAX_BUCKET_DIRS);
            }
            Collections.addAll(chunkFiles, chunksInBucket);
        }
        if (chunkFiles == null) {
            return 0;
        }
        Collections.sort(chunkFiles, BY_SEQ);
        return tryOpenNextChunk() ? rebuilder.currChunkSeq() : 0;
    }

    private byte[] readBlock(int size) throws IOException {
        final byte[] buf = new byte[size];
        readFully(in, buf);
        return buf;
    }

    private boolean tryFill(ByteBuffer buf) throws IOException {
        return tryFill(buf, true);
    }

    private boolean tryFill(ByteBuffer buf, boolean canRetry) throws IOException {
        if (in == null) {
            return tryOpenNextChunk() && tryFill(buf, false);
        }
        try {
            readFully(in, buf.array());
            return true;
        } catch (EOFException e) {
            return canRetry && tryOpenNextChunk() && tryFill(buf, false);
        }
    }

    private boolean tryOpenNextChunk() {
        try {
            if (in != null) {
                in.close();
            }
            final File chunkFile = findNonemptyFile();
            if (chunkFile == null) {
                return false;
            }
            final FileInputStream fileIn = new FileInputStream(chunkFile);
            final boolean compressed = chunkFile.getName().endsWith(COMPRESSED_SUFFIX);
            in = compressed ? compressedInputStream(fileIn) : new BufferingInputStream(fileIn);
            rebuilder.startNewChunk(seq(chunkFile), compressed);
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @SuppressFBWarnings(value = "RV",
            justification = "Deleting an empty chunk file is non-essential behavior")
    private File findNonemptyFile() {
        if (chunkFiles == null) {
            return null;
        }
        while (chunkFiles.size() > 0) {
            final File f = chunkFiles.remove(chunkFiles.size() - 1);
            if (f.length() != 0) {
                return f;
            }
            f.delete();
        }
        return null;
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

    private static long seq(File f) {
        return Long.parseLong(f.getName().substring(0, CHUNK_FNAME_LENGTH), HEX_RADIX);
    }
}
