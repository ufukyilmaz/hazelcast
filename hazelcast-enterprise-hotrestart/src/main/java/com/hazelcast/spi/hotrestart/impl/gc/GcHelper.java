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
import com.hazelcast.spi.hotrestart.InMemoryStoreRegistry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.spi.hotrestart.impl.HotRestartStoreImpl.compression;

/**
 * Contains common services needed across the hot restart codebase. Passed
 * around a lot due to the lack of proper DI support in current HZ.
 */
public final class GcHelper {
    /** Sequence number of records increases by amount proportional to record size.
     * This constant defines the proportion's ratio. */
    public static final long BYTES_PER_RECORD_SEQ_INCREMENT = 32;

    /** To optimize file access times, chunk files are distributed across
     * bucket directories. This is the maximum number of such directories.
     * INVARIANT: this number is a power of two. */
    public static final int MAX_BUCKET_DIRS = 0x1000;

    /** Bitmask used for the operation "modulo MAB_BUCKET_DIRS" */
    public static final int BUCKET_DIR_MASK = MAX_BUCKET_DIRS - 1;

    private static boolean ioDisabled;

    /** Hot Restart Store's home directory. */
    public final File homeDir;

    /** In-memory store registry used by this Hot Restart Store. */
    public final InMemoryStoreRegistry inMemoryStoreRegistry;

    /** Record Data Handler singleton, used by the GC process to
     * transfer data from the in-memory store. */
    public final RecordDataHolder recordDataHolder = new RecordDataHolder();

    private final AtomicLong chunkSeq = new AtomicLong();
    private volatile long recordSeq;

    public GcHelper(File homeDir, InMemoryStoreRegistry inMemoryStoreRegistry) {
        this.homeDir = homeDir;
        this.inMemoryStoreRegistry = inMemoryStoreRegistry;
    }

    public WriteThroughChunk newWriteThroughChunk() {
        final long seq = chunkSeq.incrementAndGet();
        return new WriteThroughChunk(seq, createFileChannel(seq, Chunk.FNAME_SUFFIX));
    }

    /** @return whether file I/O is disabled. Should return true only in testing. */
    public static boolean ioDisabled() {
        return ioDisabled;
    }

    public void initChunkSeq(long seq) {
        chunkSeq.set(seq);
    }

    public void initRecordSeq(long seq) {
        recordSeq = seq;
    }

    public long nextRecordSeq(long size) {
        return recordSeq += 1 + size / BYTES_PER_RECORD_SEQ_INCREMENT;
    }

    public long recordSeq() {
        return recordSeq;
    }

    GrowingDestChunk newDestChunk() {
        return new GrowingDestChunk(chunkSeq.incrementAndGet(), this);
    }

    public void deleteFile(Chunk chunk) {
        if (ioDisabled()) {
            return;
        }
        final File toDelete = chunkFile(chunk, false);
        if (!toDelete.delete()) {
            System.err.println("Failed to delete " + toDelete);
        }
    }

    void changeSuffix(long seq, String suffixNow, String targetSuffix) {
        if (ioDisabled()) {
            return;
        }
        final File nameNow = chunkFile(seq, suffixNow, false);
        final File nameToBe = chunkFile(seq, targetSuffix, false);
        if (!nameNow.renameTo(nameToBe)) {
            System.err.println("Failed to rename " + nameNow + " to " + nameToBe);
        }
    }

    FileChannel createFileChannel(long seq, String suffix) {
        return createFileChannel(chunkFile(seq, suffix, true));
    }

    FileOutputStream createFileOutputStream(long seq, String suffix) {
        return createFileOutputStream(chunkFile(seq, suffix, true));
    }

    static FileChannel createFileChannel(File f) {
        final FileOutputStream out = createFileOutputStream(f);
        return out == null ? null : out.getChannel();
    }

    static FileOutputStream createFileOutputStream(File f) {
        if (ioDisabled()) {
            return null;
        }
        try {
            return new FileOutputStream(f);
        } catch (FileNotFoundException e) {
            throw new HotRestartException(e);
        }
    }

    static FileInputStream createFileInputStream(File f) {
        if (ioDisabled()) {
            return null;
        }
        try {
            return new FileInputStream(f);
        } catch (FileNotFoundException e) {
            throw new HotRestartException(e);
        }
    }

    static String newStableChunkSuffix() {
        return Chunk.FNAME_SUFFIX + (compression ? Compression.COMPRESSED_SUFFIX : "");
    }

    File chunkFile(Chunk chunk, boolean mkdirs) {
        return chunkFile(chunk.seq, chunk.fnameSuffix(), mkdirs);
    }

    File chunkFile(long seq, String suffix, boolean mkdirs) {
        final String bucketDirname = String.format("%03x", seq & BUCKET_DIR_MASK);
        final String chunkFilename = String.format("%016x%s", seq, suffix);
        final File bucketDir = new File(homeDir, bucketDirname);
        if (mkdirs && !bucketDir.isDirectory() && !bucketDir.mkdirs()) {
            throw new HotRestartException("Cannot create chunk bucket directory " + bucketDir);
        }
        return new File(bucketDir, chunkFilename);
    }
}
