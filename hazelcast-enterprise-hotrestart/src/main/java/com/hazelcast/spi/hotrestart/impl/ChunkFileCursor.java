package com.hazelcast.spi.hotrestart.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.Rebuilder;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.spi.hotrestart.impl.HotRestarter.isActiveChunkFile;
import static com.hazelcast.spi.hotrestart.impl.HotRestarter.seq;
import static com.hazelcast.spi.hotrestart.impl.gc.Chunk.ACTIVE_CHUNK_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.Compressor.COMPRESSED_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.TOMB_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.VAL_HEADER_SIZE;

abstract class ChunkFileCursor {
    final Rebuilder rebuilder;
    final ByteBuffer headerBuf;

    byte[] key;
    byte[] value;
    long seq;
    long prefix;

    private final GcHelper gcHelper;
    private final List<File> chunkFiles;

    private File chunkFile;
    private long truncationPoint;
    private InputStream in;

    ChunkFileCursor(int headerSize, List<File> chunkFiles, GcHelper gcHelper, Rebuilder rebuilder) {
        this.gcHelper = gcHelper;
        this.rebuilder = rebuilder;
        this.chunkFiles = chunkFiles;
        this.headerBuf = ByteBuffer.allocate(headerSize);
    }

    final boolean advance() throws InterruptedException {
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (in == null && !tryOpenNextChunk()) {
                chunkFiles.clear();
                return false;
            }
            try {
                try {
                    headerBuf.clear();
                    if (readFullyOrNothing(in, headerBuf.array())) {
                        loadRecord();
                        truncationPoint += recordSize();
                        return true;
                    } else {
                        closeCurrentChunk();
                    }
                } catch (EOFException e) {
                    if (closeCurrentChunk()) {
                        removeBrokenTrailingRecord();
                    } else {
                        throw e;
                    }
                }
            } catch (IOException e) {
                throw new HotRestartException(e);
            }
        }
    }

    void loadRecord() throws IOException {
        seq = headerBuf.getLong();
        prefix = headerBuf.getLong();
        key = readBlock(headerBuf.getInt());
    }

    final int recordSize() {
        return headerBuf.capacity() + key.length + value.length;
    }

    final byte[] readBlock(int size) throws IOException {
        final byte[] buf = new byte[size];
        readFully(in, buf);
        return buf;
    }

    private boolean tryOpenNextChunk() {
        try {
            final File chunkFile = findNonemptyFile();
            if (chunkFile == null) {
                return false;
            }
            this.chunkFile = chunkFile;
            truncationPoint = 0;
            final FileInputStream fileIn = new FileInputStream(chunkFile);
            final boolean compressed = chunkFile.getName().endsWith(COMPRESSED_SUFFIX);
            in = compressed ? gcHelper.compressor.compressedInputStream(fileIn) : new BufferingInputStream(fileIn);
            rebuilder.startNewChunk(seq(chunkFile), compressed);
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    /** @return true if current chunk was an active chunk */
    private boolean closeCurrentChunk() throws IOException {
        in.close();
        final boolean active = isActiveChunkFile(chunkFile);
        if (active) {
            removeActiveSuffixFromChunkFile();
        }
        in = null;
        return active;
    }

    private File findNonemptyFile() {
        while (!chunkFiles.isEmpty()) {
            final File f = chunkFiles.remove(chunkFiles.size() - 1);
            if (f.length() != 0) {
                return f;
            }
            delete(f);
        }
        return null;
    }

    static boolean readFullyOrNothing(InputStream in, byte[] b) throws IOException {
        int bytesRead = 0;
        do {
            int count = in.read(b, bytesRead, b.length - bytesRead);
            if (count < 0) {
                if (bytesRead == 0) {
                    return false;
                }
                throw new EOFException();
            }
            bytesRead += count;
        } while (bytesRead < b.length);
        return true;
    }

    private static void readFully(InputStream in, byte[] b) throws IOException {
        if (!readFullyOrNothing(in, b)) {
            throw new EOFException();
        }
    }

    private void removeBrokenTrailingRecord() {
        try {
            final RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw");
            raf.setLength(truncationPoint);
            raf.getFD().sync();
            raf.close();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private void removeActiveSuffixFromChunkFile() {
        final String nameNow = chunkFile.getName();
        final String nameToBe = nameNow.substring(0, nameNow.length() - ACTIVE_CHUNK_SUFFIX.length());
        final File renamed = new File(chunkFile.getParent(), nameToBe);
        if (!chunkFile.renameTo(renamed)) {
            throw new HazelcastException("Failed to rename " + nameNow + " to " + nameToBe);
        }
        chunkFile = renamed;
    }

    static class ValChunkFileCursor extends ChunkFileCursor {
        ValChunkFileCursor(List<File> chunkFiles, Rebuilder rebuilder, GcHelper gcHelper) {
            super(VAL_HEADER_SIZE, chunkFiles, gcHelper, rebuilder);
        }

        @Override void loadRecord() throws IOException {
            super.loadRecord();
            value = readBlock(headerBuf.getInt());
        }
    }

    static class TombChunkFileCursor extends ChunkFileCursor {
        private static final byte[] TOMBSTONE_VALUE = new byte[0];

        TombChunkFileCursor(List<File> chunkFiles, Rebuilder rebuilder, GcHelper gcHelper) {
            super(TOMB_HEADER_SIZE, chunkFiles, gcHelper, rebuilder);
            this.value = TOMBSTONE_VALUE;
        }
    }
}
