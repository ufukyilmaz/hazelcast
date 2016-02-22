package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.BufferingInputStream;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.isActiveChunkFile;
import static com.hazelcast.spi.hotrestart.impl.io.Compressor.COMPRESSED_SUFFIX;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.TOMB_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.VAL_HEADER_SIZE;

/**
 * A cursor over a chunk file's contents.
 */
public abstract class ChunkFileCursor implements ChunkFileRecord {
    final ByteBuffer headerBuf;
    byte[] key;
    private final File chunkFile;
    private final InputStream in;
    private long seq;
    private long prefix;
    private long truncationPoint;

    public ChunkFileCursor(int headerSize, File chunkFile, GcHelper gcHelper) {
        this.chunkFile = chunkFile;
        this.headerBuf = ByteBuffer.allocate(headerSize);
        try {
            final FileInputStream fileIn = new FileInputStream(chunkFile);
            this.in = isCompressed(chunkFile)
                    ? gcHelper.compressor.compressedInputStream(fileIn)
                    : new BufferingInputStream(fileIn);
        } catch (FileNotFoundException e) {
            throw new HotRestartException("Failed to open chunk file " + chunkFile);
        }
    }

    public final boolean isCompressed() {
        return isCompressed(chunkFile);
    }

    public final boolean advance() {
        try {
            try {
                headerBuf.clear();
                if (readFullyOrNothing(in, headerBuf.array())) {
                    loadRecord();
                    truncationPoint += size();
                    return true;
                }
                return false;
            } catch (EOFException e) {
                if (removeBrokenTailOfActiveFile()) {
                    return false;
                } else {
                    throw e;
                }
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }


    @Override public final long recordSeq() {
        return seq;
    }

    @Override public final long prefix() {
        return prefix;
    }

    /**
     * @return Number of bytes the current record occupies in this chunk file.
     */
    @Override public int size() {
        return headerBuf.capacity() + key.length;
    }

    @Override public byte[] key() {
        return key;
    }

    @Override public byte[] value() {
        throw new UnsupportedOperationException("value");
    }

    public final void close() {
        try {
            in.close();
        } catch (IOException e) {
            throw new HotRestartException("Failed to close chunk file " + chunkFile, e);
        }
        if (isActiveChunkFile(chunkFile)) {
            ChunkFilesetCursor.removeActiveSuffix(chunkFile);
        }
    }

    abstract void loadRecord() throws IOException;

    // Begin private API


    void loadCommonHeader() throws IOException {
        seq = headerBuf.getLong();
        prefix = headerBuf.getLong();
    }


    final byte[] readPayload(int size) throws EOFException {
        final byte[] payload = new byte[size];
        try {
            readFully(in, payload);
            return payload;
        } catch (EOFException e) {
            throw e;
        } catch (IOException e) {
            throw new HotRestartException("Failed to read payload", e);
        }
    }

    private boolean removeBrokenTailOfActiveFile() {
        if (!isActiveChunkFile(chunkFile)) {
            return false;
        }
        try {
            in.close();
            final RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw");
            raf.setLength(truncationPoint);
            raf.getFD().sync();
            raf.close();
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    private static boolean isCompressed(File chunkFile) {
        return chunkFile.getName().endsWith(COMPRESSED_SUFFIX);
    }

    private static boolean readFullyOrNothing(InputStream in, byte[] b) throws IOException {
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

    public static final class Tomb extends ChunkFileCursor {
        public Tomb(File chunkFile, GcHelper gcHelper) {
            super(TOMB_HEADER_SIZE, chunkFile, gcHelper);
        }

        @Override void loadRecord() throws IOException {
            loadCommonHeader();
            key = readPayload(headerBuf.getInt());
        }
    }

    static final class Val extends ChunkFileCursor {
        private byte[] value;

        Val(File chunkFile, GcHelper gcHelper) {
            super(VAL_HEADER_SIZE, chunkFile, gcHelper);
        }

        @Override void loadRecord() throws IOException {
            loadCommonHeader();
            final int keySize = headerBuf.getInt();
            final int valueSize = headerBuf.getInt();
            key = readPayload(keySize);
            value = readPayload(valueSize);
        }

        @Override public byte[] value() {
            return value;
        }

        @Override public int size() {
            return super.size() + value.length;
        }
    }
}
