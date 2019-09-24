package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.internal.util.BufferingInputStream;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.spi.hotrestart.HotRestartException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.IOUtil.readFully;
import static com.hazelcast.internal.nio.IOUtil.readFullyOrNothing;
import static com.hazelcast.spi.hotrestart.impl.HotRestarter.BUFFER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.TOMB_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.VAL_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.isActiveChunkFile;
import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.seq;

/**
 * A cursor over a chunk file's contents. Common base class for tombstone and value chunk
 * specializations, which are nested inside it.
 */
public abstract class ChunkFileCursor implements ChunkFileRecord {

    final ByteBuffer headerBuf;
    byte[] key;
    int truncationPoint;
    private final File chunkFile;
    private final long chunkSeq;
    private final InputStream in;
    private long seq;
    private long prefix;

    public ChunkFileCursor(int headerSize, File chunkFile) {
        this.chunkFile = chunkFile;
        this.headerBuf = ByteBuffer.allocate(headerSize);
        this.chunkSeq = seq(chunkFile);
        try {
            final FileInputStream fileIn = new FileInputStream(chunkFile);
            this.in = new BufferingInputStream(fileIn, BUFFER_SIZE);
        } catch (FileNotFoundException e) {
            throw new HotRestartException("Failed to open chunk file " + chunkFile);
        }
    }

    /**
     * Advances to the next record in the chunk file. If this was an active chunk file at the time
     * of shutdown, and if a broken record is found (unable to read the amount of data indicated by
     * the header), the file is automatically truncated to the last intact record.
     * @return whether there was a next record to advance to
     */
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

    public final void close() {
        try {
            in.close();
        } catch (IOException e) {
            throw new HotRestartException("Failed to close chunk file " + chunkFile, e);
        }
        // close() must be idempotent, therefore check that the file still exists
        if (isActiveChunkFile(chunkFile) && chunkFile.exists()) {
            ChunkFilesetCursor.removeActiveSuffix(chunkFile);
        }
    }

    // Implementation of ChunkFileRecord

    @Override
    public final long chunkSeq() {
        return chunkSeq;
    }

    @Override
    public final long recordSeq() {
        return seq;
    }

    @Override
    public final long prefix() {
        return prefix;
    }

    @Override
    public int size() {
        return headerBuf.capacity() + key.length;
    }

    @SuppressFBWarnings(value = "EI", justification = "Returned array is never modified")
    @Override
    public byte[] key() {
        return key;
    }

    // Begin private API

    abstract void loadRecord() throws IOException;

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
        RandomAccessFile raf = null;
        try {
            in.close();
            raf = new RandomAccessFile(chunkFile, "rw");
            raf.setLength(truncationPoint);
            raf.getFD().sync();
            return true;
        } catch (IOException e) {
            throw new HotRestartException(e);
        } finally {
            IOUtil.closeResource(raf);
        }
    }

    /**
     * Specialization of {@code ChunkFileCursor} to tombstone chunk.
     */
    public static final class Tomb extends ChunkFileCursor {
        public Tomb(File chunkFile) {
            super(TOMB_HEADER_SIZE, chunkFile);
        }

        @Override
        void loadRecord() throws IOException {
            loadCommonHeader();
            key = readPayload(headerBuf.getInt());
        }

        @Override
        public int filePos() {
            return truncationPoint - size();
        }

        @Override
        public byte[] value() {
            return null;
        }
    }

    /**
     * Specialization of {@code ChunkFileCursor} to value chunk.
     */
    static final class Val extends ChunkFileCursor {
        private byte[] value;

        Val(File chunkFile) {
            super(VAL_HEADER_SIZE, chunkFile);
        }

        @Override
        void loadRecord() throws IOException {
            loadCommonHeader();
            final int keySize = headerBuf.getInt();
            final int valueSize = headerBuf.getInt();
            key = readPayload(keySize);
            value = readPayload(valueSize);
        }

        @Override
        public byte[] value() {
            return value;
        }

        @Override
        public int filePos() {
            return 0;
        }

        @Override
        public int size() {
            return super.size() + value.length;
        }
    }
}
