package com.hazelcast.spi.hotrestart.impl.io;

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

import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.TOMB_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.VAL_HEADER_SIZE;
import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.isActiveChunkFile;
import static com.hazelcast.spi.hotrestart.impl.io.ChunkFilesetCursor.seq;

/**
 * A cursor over a chunk file's contents.
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
            this.in = new BufferingInputStream(fileIn);
        } catch (FileNotFoundException e) {
            throw new HotRestartException("Failed to open chunk file " + chunkFile);
        }
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

    /**
     * @return Number of bytes the current record occupies in this chunk file.
     */
    @Override
    public int size() {
        return headerBuf.capacity() + key.length;
    }

    @SuppressFBWarnings(value = "EI", justification = "Returned array is never modified")
    @Override
    public byte[] key() {
        return key;
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

    public static boolean readFullyOrNothing(InputStream in, byte[] b) throws IOException {
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
