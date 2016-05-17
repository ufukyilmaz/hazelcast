package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/**
 * Read-only accessor of data inside a tombstone chunk file. Uses a {@code MappedByteBuffer}.
 */
public final class TombFileAccessor implements Closeable {
    private MappedByteBuffer buf;

    // These three variables are updated on each call to loadAndCopyTombstone()
    private long recordSeq;
    private long keyPrefix;
    private int recordSize;

    public TombFileAccessor(File tombFile) {
        try {
            final FileChannel chan = new FileInputStream(tombFile).getChannel();
            long size = chan.size();
            this.buf = size > 0 ? chan.map(READ_ONLY, 0, size) : null;
            chan.close();
        } catch (IOException e) {
            throw new HotRestartException("Failed to create tombstone file accessor", e);
        }
    }

    /**
     * Loads the header data of the tombstone at {@code pos} into the cursor object and copies the tombstone into
     * the supplied {@code ChunkFileOut}.
     * @param pos position (file offset) of the tombstone to load and copy
     * @param out destination for the tombstone data
     * @return the size of the tombstone record
     * @throws IOException if an IO operation fails
     */
    public int loadAndCopyTombstone(int pos, ChunkFileOut out) throws IOException {
        assert buf != null : "tombstone chunk is empty or accessor has been closed";
        buf.position(pos);
        recordSeq = buf.getLong();
        keyPrefix = buf.getLong();
        int keySize = buf.getInt();
        out.writeTombstone(recordSeq, keyPrefix, buf, keySize);
        recordSize = buf.position() - pos;
        return recordSize;
    }

    /** @return record seq of the most recently loaded tombstone */
    public long recordSeq() {
        return recordSeq;
    }

    /** @return key prefix of the most recently loaded tombstone */
    public long keyPrefix() {
        return keyPrefix;
    }

    /** @return size of the most recently loaded tombstone */
    public int recordSize() {
        return recordSize;
    }

    /**
     * Disposes the underlying {@code MappedByteBuffer}.
     */
    public void close() {
        if (buf != null) {
            GcHelper.disposeMappedBuffer(buf);
        }
        buf = null;
    }

}
