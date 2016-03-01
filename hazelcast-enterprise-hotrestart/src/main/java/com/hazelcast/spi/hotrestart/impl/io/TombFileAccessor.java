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

public final class TombFileAccessor implements Closeable {
    private MappedByteBuffer buf;

    private long recordSeq;
    private long keyPrefix;
    // These three variables are updated on each call to loadAndCopyTombstone()
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

    public long recordSeq() {
        return recordSeq;
    }

    public long keyPrefix() {
        return keyPrefix;
    }

    public int recordSize() {
        return recordSize;
    }

    public void close() {
        if (buf != null) {
            GcHelper.disposeMappedBuffer(buf);
        }
        buf = null;
    }

}
