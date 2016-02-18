package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class TombFileAccessor implements Closeable {
    private final MappedByteBuffer buf;

    private long recordSeq;
    private long keyPrefix;
    // These three variables are updated on each call to loadAndCopyTombstone()
    private int recordSize;

    public TombFileAccessor(File tombFile) {
        try {
            final FileChannel chan = new FileInputStream(tombFile).getChannel();
            this.buf = chan.map(READ_ONLY, 0, chan.size());
            chan.close();
        } catch (IOException e) {
            throw new HotRestartException("Failed to create tombstone file accessor", e);
        }
    }

    public int loadAndCopyTombstone(int pos, ChunkFileOut out) throws IOException {
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
        ((sun.nio.ch.DirectBuffer) buf).cleaner().clean();
    }
}
