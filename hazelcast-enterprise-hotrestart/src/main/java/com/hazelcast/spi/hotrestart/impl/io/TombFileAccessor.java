package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class TombFileAccessor implements Closeable {
    private final FileChannel chan;
    private final MappedByteBuffer buf;

    // These three variables are updated on each call to loadAndCopyTombstone()
    private long recordSeq;
    private long keyPrefix;
    private int recordSize;

    public TombFileAccessor(File tombFile) {
        try {
            this.chan = new RandomAccessFile(tombFile, "rw").getChannel();
            this.buf = chan.map(READ_ONLY, 0, chan.size());
        } catch (IOException e) {
            throw new HotRestartException("Failed to create tombstone file accessor", e);
        }
    }

    public int loadAndCopyTombstone(int pos, DataOutputStream out) throws IOException {
        buf.position(pos);
        recordSeq = buf.getLong();
        keyPrefix = buf.getLong();
        int keySize = buf.getInt();
        out.writeLong(recordSeq);
        out.writeLong(keyPrefix);
        out.writeInt(keySize);
        for (int i = 0; i < keySize; i++) {
            out.write(buf.get());
        }
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
        try {
            chan.close();
        } catch (IOException e) {
            throw new HotRestartException("Failed to close tombstone file accessor", e);
        }
    }
}
