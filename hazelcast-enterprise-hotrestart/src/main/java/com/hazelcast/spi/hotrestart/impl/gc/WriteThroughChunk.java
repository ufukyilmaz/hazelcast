package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.HotRestartException;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.hazelcast.spi.hotrestart.impl.gc.GcHelper.bufferedOutputStream;

/**
 * A growing chunk which immediately writes added entries to the backing file.
 * <p>
 * Not thread-safe.
 */
public final class WriteThroughChunk extends GrowingChunk implements Closeable {
    private final FileOutputStream fileOut;
    private final DataOutputStream dataOut;
    private final GcHelper gcHelper;
    private long youngestSeq;

    WriteThroughChunk(long seq, RecordMap records, FileOutputStream out, GcHelper gcHelper) {
        super(seq, records);
        this.fileOut = out;
        this.gcHelper = gcHelper;
        this.dataOut = new DataOutputStream(bufferedOutputStream(out));
    }

    /**
     * Writes a new record to the chunk file and updates the chunk's size.
     * Called by the mutator thread.
     *
     * @return true if the chunk is now full.
     */
    public boolean addStep1(long keyPrefix, long recordSeq, boolean isTombstone, byte[] keyBytes, byte[] valueBytes) {
        if (full()) {
            throw new HotRestartException(String.format("Attempted to write to a full file #%03x", seq));
        }
        try {
            dataOut.writeLong(recordSeq);
            dataOut.writeLong(keyPrefix);
            dataOut.writeInt(keyBytes.length);
            dataOut.writeInt(isTombstone ? -1 : valueBytes.length);
            dataOut.write(keyBytes);
            dataOut.write(valueBytes);
            size += Record.HEADER_SIZE + keyBytes.length + valueBytes.length;
            youngestSeq = recordSeq;
            return full();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    @Override public void close() {
        if (dataOut == null) {
            return;
        }
        try {
            fsync();
            dataOut.close();
            gcHelper.changeSuffix(seq, Chunk.FNAME_SUFFIX + ACTIVE_CHUNK_SUFFIX, Chunk.FNAME_SUFFIX);
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }

    public void fsync() {
        try {
            dataOut.flush();
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
        fsync(fileOut);
    }

    StableChunk toStableChunk() {
        return new StableChunk(this, youngestSeq, false);
    }
}
