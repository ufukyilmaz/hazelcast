package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Represents a chunk file which is still growing.
 */
public abstract class GrowingChunk extends Chunk {
    public long size;

    protected GrowingChunk(long seq, RecordMap records) {
        super(seq, records);
    }

    /**
     * Updates this chunk's size by adding the supplied record size.
     * May be called by the mutator thread.
     *
     * @return true if this chunk has now reached capacity
     * @throws HotRestartException if attempting to add a record to a full chunk
     */
    public final boolean addStep1(int recordSize) {
        if (full()) {
            throw new HotRestartException(
                    String.format("Attempted to add record to a full chunk (no. %03x, size %d)", seq, size));
        }
        this.size += recordSize;
        return full();
    }

    /**
     * Adds the record to this chunk's RAM-based index of records. Called by the collector thread.
     */
    public void addStep2(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone) {
        final Record existing = records.putIfAbsent(prefix, kh, seq, size, isTombstone, 0);
        if (existing != null) {
            existing.update(seq, size, isTombstone);
        }
        liveRecordCount++;
    }

    @Override public final long size() {
        return size;
    }

    public boolean full() {
        return size() >= VAL_SIZE_LIMIT;
    }

    public static void fsync(FileOutputStream out) {
        try {
            if (out != null) {
                out.getFD().sync();
            }
        } catch (IOException e) {
            throw new HotRestartException(e);
        }
    }
}
