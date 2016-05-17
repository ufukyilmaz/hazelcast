package com.hazelcast.spi.hotrestart.impl.gc.chunk;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.gc.GcHelper;
import com.hazelcast.spi.hotrestart.impl.gc.record.Record;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.spi.hotrestart.impl.io.ChunkFileOut;

/**
 * Write-through chunk specialized to contain value records. Used as the active value chunk.
 */
public final class ActiveValChunk extends WriteThroughChunk implements ActiveChunk {

    public ActiveValChunk(long seq, String suffix, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, suffix, records, out, gcHelper);
    }

    /**
     * Writes a new record to the chunk file and updates the chunk's size.
     * May be called by the mutator thread.
     *
     * @return true if the chunk is now full.
     */
    public boolean addStep1(long recordSeq, long keyPrefix, byte[] keyBytes, byte[] valueBytes) {
        assert hasRoom();
        out.writeValueRecord(recordSeq, keyPrefix, keyBytes, valueBytes);
        grow(Record.VAL_HEADER_SIZE + keyBytes.length + valueBytes.length);
        return full();
    }

    @Override
    public StableValChunk toStableChunk() {
        return new StableValChunk(this);
    }

    @Override
    public void insertOrUpdate(long prefix, KeyHandle kh, long seq, int ignored, int size) {
        insertOrUpdateValue(prefix, kh, seq, size);
    }

    @Override
    protected int determineSizeLimit() {
        return valChunkSizeLimit();
    }
}
