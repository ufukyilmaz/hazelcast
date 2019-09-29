package com.hazelcast.internal.hotrestart.impl.gc.chunk;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.gc.GcHelper;
import com.hazelcast.internal.hotrestart.impl.gc.record.Record;
import com.hazelcast.internal.hotrestart.impl.gc.record.RecordMap;
import com.hazelcast.internal.hotrestart.impl.io.ChunkFileOut;

/**
 * Represents the currently active value chunk.
 */
// class non-final for mockability
public class ActiveValChunk extends WriteThroughChunk implements ActiveChunk {

    public ActiveValChunk(long seq, RecordMap records, ChunkFileOut out, GcHelper gcHelper) {
        super(seq, ACTIVE_FNAME_SUFFIX, records, out, gcHelper);
    }

    @Override
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
    public void insertOrUpdate(long recordSeq, long keyPrefix, KeyHandle kh, int ignored, int size) {
        insertOrUpdateValue(recordSeq, keyPrefix, kh, size);
    }

    @Override
    protected int determineSizeLimit() {
        return valChunkSizeLimit();
    }
}
