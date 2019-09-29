package com.hazelcast.internal.hotrestart.impl.gc.tracker;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Flyweight object around a pointer to a native-memory Tracker structure.
 */
final class TrackerOffHeap extends Tracker {
    static final int SIZE = 0x10;
    private static final int CHUNK_SEQ_OFFSET = 0x0;
    private static final int GARBAGE_COUNT_OFFSET = 0x8;

    long address = NULL_ADDRESS;

    @Override
    public long rawChunkSeq() {
        return AMEM.getLong(address + CHUNK_SEQ_OFFSET);
    }

    @Override
    public void setRawChunkSeq(long rawChunkSeqValue) {
        AMEM.putLong(address + CHUNK_SEQ_OFFSET, rawChunkSeqValue);
    }

    @Override
    public long garbageCount() {
        return AMEM.getLong(address + GARBAGE_COUNT_OFFSET);
    }

    @Override
    public void setGarbageCount(long garbageCount) {
        AMEM.putLong(address + GARBAGE_COUNT_OFFSET, garbageCount);
    }

    @Override
    public String toString() {
        if (address == NULL_ADDRESS) {
            return "(NULL)";
        }
        return "(" + chunkSeq() + ',' + isTombstone() + ',' + garbageCount() + ')';
    }
}
