package com.hazelcast.spi.hotrestart.impl.gc;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.nio.UnsafeHelper.UNSAFE;

/**
 * Flyweight object around a pointer to a native-memory Tracker structure.
 */
final class TrackerOffHeap extends Tracker {
    static final int SIZE = 0x10;
    private static final int CHUNK_SEQ_OFFSET = 0x0;
    private static final int GARBAGE_COUNT_OFFSET = 0x8;

    long address = NULL_ADDRESS;

    @Override long rawChunkSeq() {
        return UNSAFE.getLong(address + CHUNK_SEQ_OFFSET);
    }

    @Override void setRawChunkSeq(long rawChunkSeqValue) {
        UNSAFE.putLong(address + CHUNK_SEQ_OFFSET, rawChunkSeqValue);
    }

    @Override long garbageCount() {
        return UNSAFE.getLong(address + GARBAGE_COUNT_OFFSET);
    }

    @Override void setGarbageCount(long garbageCount) {
        UNSAFE.putLong(address + GARBAGE_COUNT_OFFSET, garbageCount);
    }

    @Override public String toString() {
        if (address == NULL_ADDRESS) {
            return "(NULL)";
        }
        return "(" + chunkSeq() + ',' + isTombstone() + ',' + garbageCount() + ')';
    }
}
