package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.KeyOnHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.gc.chunk.StableValChunk;

/**
 * Holds additional data needed during GC cycle.
 */
public abstract class GcRecord extends RecordOnHeap {
    private RecordMap ownerRecordMap;

    public GcRecord(Record r, StableValChunk chunk) {
        super(r);
        this.ownerRecordMap = chunk.records;
        setGarbageCount(0);
    }

    @Override public boolean isAlive() {
        return ownerRecordMap != null ? ownerRecordMap.get(toKeyHandle()).isAlive() : super.isAlive();
    }

    /**
     * After the record is moved to the destination chunk, its default {@code isAlive()} method
     * starts behaving correctly. This method reroutes future {@code isAlive()} calls to the default
     * implementation.
     */
    public void movedToDestChunk() {
        ownerRecordMap = null;
    }

    public abstract KeyHandle toKeyHandle();

    /** GC record used with on-heap Hot Restart store */
    public static final class WithHeapHandle extends GcRecord {
        private final KeyOnHeap key;

        public WithHeapHandle(Record r, StableValChunk chunk, KeyOnHeap key) {
            super(r, chunk);
            this.key = key;
        }

        @Override public long keyPrefix(KeyHandle ignored) {
            return key.prefix();
        }

        @Override public KeyHandle toKeyHandle() {
            return key;
        }
    }

    /** GC record used with off-heap Hot Restart store */
    public static final class WithOffHeapHandle extends GcRecord {
        private final long prefix;
        private final KeyHandle keyHandle;

        public WithOffHeapHandle(RecordOffHeap r, StableValChunk chunkSeq, long address, long sequenceId) {
            super(r, chunkSeq);
            this.prefix = r.keyPrefix(null);
            this.keyHandle = new SimpleHandleOffHeap(address, sequenceId);
        }

        @Override public long keyPrefix(KeyHandle ignored) {
            return prefix;
        }

        @Override public KeyHandle toKeyHandle() {
            return keyHandle;
        }
    }
}
