package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

/**
 * Holds additional data needed during GC cycle.
 */
public abstract class GcRecord extends RecordOnHeap {
    public final long chunkSeq;

    public GcRecord(Record r, long chunkSeq) {
        super(r);
        this.chunkSeq = chunkSeq;
        setGarbageCount(0);
    }

    public abstract KeyHandle toKeyHandle();

    /** GC record used with on-heap Hot Restart store */
    public static final class WithHeapHandle extends GcRecord {
        private final KeyHandle keyHandle;

        public WithHeapHandle(Record r, long chunkSeq, KeyHandle keyHandle) {
            super(r, chunkSeq);
            this.keyHandle = keyHandle;
        }

        @Override public KeyHandle toKeyHandle() {
            return keyHandle;
        }
    }

    /** GC record used with off-heap Hot Restart store */
    public static final class WithOffHeapHandle extends GcRecord {
        private final long address;
        private final long sequenceId;
        private final long prefix;

        public WithOffHeapHandle(RecordOffHeap r, long chunkSeq, long address, long sequenceId) {
            super(r, chunkSeq);
            this.prefix = r.keyPrefix(null);
            this.address = address;
            this.sequenceId = sequenceId;
        }

        @Override public long keyPrefix(KeyHandle ignored) {
            return prefix;
        }

        @Override public KeyHandle toKeyHandle() {
            return new SimpleHandleOffHeap(address, sequenceId);
        }
    }
}
