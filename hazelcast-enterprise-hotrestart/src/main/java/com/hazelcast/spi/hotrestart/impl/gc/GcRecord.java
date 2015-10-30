package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

/**
 * Holds additional data needed during GC cycle.
 */
abstract class GcRecord extends RecordOnHeap {
    final long chunkSeq;

    GcRecord(Record r, long chunkSeq) {
        super(r);
        this.chunkSeq = chunkSeq;
        setGarbageCount(0);
    }

    abstract KeyHandle toKeyHandle();

    /** GC record used with on-heap Hot Restart store */
    static final class WithHeapHandle extends GcRecord {
        private final KeyHandle keyHandle;

        WithHeapHandle(Record r, long chunkSeq, KeyHandle keyHandle) {
            super(r, chunkSeq);
            this.keyHandle = keyHandle;
        }

        @Override KeyHandle toKeyHandle() {
            return keyHandle;
        }
    }

    /** GC record used with off-heap Hot Restart store */
    static final class WithOffHeapHandle extends GcRecord {
        private final long address;
        private final long sequenceId;
        private final long prefix;

        WithOffHeapHandle(RecordOffHeap r, long chunkSeq, long address, long sequenceId) {
            super(r, chunkSeq);
            this.prefix = r.keyPrefix(null);
            this.address = address;
            this.sequenceId = sequenceId;
        }

        @Override long keyPrefix(KeyHandle ignored) {
            return prefix;
        }

        @Override KeyHandle toKeyHandle() {
            return new SimpleHandleOffHeap(address, sequenceId);
        }
    }
}
