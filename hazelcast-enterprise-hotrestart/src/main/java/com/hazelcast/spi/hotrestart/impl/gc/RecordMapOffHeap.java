package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray16byteKey;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl;
import com.hazelcast.internal.util.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.spi.hotrestart.impl.gc.Record.toRawSizeValue;

/**
 * Record map backed by off-heap storage.
 */
final class RecordMapOffHeap implements RecordMap {
    private static final int DEFAULT_INITIAL_CAPACITY = 256;

    private HashSlotArray16byteKey records;
    private final RecordOffHeap rec = new RecordOffHeap();

    public RecordMapOffHeap(MemoryManager memMgr, int initialCapacity) {
        this.records = new HashSlotArray16byteKeyImpl(
                0L, memMgr, RecordOffHeap.SIZE, initialCapacity, DEFAULT_LOAD_FACTOR);
        records.gotoNew();
    }

    RecordMapOffHeap(MemoryManager memMgr, RecordMap gcRecordMap) {
        this(memMgr, gcRecordMap.size());
        for (Cursor cur = gcRecordMap.cursor(); cur.advance();) {
            final Record r = cur.asRecord();
            if (!r.isAlive() && r.garbageCount() == 0) {
                continue;
            }
            final KeyHandleOffHeap ohk = (KeyHandleOffHeap) cur.toKeyHandle();
            rec.address = records.ensure(ohk.address(), ohk.sequenceId());
            rec.setKeyPrefix(r.keyPrefix(null));
            rec.setRawSeqSize(r.rawSeqValue(), r.rawSizeValue());
            rec.setGarbageCount(r.garbageCount());
        }
    }

    public RecordMapOffHeap(MemoryManager memMgr) {
        this(memMgr, DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public Record putIfAbsent(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone, int garbageCount) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final long addr = records.ensure(ohk.address(), ohk.sequenceId());
        if (addr > 0) {
            rec.address = addr;
            rec.setKeyPrefix(prefix);
            rec.setRawSeqSize(seq, toRawSizeValue(size, isTombstone));
            rec.setGarbageCount(garbageCount);
            return null;
        } else {
            rec.address = -addr;
            return rec;
        }
    }

    @Override public RecordOffHeap get(KeyHandle kh) {
        final KeyHandleOffHeap handle = (KeyHandleOffHeap) kh;
        final long addr = records.get(handle.address(), handle.sequenceId());
        if (addr == NULL_ADDRESS) {
            return null;
        }
        rec.address = addr;
        return rec;
    }

    @Override public int size() {
        return (int) records.size();
    }

    @Override public Cursor cursor() {
        return new CursorOffHeap();
    }

    @Override public void dispose() {
        records.dispose();
    }

    private final class CursorOffHeap implements Cursor {
        private final HashSlotCursor16byteKey c = records.cursor();
        private final RecordOffHeap r = new RecordOffHeap();

        @Override public boolean advance() {
            if (!c.advance()) {
                return false;
            }
            r.address = c.valueAddress();
            return true;
        }

        @Override public KeyHandleOffHeap toKeyHandle() {
            return new SimpleHandleOffHeap(c.key1(), c.key2());
        }

        @Override public Record asRecord() {
            return r;
        }

        @Override public GcRecord toGcRecord(long chunkSeq) {
            return new GcRecord.WithOffHeapHandle(r, chunkSeq, c.key1(), c.key2());
        }
    }
}
