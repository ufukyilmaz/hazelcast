package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.elastic.LongArray;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotCursor16byteKey;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl;
import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.KeyHandleOffHeap;
import com.hazelcast.internal.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.internal.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;

import static com.hazelcast.internal.elastic.CapacityUtil.DEFAULT_LOAD_FACTOR;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.valueAddr2slotBase;
import static com.hazelcast.internal.hotrestart.impl.gc.record.Record.toRawSizeValue;
import static com.hazelcast.internal.hotrestart.impl.gc.record.RecordOffHeap.TOMBSTONE_SIZE;
import static com.hazelcast.internal.hotrestart.impl.gc.record.RecordOffHeap.VALUE_RECORD_SIZE;

/**
 * Record map backed by off-heap storage.
 */
public final class RecordMapOffHeap implements RecordMap {
    private static final int DEFAULT_INITIAL_CAPACITY = 256;
    private final MemoryManager memMgr;
    private final MemoryManager stableMemMgr;
    private final boolean isTombstoneMap;

    private HashSlotArray16byteKeyImpl hsa;
    private final RecordOffHeap rec = new RecordOffHeap();

    private RecordMapOffHeap(MemoryManager memMgr, MemoryManager stableMemMgr,
                             boolean isTombstoneMap, int initialCapacity) {
        this.memMgr = memMgr;
        this.stableMemMgr = stableMemMgr;
        this.isTombstoneMap = isTombstoneMap;
        this.hsa = new HashSlotArray16byteKeyImpl(0L, memMgr,
                isTombstoneMap ? TOMBSTONE_SIZE : VALUE_RECORD_SIZE, initialCapacity, DEFAULT_LOAD_FACTOR);
        hsa.gotoNew();
    }

    // Not a general-purpose copy constructor! Specialized for the toStable() method.
    private RecordMapOffHeap(RecordMapOffHeap growing) {
        this.memMgr = growing.stableMemMgr;
        this.stableMemMgr = null;
        this.isTombstoneMap = growing.isTombstoneMap;
        this.hsa = growing.hsa;
        hsa.migrateTo(memMgr.getAllocator());
    }

    public static RecordMapOffHeap newRecordMapOffHeap(MemoryManager memMgr, MemoryManager stableMemMgr) {
        return new RecordMapOffHeap(memMgr, stableMemMgr, false, DEFAULT_INITIAL_CAPACITY);
    }

    public static RecordMapOffHeap newTombstoneMapOffHeap(MemoryManager memMgr) {
        return new RecordMapOffHeap(memMgr, null, true, DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public Record putIfAbsent(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone, int additionalInt) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final SlotAssignmentResult slot = hsa.ensure(ohk.address(), ohk.sequenceId());
        rec.address = slot.address();
        if (!slot.isNew()) {
            return rec;
        }
        if (!isTombstoneMap) {
            rec.setKeyPrefix(prefix);
        }
        rec.setRawSeqSize(seq, toRawSizeValue(size, isTombstone));
        rec.setAdditionalInt(additionalInt);
        return null;
    }

    @Override
    public RecordOffHeap get(KeyHandle kh) {
        final KeyHandleOffHeap handle = (KeyHandleOffHeap) kh;
        final long addr = hsa.get(handle.address(), handle.sequenceId());
        if (addr == NULL_ADDRESS) {
            return null;
        }
        rec.address = addr;
        return rec;
    }

    @Override
    public int size() {
        return (int) hsa.size();
    }

    @Override
    public SortedBySeqRecordCursor sortedBySeqCursor(
            int liveRecordCount, RecordMap[] recordMaps, MutatorCatchup mc
    ) {
        final LongArray seqsAndSlotBases = new LongArray(memMgr, 2L * liveRecordCount);
        final RecordOffHeap r = new RecordOffHeap();
        mc.catchupNow();
        int i = 0;
        for (RecordMap map : recordMaps) {
            for (HashSlotCursor16byteKey cursor = ((RecordMapOffHeap) map).hsa.cursor(); cursor.advance();) {
                r.address = cursor.valueAddress();
                if (r.isAlive()) {
                    seqsAndSlotBases.set(i++, r.liveSeq());
                    seqsAndSlotBases.set(i++, valueAddr2slotBase(r.address));
                }
                mc.catchupAsNeeded();
            }
        }
        return new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, i, memMgr, mc);
    }

    @Override
    public CursorOffHeap cursor() {
        return new CursorOffHeap();
    }

    @Override
    public RecordMap toStable() {
        return stableMemMgr != null ? new RecordMapOffHeap(this) : this;
    }

    @Override
    public void dispose() {
        hsa.dispose();
    }

    final class CursorOffHeap implements Cursor {
        private final HashSlotCursor16byteKey c = hsa.cursor();
        private final RecordOffHeap r = new RecordOffHeap();

        @Override
        public boolean advance() {
            if (!c.advance()) {
                return false;
            }
            r.address = c.valueAddress();
            return true;
        }

        @Override
        public KeyHandleOffHeap toKeyHandle() {
            return new SimpleHandleOffHeap(c.key1(), c.key2());
        }

        @Override
        public Record asRecord() {
            return r;
        }
    }
}
