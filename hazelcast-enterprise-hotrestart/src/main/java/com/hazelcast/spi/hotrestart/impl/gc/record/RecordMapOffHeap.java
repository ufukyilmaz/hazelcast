package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.elastic.map.hashslot.HashSlotArrayTwinKeyImpl;
import com.hazelcast.elastic.map.hashslot.HashSlotCursorTwinKey;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import static com.hazelcast.elastic.map.hashslot.HashSlotArrayTwinKeyImpl.valueAddr2slotBase;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.toRawSizeValue;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordOffHeap.TOMBSTONE_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordOffHeap.VALUE_RECORD_SIZE;

/**
 * Record map backed by off-heap storage.
 */
public final class RecordMapOffHeap implements RecordMap {
    private static final int DEFAULT_INITIAL_CAPACITY = 256;
    private final MemoryAllocator malloc;
    private final MemoryAllocator stableMalloc;
    private final boolean isTombstoneMap;

    private HashSlotArrayTwinKeyImpl hsa;
    private final RecordOffHeap rec = new RecordOffHeap();

    private RecordMapOffHeap(MemoryAllocator malloc, MemoryAllocator stableMalloc,
                             boolean isTombstoneMap, int initialCapacity) {
        this.malloc = malloc;
        this.stableMalloc = stableMalloc;
        this.isTombstoneMap = isTombstoneMap;
        this.hsa = new HashSlotArrayTwinKeyImpl(
                0L, malloc, isTombstoneMap ? TOMBSTONE_SIZE : VALUE_RECORD_SIZE, initialCapacity);
    }

    // Not a general-purpose copy constructor! Specialized for the toStable() method.
    private RecordMapOffHeap(RecordMapOffHeap growing) {
        this.malloc = growing.stableMalloc;
        this.stableMalloc = null;
        this.isTombstoneMap = growing.isTombstoneMap;
        this.hsa = new HashSlotArrayTwinKeyImpl(growing.hsa, malloc);
        growing.hsa.dispose();
    }

    public static RecordMapOffHeap newRecordMapOffHeap(MemoryAllocator malloc, MemoryAllocator stableMalloc) {
        return new RecordMapOffHeap(malloc, stableMalloc, false, DEFAULT_INITIAL_CAPACITY);
    }

    public static RecordMapOffHeap newTombstoneMapOffHeap(MemoryAllocator malloc) {
        return new RecordMapOffHeap(malloc, null, true, DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public Record putIfAbsent(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone, int additionalInt) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final long addr = hsa.ensure(ohk.address(), ohk.sequenceId());
        if (addr > 0) {
            rec.address = addr;
            if (!isTombstoneMap) {
                rec.setKeyPrefix(prefix);
            }
            rec.setRawSeqSize(seq, toRawSizeValue(size, isTombstone));
            rec.setAdditionalInt(additionalInt);
            return null;
        } else {
            rec.address = -addr;
            return rec;
        }
    }

    @Override public RecordOffHeap get(KeyHandle kh) {
        final KeyHandleOffHeap handle = (KeyHandleOffHeap) kh;
        final long addr = hsa.get(handle.address(), handle.sequenceId());
        if (addr == NULL_ADDRESS) {
            return null;
        }
        rec.address = addr;
        return rec;
    }

    @Override public int size() {
        return (int) hsa.size();
    }

    @Override public SortedBySeqRecordCursor sortedBySeqCursor(
            int liveRecordCount, RecordMap[] recordMaps, MutatorCatchup mc
    ) {
        final LongArray seqsAndSlotBases = new LongArray(malloc, 2 * liveRecordCount);
        final RecordOffHeap r = new RecordOffHeap();
        mc.catchupNow();
        int i = 0;
        for (RecordMap map : recordMaps) {
            for (HashSlotCursorTwinKey cursor = ((RecordMapOffHeap) map).hsa.cursor(); cursor.advance();) {
                r.address = cursor.valueAddress();
                if (r.isAlive()) {
                    seqsAndSlotBases.set(i++, r.liveSeq());
                    seqsAndSlotBases.set(i++, valueAddr2slotBase(r.address));
                }
                mc.catchupAsNeeded();
            }
        }
        return new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, i, malloc, mc);
    }

    @Override public CursorOffHeap cursor() {
        return new CursorOffHeap();
    }

    @Override public RecordMap toStable() {
        return stableMalloc != null ? new RecordMapOffHeap(this) : this;
    }

    @Override public void dispose() {
        hsa.dispose();
    }

    final class CursorOffHeap implements Cursor {
        private final HashSlotCursorTwinKey c = hsa.cursor();
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
    }
}
