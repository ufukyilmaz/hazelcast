package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.elastic.map.HashSlotArray;
import com.hazelcast.elastic.map.HashSlotArrayImpl;
import com.hazelcast.elastic.map.HashSlotCursor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SimpleHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import static com.hazelcast.elastic.map.HashSlotArrayImpl.valueAddr2slotBase;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.spi.hotrestart.impl.gc.record.Record.toRawSizeValue;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordOffHeap.TOMBSTONE_SIZE;
import static com.hazelcast.spi.hotrestart.impl.gc.record.RecordOffHeap.VALUE_RECORD_SIZE;

/**
 * Record map backed by off-heap storage.
 */
public final class RecordMapOffHeap implements RecordMap {
    private static final float LOAD_FACTOR = 0.6f;
    private static final int DEFAULT_INITIAL_CAPACITY = 256;
    private final MemoryAllocator malloc;
    private final boolean isTombstoneMap;

    private HashSlotArray records;
    private final RecordOffHeap rec = new RecordOffHeap();

    private RecordMapOffHeap(MemoryAllocator malloc, boolean isTombstoneMap, int initialCapacity) {
        this.malloc = malloc;
        this.isTombstoneMap = isTombstoneMap;
        this.records = new HashSlotArrayImpl(malloc, isTombstoneMap ? TOMBSTONE_SIZE : VALUE_RECORD_SIZE,
                initialCapacity, LOAD_FACTOR);
    }

    public static RecordMapOffHeap newRecordMapOffHeap(MemoryAllocator malloc) {
        return new RecordMapOffHeap(malloc, false, DEFAULT_INITIAL_CAPACITY);
    }

    public static RecordMapOffHeap newTombstoneMapOffHeap(MemoryAllocator malloc) {
        return new RecordMapOffHeap(malloc, true, DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public Record putIfAbsent(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone, int additionalInt) {
        final KeyHandleOffHeap ohk = (KeyHandleOffHeap) kh;
        final long addr = records.ensure(ohk.address(), ohk.sequenceId());
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

    @Override public SortedBySeqRecordCursor sortedBySeqCursor(
            int liveRecordCount, RecordMap[] recordMaps, MutatorCatchup mc
    ) {
        final LongArray seqsAndSlotBases = new LongArray(malloc, 2 * liveRecordCount);
        int i = 0;
        final RecordOffHeap r = new RecordOffHeap();
        for (RecordMap map : recordMaps) {
            for (HashSlotCursor cursor = ((RecordMapOffHeap) map).records.cursor(); cursor.advance();) {
                r.address = cursor.valueAddress();
                if (r.isAlive()) {
                    seqsAndSlotBases.set(i++, r.liveSeq());
                    seqsAndSlotBases.set(i++, valueAddr2slotBase(r.address));
                }
            }
        }
        assert i == 2 * liveRecordCount : String.format(
                "Mismatch between supplied and actual live record counts: supplied %,d actual %,d",
                liveRecordCount, i / 2);
        mc.catchupNow();
        return new SortedBySeqRecordCursorOffHeap(seqsAndSlotBases, malloc, mc);
    }

    @Override public CursorOffHeap cursor() {
        return new CursorOffHeap();
    }

    @Override public void dispose() {
        records.dispose();
    }

    final class CursorOffHeap implements Cursor {
        private final HashSlotCursor c = records.cursor();
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
