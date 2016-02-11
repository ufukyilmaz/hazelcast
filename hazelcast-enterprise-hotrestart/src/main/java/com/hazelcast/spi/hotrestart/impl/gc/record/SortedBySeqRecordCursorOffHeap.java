package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.elastic.LongArray;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import static com.hazelcast.elastic.map.HashSlotArrayImpl.addrOfValueAt;
import static com.hazelcast.elastic.map.HashSlotArrayImpl.key1At;
import static com.hazelcast.elastic.map.HashSlotArrayImpl.key2At;
import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;
import static java.lang.Math.min;

/**
 * Off-heap implementation of {@link SortedBySeqRecordCursor}.
 */
final class SortedBySeqRecordCursorOffHeap implements SortedBySeqRecordCursor, KeyHandleOffHeap {
    private final LongArray seqsAndSlotBases;
    private final RecordOffHeap r = new RecordOffHeap();
    private int position = -1;

    SortedBySeqRecordCursorOffHeap(LongArray seqsAndSlotBases, MemoryAllocator malloc, MutatorCatchup mc) {
        this.seqsAndSlotBases = sortedByRecordSeq(seqsAndSlotBases, malloc, mc);
    }

    @Override public boolean advance() {
        if (position != seqsAndSlotBases.length() - 1) {
            position += 2;
            r.address = addrOfValueAt(seqsAndSlotBases.get(position));
            return true;
        } else {
            r.address = NULL_ADDRESS;
            return false;
        }
    }

    @Override public Record asRecord() {
        return r;
    }

    @Override public KeyHandle asKeyHandle() {
        assert r.address != NULL_ADDRESS : "Invalid cursor state";
        return this;
    }

    @Override public long address() {
        assert r.address != NULL_ADDRESS : "Invalid cursor state";
        return key1At(seqsAndSlotBases.get(position));
    }

    @Override public long sequenceId() {
        assert r.address != NULL_ADDRESS : "Invalid cursor state";
        return key2At(seqsAndSlotBases.get(position));
    }

    private static LongArray sortedByRecordSeq(LongArray seqsAndSlotBases, MemoryAllocator malloc, MutatorCatchup mc) {
        final int size = (int) seqsAndSlotBases.length();
        LongArray from = seqsAndSlotBases;
        LongArray to = new LongArray(malloc, size);
        for (int width = 2; width < size; width *= 2) {
            for (int i = 0; i < size; i += 2 * width) {
                bottomUpMerge(from, i, min(i + width, size), min(i + 2 * width, size), to, mc);
            }
            final LongArray fromBackup = from;
            from = to;
            to = fromBackup;
        }
        to.dispose();
        return from;
    }

    private static void bottomUpMerge(LongArray from, int leftStart, int rightStart, int rightEnd, LongArray to,
                                      MutatorCatchup mc
    ) {
        int currLeft = leftStart;
        int currRight = rightStart;
        for (int i = leftStart; i < rightEnd; i += 2) {
            if (currLeft < rightStart && (currRight >= rightEnd || from.get(currLeft) <= from.get(currRight))) {
                to.set(i, from.get(currLeft));
                to.set(i + 1, from.get(currLeft + 1));
                currLeft += 2;
            } else {
                to.set(i, from.get(currRight));
                to.set(i + 1, from.get(currRight + 1));
                currRight += 2;
            }
            mc.catchupAsNeeded();
        }
    }

    @Override public void dispose() {
        seqsAndSlotBases.dispose();
    }
}
