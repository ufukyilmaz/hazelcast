package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.internal.elastic.LongArray;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.spi.hotrestart.KeyHandleOffHeap;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.addrOfKey1At;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.addrOfKey2At;
import static com.hazelcast.internal.util.hashslot.impl.HashSlotArray16byteKeyImpl.addrOfValueAt;
import static java.lang.Math.min;

/**
 * Off-heap implementation of {@link com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor}.
 * Contains pointers to record structures inside the {@link RecordMap}s, therefore becomes
 * invalid as soon as any of the record maps is updated.
 */
final class SortedBySeqRecordCursorOffHeap implements SortedBySeqRecordCursor, KeyHandleOffHeap {
    private final LongArray seqsAndSlotBases;
    private final int size;
    private final RecordOffHeap r = new RecordOffHeap();
    private final MemoryAccessor mem;
    private int position = -1;

    SortedBySeqRecordCursorOffHeap(LongArray seqsAndSlotBases, int actualSize, MemoryManager memMgr, MutatorCatchup mc) {
        this.size = actualSize;
        this.mem = memMgr.getAccessor();
        this.seqsAndSlotBases = sortedByRecordSeq(seqsAndSlotBases, actualSize, memMgr, mc);
    }

    @Override
    public boolean advance() {
        if (position != size - 1) {
            position += 2;
            r.address = addrOfValueAt(seqsAndSlotBases.get(position));
            return true;
        } else {
            r.address = NULL_ADDRESS;
            return false;
        }
    }

    @Override
    public Record asRecord() {
        return r;
    }

    @Override
    public KeyHandleOffHeap asKeyHandle() {
        assert r.address != NULL_ADDRESS : "Invalid cursor state";
        return this;
    }

    @Override
    public long address() {
        assert r.address != NULL_ADDRESS : "Invalid cursor state";
        return mem.getLong(addrOfKey1At(seqsAndSlotBases.get(position)));
    }

    @Override
    public long sequenceId() {
        assert r.address != NULL_ADDRESS : "Invalid cursor state";
        return mem.getLong(addrOfKey2At(seqsAndSlotBases.get(position)));
    }

    @Override
    public void dispose() {
        seqsAndSlotBases.dispose();
    }

    private static LongArray sortedByRecordSeq(
            LongArray seqsAndSlotBases, int size, MemoryManager memMgr, MutatorCatchup mc
    ) {
        LongArray from = seqsAndSlotBases;
        LongArray to = new LongArray(memMgr, size);
        mc.catchupNow();
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

    private static void bottomUpMerge(
            LongArray from, int leftStart, int rightStart, int rightEnd, LongArray to, MutatorCatchup mc
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
}
