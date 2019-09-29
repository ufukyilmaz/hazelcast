package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;

import java.util.List;

import static java.lang.Math.min;
import static java.util.Arrays.asList;

/**
 * On-heap implementation of {@link SortedBySeqRecordCursor}.
 */
public class SortedBySeqRecordCursorOnHeap implements SortedBySeqRecordCursor {
    private final List<KeyHandle> keyHandles;
    private final List<Record> records;
    private int position = -1;

    SortedBySeqRecordCursorOnHeap(List<KeyHandle> keyHandles, List<Record> records, MutatorCatchup mc) {
        assert keyHandles.size() == records.size() : "Handles and records sizes mismatch";
        final int size = records.size();
        List<Record> recFrom = records;
        List<Record> recTo = asList(new Record[size]);
        List<KeyHandle> khFrom = keyHandles;
        List<KeyHandle> khTo = asList(new KeyHandle[size]);
        for (int width = 1; width < size; width *= 2) {
            for (int i = 0; i < size; i += 2 * width) {
                bottomUpMerge(recFrom, khFrom, i, min(i + width, size), min(i + 2 * width, size), recTo, khTo, mc);
            }
            final List<Record> fromBackup = recFrom;
            recFrom = recTo;
            recTo = fromBackup;
            final List<KeyHandle> khFromBackup = khFrom;
            khFrom = khTo;
            khTo = khFromBackup;
        }
        this.keyHandles = khFrom;
        this.records = recFrom;
    }

    @Override
    public boolean advance() {
        if (position == records.size() - 1) {
            return false;
        }
        position++;
        return true;
    }

    @Override
    public KeyHandle asKeyHandle() {
        assert position >= 0 : "asKeyHandle() called on an invalid cursor";
        return keyHandles.get(position);
    }

    @Override
    public Record asRecord() {
        assert position >= 0 : "asRecord() called on an invalid cursor";
        return records.get(position);
    }

    @Override
    public void dispose() {
    }

    private static void bottomUpMerge(List<Record> from, List<KeyHandle> khFrom, int leftStart, int rightStart,
                                      int rightEnd, List<Record> to, List<KeyHandle> khTo, MutatorCatchup mc
    ) {
        int currLeft = leftStart;
        int currRight = rightStart;
        for (int i = leftStart; i < rightEnd; i++) {
            if (currLeft < rightStart
                    && (currRight >= rightEnd
                    || from.get(currLeft).deadOrAliveSeq() <= from.get(currRight).deadOrAliveSeq())
                    ) {
                to.set(i, from.get(currLeft));
                khTo.set(i, khFrom.get(currLeft++));
            } else {
                to.set(i, from.get(currRight));
                khTo.set(i, khFrom.get(currRight++));
            }
            mc.catchupAsNeeded();
        }
    }
}
