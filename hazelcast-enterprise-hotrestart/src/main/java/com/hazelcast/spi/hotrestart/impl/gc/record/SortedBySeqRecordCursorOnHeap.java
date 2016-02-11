package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.record.RecordMapOnHeap.KhsRecsHolder;

import java.util.List;

/**
 * On-heap implementation of {@link SortedBySeqRecordCursor}.
 */
public class SortedBySeqRecordCursorOnHeap implements SortedBySeqRecordCursor {
    private final List<KeyHandle> keyHandles;
    private final List<Record> records;
    private int position = -1;

    public SortedBySeqRecordCursorOnHeap(List<KeyHandle> keyHandles, List<Record> records) {
        this.keyHandles = keyHandles;
        this.records = records;
    }

    @Override public boolean advance() {
        if (position == records.size() - 1) {
            return false;
        }
        position++;
        return true;
    }

    @Override public KeyHandle asKeyHandle() {
        assert position >= 0 : "asKeyHandle() called on an invalid cursor";
        return keyHandles.get(position);
    }

    @Override public Record asRecord() {
        assert position >= 0 : "asRecord() called on an invalid cursor";
        return records.get(position);
    }

    @Override public void dispose() {
    }
}
