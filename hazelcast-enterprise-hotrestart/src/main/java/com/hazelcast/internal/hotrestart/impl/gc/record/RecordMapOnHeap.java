package com.hazelcast.internal.hotrestart.impl.gc.record;

import com.hazelcast.internal.hotrestart.KeyHandle;
import com.hazelcast.internal.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.internal.hotrestart.impl.gc.MutatorCatchup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Map with on-heap keys and records.
 */
public final class RecordMapOnHeap implements RecordMap {
    private final Map<KeyHandle, Record> records;

    public RecordMapOnHeap() {
        this.records = new HashMap<KeyHandle, Record>();
    }

    @Override
    public Record putIfAbsent(
            long ignored, KeyHandle kh, long seq, int size, boolean isTombstone, int additionalInt
    ) {
        final Record rec = records.get(kh);
        if (rec != null) {
            return rec;
        }
        records.put(kh, new RecordOnHeap(seq, size, isTombstone, additionalInt));
        return null;
    }

    @Override
    public Record get(KeyHandle kh) {
        return records.get(kh);
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public SortedBySeqRecordCursor sortedBySeqCursor(
            int liveRecordCount, RecordMap[] recordMaps, MutatorCatchup mc
    ) {
        final List<KeyHandle> khs = new ArrayList<KeyHandle>(liveRecordCount);
        final List<Record> recs = new ArrayList<Record>(liveRecordCount);
        for (RecordMap map : recordMaps) {
            for (Cursor c = map.cursor(); c.advance();) {
                final Record r = c.asRecord();
                if (r.isAlive()) {
                    khs.add(c.toKeyHandle());
                    recs.add(r);
                }
            }
        }
        mc.catchupNow();
        return new SortedBySeqRecordCursorOnHeap(khs, recs, mc);
    }

    @Override
    public Cursor cursor() {
        return new HeapCursor();
    }

    @Override
    public RecordMap toStable() {
        return this;
    }

    @Override
    public void dispose() { }

    private final class HeapCursor implements Cursor {
        private final Iterator<Entry<KeyHandle, Record>> iter = records.entrySet().iterator();
        private Entry<KeyHandle, Record> current;

        @Override
        public boolean advance() {
            if (!iter.hasNext()) {
                return false;
            }
            current = iter.next();
            return true;
        }

        @Override
        public KeyHandle toKeyHandle() {
            return current.getKey();
        }

        @Override
        public Record asRecord() {
            return current.getValue();
        }
    }
}
