package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.spi.hotrestart.KeyHandle;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Map with on-heap keys and records.
 */
final class RecordMapOnHeap implements RecordMap {
    static final float LOAD_FACTOR = 0.75f;
    private final Map<KeyHandle, Record> records;

    RecordMapOnHeap() {
        this.records = new HashMap<KeyHandle, Record>();
    }

    RecordMapOnHeap(RecordMap gcRecordMap) {
        // The semantics of HashMap's initial capacity are unfortunate: instead
        // of ensuring capacity for the given number of entries without expansion,
        // it just makes the bucket array that large, then expands at load factor threshold.
        this.records = new HashMap<KeyHandle, Record>((int) (gcRecordMap.size() / LOAD_FACTOR), LOAD_FACTOR);
        for (Cursor cur = gcRecordMap.cursor(); cur.advance();) {
            final Record r = cur.asRecord();
            if (r.isAlive() || r.garbageCount() != 0) {
                records.put(cur.toKeyHandle(), new RecordOnHeap(r));
            }
        }
    }

    @Override public Record putIfAbsent(
            long ignored, KeyHandle kh, long seq, int size, boolean isTombstone, int garbageCount) {
        final Record rec = records.get(kh);
        if (rec != null) {
            return rec;
        }
        records.put(kh, new RecordOnHeap(seq, size, isTombstone, garbageCount));
        return null;
    }

    void put(KeyHandle kh, GcRecord gcr) {
        records.put(kh, gcr);
    }

    @Override public Record get(KeyHandle kh) {
        return records.get(kh);
    }

    @Override public int size() {
        return records.size();
    }

    @Override public Cursor cursor() {
        return new HeapCursor();
    }

    @Override public void dispose() { }

    private final class HeapCursor implements Cursor {
        private final Iterator<Entry<KeyHandle, Record>> iter = records.entrySet().iterator();
        private Entry<KeyHandle, Record> current;

        @Override public boolean advance() {
            if (!iter.hasNext()) {
                return false;
            }
            current = iter.next();
            return true;
        }

        @Override public KeyHandle toKeyHandle() {
            return current.getKey();
        }

        @Override public Record asRecord() {
            return current.getValue();
        }

        @Override public GcRecord toGcRecord(long chunkSeq) {
            return new GcRecord.WithHeapHandle(asRecord(), chunkSeq, toKeyHandle());
        }
    }
}
