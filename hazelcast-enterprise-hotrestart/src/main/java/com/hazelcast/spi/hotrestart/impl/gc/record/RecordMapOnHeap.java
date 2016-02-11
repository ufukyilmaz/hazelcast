package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.Math.min;
import static java.util.Arrays.asList;

/**
 * Map with on-heap keys and records.
 */
public final class RecordMapOnHeap implements RecordMap {
    static final float LOAD_FACTOR = 0.75f;
    private final Map<KeyHandle, Record> records;

    public RecordMapOnHeap() {
        this.records = new HashMap<KeyHandle, Record>();
    }

    @Override public Record putIfAbsent(
            long ignored, KeyHandle kh, long seq, int size, boolean isTombstone, int additionalInt
    ) {
        final Record rec = records.get(kh);
        if (rec != null) {
            return rec;
        }
        records.put(kh, new RecordOnHeap(seq, size, isTombstone, additionalInt));
        return null;
    }

    @Override public Record get(KeyHandle kh) {
        return records.get(kh);
    }

    @Override public int size() {
        return records.size();
    }

    @Override public SortedBySeqRecordCursor sortedBySeqCursor(
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
        return sortedCursor(khs, recs, mc);
    }

    @Override public Cursor cursor() {
        return new HeapCursor();
    }

    @Override public void dispose() { }

    private static SortedBySeqRecordCursor sortedCursor(List<KeyHandle> khs, List<Record> recs, MutatorCatchup mc) {
        final int size = recs.size();
        List<Record> recFrom = recs;
        List<Record> recTo = asList(new Record[size]);
        List<KeyHandle> khFrom = khs;
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
        return new SortedBySeqRecordCursorOnHeap(khFrom, recFrom);
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

    static final class KhsRecsHolder {
        List<KeyHandle> keyHandles;
        List<Record> records;

        KhsRecsHolder(List<KeyHandle> keyHandles, List<Record> records) {
            this.keyHandles = keyHandles;
            this.records = records;
        }
    }

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
    }
}
