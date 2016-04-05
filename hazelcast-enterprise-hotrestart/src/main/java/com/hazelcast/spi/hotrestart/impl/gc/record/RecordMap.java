package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.GcExecutor.MutatorCatchup;

/**
 * Abstraction for chunk's record map.
 */
public interface RecordMap extends Disposable {
    /**
     * If there is no current mapping for {@code kh}, establishes a mapping from
     * {@code kh} to a record defined by ({@code seq}, {@code size}, {@code isTombstone}, {@code additionalInt})
     * and returns {@code null}. If a mapping already exists, returns the existing
     * record without updating it.
     * <p><b>Warning! The returned instance is valid only up to the next update operation
     * on this map. The caller should generally not retain it.</b>
     * @param additionalInt for a value record, this is its garbage count;
     *                      for tombstone, it is its position in the chunk file.
     */
    Record putIfAbsent(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone, int additionalInt);

    /**
     * Returns the record mapped by the supplied {@code KeyHandle}, if any; otherwise
     * returns {@code null}.
     * <b>Warning! The returned instance is valid only up to the next operation
     * on this map, including another {@code get}. The caller should generally not retain it.</b>
     */
    Record get(KeyHandle kh);

    int size();

    /** Creates a sorted-by-seq cursor over all live records in the provided record maps. The record maps
     * must be of the same type as this record map. */
    SortedBySeqRecordCursor sortedBySeqCursor(int liveRecordCount, RecordMap[] recordMaps, MutatorCatchup mc);

    Cursor cursor();

    RecordMap toStable();

    /** Cursor over record map's contents */
    interface Cursor {
        boolean advance();
        Record asRecord();
        KeyHandle toKeyHandle();
    }
}
