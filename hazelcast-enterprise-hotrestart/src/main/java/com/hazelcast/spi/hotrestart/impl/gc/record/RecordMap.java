package com.hazelcast.spi.hotrestart.impl.gc.record;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;
import com.hazelcast.spi.hotrestart.impl.SortedBySeqRecordCursor;
import com.hazelcast.spi.hotrestart.impl.gc.MutatorCatchup;

/**
 * Special-purpose map from {@link KeyHandle} to {@link Record}, used to hold GC-related
 * metadata about one chunk.
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

    /** @return the number of entries in this map. */
    int size();

    /** Creates a sorted-by-seq cursor over all live records in the provided record maps. The record maps
     * must be of the same type as this record map. */
    SortedBySeqRecordCursor sortedBySeqCursor(int liveRecordCount, RecordMap[] recordMaps, MutatorCatchup mc);

    Cursor cursor();

    /**
     * Special-purpose method supporting an off-heap memory optimization where during GC the growing chunk's
     * record map is stored using an auxiliary memory allocator that allocates from a different pool than
     * the main one, thus reducing the peak memory demand on the main allocator. This method moves the data
     * back to the main allocator.
     *
     * @return a copy of this record map appropriate to be used inside a {@code StableChunk}.
     */
    RecordMap toStable();

    /** Cursor over record map's contents. A newly obtained cursor is positioned before the first record. */
    interface Cursor {

        /**
         * Attempts to advance the cursor to the next position.
         * @return {@code true} if the cursor advanced; {@code false} otherwise.
         */
        boolean advance();

        /**
         * Returns the record at the current cursor position. May return the cursor object itself,
         * therefore the returned object is valid only until the cursor is updated by calling {@link #advance()}.
         */
        Record asRecord();

        /** @return the key handle associated with the record at the current cursor position */
        KeyHandle toKeyHandle();
    }
}
