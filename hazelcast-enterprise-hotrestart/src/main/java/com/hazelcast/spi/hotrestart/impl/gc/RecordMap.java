package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;

/**
 * Abstraction for chunk's record map.
 */
public interface RecordMap extends Disposable {
    /**
     * If there is no current mapping for {@code kh}, establishes a mapping from
     * {@code kh} to a record defined by ({@code seq}, {@code size}, {@code isTombstone})
     * and returns {@code null}. If a mapping already exists, returns the existing
     * record without updating it.
     * <p><b>Warning! The returned instance is valid only up to the next update operation
     * on this map. The caller should generally not retain it.</b>
     */
    Record putIfAbsent(long prefix, KeyHandle kh, long seq, int size, boolean isTombstone, int garbageCount);

    /**
     * Returns the record mapped by the supplied {@code KeyHandle}, if any; otherwise
     * returns {@code null}.
     * <b>Warning! The returned instance is valid only up to the next operation
     * on this map, including another {@code get}. The caller should generally not retain it.</b>
     */
    Record get(KeyHandle kh);

    int size();

    Cursor cursor();

    /** Cursor over record map's contents */
    interface Cursor {
        boolean advance();
        Record asRecord();
        KeyHandle toKeyHandle();
        GcRecord toGcRecord(long chunkSeq);
    }
}
