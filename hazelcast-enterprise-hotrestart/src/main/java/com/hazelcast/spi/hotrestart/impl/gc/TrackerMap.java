package com.hazelcast.spi.hotrestart.impl.gc;

import com.hazelcast.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;

/**
 * Map for record trackers
 */
interface TrackerMap extends Disposable {

    /**
     * If there is no current mapping for {@code kh}, establishes a mapping from
     * {@code kh} to a tracker defined by ({@code chunkSeq}, {@code isTombstone})
     * and returns {@code null}. If a mapping already exists, returns the existing
     * tracker without updating it.
     */
    Tracker putIfAbsent(KeyHandle kh, long chunkSeq, boolean isTombstone);

    /**
     * Returns the tracker mapped by the supplied {@code KeyHandle}, if any; otherwise
     * returns {@code null}.
     * <b>Warning! The returned instance is valid only up to the next operation
     * on this map, including another {@code get}. The caller should generally not retain it.</b>
     */
    Tracker get(KeyHandle kh);

    void remove(KeyHandle kh);

    long size();

    Cursor cursor();

    /** Cursor over tracker map's entries */
    interface Cursor {
        boolean advance();

        KeyHandle asKeyHandle();

        KeyHandle toKeyHandle();

        Tracker asTracker();

        void remove();
    }
}
