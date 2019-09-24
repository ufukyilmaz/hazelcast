package com.hazelcast.spi.hotrestart.impl.gc.tracker;

import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.spi.hotrestart.KeyHandle;

/**
 * Special-purpose map from {@link KeyHandle} to {@link Tracker}, used to hold
 * global GC-related metadata about all records in the Hot Restart Store.
 */
public interface TrackerMap extends Disposable {

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

    /**
     * Removes the entry for {@code kh} assuming that it maps to
     * a tracker representing a live tombstone. This assumption is
     * not verified.
     */
    void removeLiveTombstone(KeyHandle kh);

    /**
     * Removes the entry for {@code tr} if {@code tr} represents a dead record.
     * {@code tr} must be the tracker mapped under {@code kh}, but this is not verified.
     */
    void removeIfDead(KeyHandle kh, Tracker tr);

    /** @return the number of entries in this map. */
    long size();

    Cursor cursor();

    /** Cursor over tracker map's entries. A newly obtained cursor is positioned before the first item. */
    interface Cursor {

        /**
         * Attempts to advance the cursor to the next position.
         * @return {@code true} if the cursor advanced; {@code false} otherwise.
         */
        boolean advance();

        /**
         * Returns the key handle associated with the tracker at the current cursor position. May return
         * the cursor object itself, therefore the returned object is valid only until the cursor is
         * updated by calling {@link #advance()} or the owning map is updated.
         */
        KeyHandle asKeyHandle();

        /** @return the key handle associated with the tracker at the current cursor position */
        KeyHandle toKeyHandle();

        /** @return the tracker at the current cursor position. May return the cursor object itself,
         * therefore the returned object is valid only until the cursor is updated by calling
         * {@link #advance()} or the owning map is updated. */
        Tracker asTracker();
    }
}
