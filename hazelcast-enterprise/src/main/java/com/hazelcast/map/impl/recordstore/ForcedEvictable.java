package com.hazelcast.map.impl.recordstore;

import java.util.Iterator;

/**
 * Mark {@link Storage} as capable of running forced-evictions.
 *
 * It's surprisingly difficult to mass-remove entries without
 * creating clusters after a few cycles. Data structures like
 * Open Addressing Hash Map with linear probing are very prone
 * to clustering. This leads to a performance degradation.
 *
 * Iterator returned by {@link #newForcedEvictionValuesIterator()}
 * is optimized for mass removals and it should do its best
 * to prevent data clustering.
 */
public interface ForcedEvictable<T> {

    /**
     * Return iterator to be used for forced evictions. This is
     * not a general-purpose iterator, it's intended to be used
     * exclusively for evictions. It does not guarantee to iterate
     * over all entries and it can give you the same entries twice.
     *
     * This iterator does <b>not</b> have a fail-fast semantic. A caller can
     * use {@link Iterator#remove()} method to remove a current record or a
     * {@link Storage#removeRecord(Object)} - but it's only allowed to remove
     * a record the iterator is currently pointing to.  No other modification
     * of the underlying storage is allowed while using this iterator.
     *
     * @return
     */
    Iterator<T> newRandomEvictionKeyIterator();
}
