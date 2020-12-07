package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Iterator;

/**
 * Represents an API for off-heap concurrent B+Tree index.
 * @param <T> the type of the lookup/range scan entries
 */
public interface BPlusTree<T extends QueryableEntry> extends Disposable {

    /**
     * Inserts new entry into B+tree. Overwrites old value if it exists.
     *
     * @param indexKey the index key component
     * @param entryKey the key of the entry corresponding to the indexKey
     * @param value    a reference to the value to be indexed
     * @return old value if it exists, {@code null} otherwise
     */
    NativeMemoryData insert(Comparable indexKey, NativeMemoryData entryKey, MemoryBlock value);

    /**
     * Removes an entry from the B+tree.
     *
     * @param indexKey the index key component
     * @param entryKey the key of the entry corresponding to the indexKey
     * @return the old removed value if it exists, {@code null} otherwise
     */
    NativeMemoryData remove(Comparable indexKey, NativeMemoryData entryKey);

    /**
     * Looks up the index and returns an iterator of entries matching the indexKey.
     *
     * @param indexKey the index key to be searched in the index
     * @return the iterator of {@code QueryableEntry} matching the lookup criteria
     * @throws IllegalArgumentException if the indexKey is {@code null}
     */
    Iterator<T> lookup(Comparable indexKey);

    /**
     * Returns a range scan iterator of entries matching the boundaries of the range.
     *
     * @param from          the beginning of the range
     * @param fromInclusive {@code true} if the beginning of the range is inclusive,
     *                      {@code false} otherwise.
     * @param to            the end of the range
     * @param toInclusive   {@code true} if the end of the range is inclusive,
     *                      {@code false} otherwise.
     * @param descending    {@code true} if return entries in the descending order,
     *                      {@code false} if return entries in the ascending order.
     * @return the iterator of entries in the range
     */
    Iterator<T> lookup(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive, boolean descending);

    /**
     * Returns an iterator of all unique index keys in the B+tree.
     * @return the iterator of all unique index keys
     */
    Iterator<Data> keys();

    /**
     * Removes all entries from the B+tree.
     */
    void clear();
}
