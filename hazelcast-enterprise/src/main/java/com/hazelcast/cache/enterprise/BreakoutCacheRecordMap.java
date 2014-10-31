package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.elastic.SlottableIterator;

/**
 * @param <K> Type of key for cache record stored in this cache record map
 * @param <V> Type of value for cache record stored in this cache record map
 *
 * @author sozal 14/10/14
 */
public interface BreakoutCacheRecordMap<K, V> extends CacheRecordMap<K, V> {

    /**
     * Returns an slottable iterator for this {@link BreakoutCacheRecordMap} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @param <E>  the type of the entry iterated by the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    <E> SlottableIterator<E> iterator(int slot);

}
