package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.elastic.SlottableIterator;
import com.hazelcast.nio.serialization.Data;

/**
 * @param <R> Type of the cache record stored in this cache record map
 *
 * @author sozal 14/10/14
 */
public interface HiDensityCacheRecordMap<R extends HiDensityCacheRecord>
        extends CacheRecordMap<Data, R> {

    /**
     * Returns an slottable iterator for this {@link HiDensityCacheRecordMap} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @param <E>  the type of the entry iterated by the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    <E> SlottableIterator<E> iterator(int slot);

    /**
     * Forcefully evict records as given <code>evictionPercentage</code>.
     *
     * @param evictionPercentage Percentage to determine how many records will be evicted
     * @return evicted entry count
     */
    int forceEvict(int evictionPercentage);

}
