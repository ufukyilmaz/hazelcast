package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.nio.serialization.Data;

/**
 * Contract for various {@link QueryCacheRecord} factories.
 */
public interface QueryCacheRecordFactory {

    /**
     * Creates new {@link QueryCacheRecord}.
     *
     * @param key   {@link Data} key
     * @param value {@link Data} value
     * @return an instance of {@link QueryCacheRecord}
     */
    QueryCacheRecord createEntry(Data key, Data value);

    boolean isEquals(Object cacheRecordValue, Object value);
}
