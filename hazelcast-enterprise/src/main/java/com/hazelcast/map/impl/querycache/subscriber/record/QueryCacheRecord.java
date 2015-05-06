package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.cache.impl.eviction.Evictable;
import com.hazelcast.nio.serialization.Data;

/**
 * Represents a {@link com.hazelcast.map.QueryCache QueryCache} record.
 *
 * @param <V> the type of the value of this record.
 */
public interface QueryCacheRecord<V> extends Evictable {

    Data getKey();

    V getValue();

    /**
     * Sets the access time of this {@link Evictable} in milliseconds.
     *
     * @param time the latest access time of this {@link Evictable} in milliseconds
     */
    void setAccessTime(long time);

    /**
     * Increases the access hit count of this {@link Evictable} as <code>1</code>.
     */
    void incrementAccessHit();

}

