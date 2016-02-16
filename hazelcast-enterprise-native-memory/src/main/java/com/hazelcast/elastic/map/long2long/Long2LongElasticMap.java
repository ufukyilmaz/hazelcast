package com.hazelcast.elastic.map.long2long;

import com.hazelcast.elastic.map.ElasticMap;

/**
 *
 */
public interface Long2LongElasticMap extends ElasticMap<Long, Long> {

    long get(long key);

    long put(long key, long value);

    boolean set(long key, long value);

    long putIfAbsent(long key, long value);

    boolean replace(long key, long oldValue, long newValue);

    long replace(long key, long value);

    long remove(long key);

    boolean delete(long key);

    boolean remove(long key, long value);

    boolean containsKey(long key);

    boolean containsValue(long value);

}
