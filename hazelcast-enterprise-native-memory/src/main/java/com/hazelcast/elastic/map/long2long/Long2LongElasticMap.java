package com.hazelcast.elastic.map.long2long;

/**
 * A map that uses primitive {@code long}s for both key and value.
 */
public interface Long2LongElasticMap {

    long get(long key);

    long put(long key, long value);

    long putIfAbsent(long key, long value);

    void putAll(Long2LongElasticMap entries);

    boolean replace(long key, long oldValue, long newValue);

    long replace(long key, long value);

    long remove(long key);

    boolean remove(long key, long value);

    boolean containsKey(long key);

    long size();

    boolean isEmpty();

    void clear();

    void dispose();

    LongLongCursor cursor();
}
