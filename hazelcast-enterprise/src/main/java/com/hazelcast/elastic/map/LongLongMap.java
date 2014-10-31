package com.hazelcast.elastic.map;

import com.hazelcast.elastic.LongIterator;

public interface LongLongMap {

    /**
     * Maximum capacity for an array that is of power-of-two size and still
     * allocatable in Java (not a negative int).
     */
    int MAX_CAPACITY = 0x80000000 >>> 1;

    /**
     * Minimum capacity for a hash container.
     */
    int MIN_CAPACITY = 4;

    /**
     * Default capacity for a hash container.
     */
    int DEFAULT_CAPACITY = 16;

    /**
     * Default load factor.
     */
    float DEFAULT_LOAD_FACTOR = 0.75f;

    long put(long key, long value);

    long putIfAbsent(long key, long value);

    boolean replace(long key, long oldValue, long newValue);

    long replace(long key, long value);

    long remove(long key);

    boolean delete(long key);

    boolean remove(long key, long value);

    long get(long key);

    boolean containsKey(long key);

    boolean containsValue(long value);

    LongIterator keysIterator();

    LongIterator valuesIterator();

    long nullValue();

    int size();

    boolean isEmpty();

    void clear();

    void destroy();
}
