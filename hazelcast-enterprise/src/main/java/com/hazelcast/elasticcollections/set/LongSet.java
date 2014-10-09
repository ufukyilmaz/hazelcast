package com.hazelcast.elasticcollections.set;

import com.hazelcast.elasticcollections.LongIterator;

public interface LongSet {

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


    boolean add(long value);

    boolean remove(long value);

    boolean contains(long value);

    int size();

    boolean isEmpty();

    void clear();

    void destroy();

    LongIterator iterator();

}
