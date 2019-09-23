package com.hazelcast.internal.elastic;

/**
 * Interface for queue iterator implementations.
 */
public interface LongIterator {

    boolean hasNext();

    long next();

    void remove();

    void reset();
}
