package com.hazelcast.elasticcollections;

/**
 * @author mdogan 09/01/14
 */
public interface LongIterator {

    boolean hasNext();

    long next();

    void remove();

    void reset();
}
