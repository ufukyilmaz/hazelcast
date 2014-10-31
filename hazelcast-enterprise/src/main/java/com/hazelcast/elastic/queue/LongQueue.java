package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;

/**
 * @author mdogan 21/01/14
 */
public interface LongQueue {

    boolean offer(long value);

    long peek();

    long poll();

    int size();

    boolean isEmpty();

    int capacity();

    int remainingCapacity();

    void clear();

    void destroy();

    long nullItem();

    LongIterator iterator();

}
