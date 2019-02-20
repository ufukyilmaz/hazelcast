package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.nio.Disposable;

/**
 * Queue for primitive {@code long}s.
 */
public interface LongQueue extends Disposable {

    boolean offer(long value);

    long peek();

    long poll();

    int size();

    boolean isEmpty();

    int capacity();

    int remainingCapacity();

    void clear();

    long nullItem();

    LongIterator iterator();
}