package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.nio.Disposable;

/** Set of primitive long integers. */
public interface LongSet extends Disposable {

    boolean add(long value);

    boolean remove(long value);

    boolean contains(long value);

    int size();

    boolean isEmpty();

    void clear();

    LongIterator iterator();

}
