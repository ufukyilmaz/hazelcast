package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongIterator;

public interface LongSet {

    boolean add(long value);

    boolean remove(long value);

    boolean contains(long value);

    int size();

    boolean isEmpty();

    void clear();

    void destroy();

    LongIterator iterator();

}
