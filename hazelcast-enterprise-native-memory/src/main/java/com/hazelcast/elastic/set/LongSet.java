package com.hazelcast.elastic.set;

import com.hazelcast.elastic.LongCursor;
import com.hazelcast.nio.Disposable;

/** Set of primitive long integers. */
public interface LongSet extends Disposable {

    boolean add(long value);

    boolean remove(long value);

    boolean contains(long value);

    long size();

    boolean isEmpty();

    void clear();

    LongCursor cursor();
}
