package com.hazelcast.elastic;

import java.util.Iterator;

/**
 * @param <E> {@inheritDoc}
 */
public interface SlottableIterator<E> extends Iterator<E> {

    int advance(int start);
    int nextSlot();
    int getNextSlot();
    int getCurrentSlot();

}
