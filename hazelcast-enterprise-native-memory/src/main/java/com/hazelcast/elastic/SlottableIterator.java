package com.hazelcast.elastic;

import java.util.Iterator;

/**
 * @param <E> {@inheritDoc}
 */
public interface SlottableIterator<E> extends Iterator<E> {

    int nextSlot();
    int getNextSlot();
    int getCurrentSlot();

}
