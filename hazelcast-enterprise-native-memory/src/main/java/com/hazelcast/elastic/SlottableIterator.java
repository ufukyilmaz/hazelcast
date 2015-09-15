package com.hazelcast.elastic;

import java.util.Iterator;

/**
 * @author sozal 26/10/14
 */
public interface SlottableIterator<E> extends Iterator<E> {

    int advance(int start);
    int nextSlot();
    int getNextSlot();
    int getCurrentSlot();

}
