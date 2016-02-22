package com.hazelcast.elastic.offheapstorage.iterator;

import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueSortedStorage;
import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;

import java.util.NoSuchElementException;

/**
 * Key iterator over the red-black tree.
 */
public class OffHeapKeyRedBlackTreeKeysIteratorImpl implements OffHeapKeyIterator {

    private long pointer;
    private OrderingDirection direction;
    private final OffHeapKeyValueSortedStorage storage;

    public OffHeapKeyRedBlackTreeKeysIteratorImpl(OffHeapKeyValueSortedStorage storage) {
        this.storage = storage;
    }

    @Override
    public void setDirection(OrderingDirection direction) {
        this.direction = direction;

        pointer = storage.first(direction);
    }

    @Override
    public boolean hasNext() {
        return pointer != 0L;
    }

    @Override
    public long next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        long result = pointer;
        pointer = storage.getNext(pointer, direction);
        return result;
    }
}
