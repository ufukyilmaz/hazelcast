package com.hazelcast.elastic.offheapstorage.iterator;

import java.util.NoSuchElementException;

import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;
import com.hazelcast.elastic.offheapstorage.sorted.OffHeapKeyValueSortedStorage;

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
        this.pointer = this.storage.first(direction);
    }

    @Override
    public boolean hasNext() {
        return this.pointer != 0L;
    }

    @Override
    public long next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        long result = pointer;
        this.pointer = this.storage.getNext(this.pointer, this.direction);
        return result;
    }
}
