package com.hazelcast.elastic.binarystorage.iterator;

import com.hazelcast.elastic.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.elastic.binarystorage.sorted.OrderingDirection;

import java.util.NoSuchElementException;

/**
 * Key iterator over the red-black tree.
 */
public class BinaryKeyRedBlackTreeKeysIteratorImpl implements BinaryKeyIterator {

    private long pointer;
    private OrderingDirection direction;
    private final BinaryKeyValueSortedStorage storage;

    public BinaryKeyRedBlackTreeKeysIteratorImpl(BinaryKeyValueSortedStorage storage) {
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
