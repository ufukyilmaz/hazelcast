package com.hazelcast.elastic.tree.iterator;

import com.hazelcast.elastic.tree.sorted.OffHeapKeyValueSortedStorage;
import com.hazelcast.elastic.tree.sorted.OrderingDirection;

import java.util.NoSuchElementException;

/***
 * Iterator to iterate over keys of the off-heap storage structure;
 */
public class OffHeapKeyRedBlackTreeKeysIteratorImpl implements OffHeapKeyIterator {
    private long pointer;
    private OrderingDirection direction;
    private final OffHeapKeyValueSortedStorage storage;

    public OffHeapKeyRedBlackTreeKeysIteratorImpl(OffHeapKeyValueSortedStorage storage) {
        this.storage = storage;
        this.pointer = this.storage.first(direction);
    }

    public OffHeapKeyRedBlackTreeKeysIteratorImpl(OffHeapKeyValueSortedStorage storage, long keyEntryPointer) {
        this.storage = storage;
        this.pointer = keyEntryPointer;
    }

    @Override
    public void setDirection(OrderingDirection direction) {
        this.direction = direction;
        this.pointer = this.storage.first(direction);
    }

    @Override
    public void setDirection(OrderingDirection direction, long keyEntryPointer) {
        this.direction = direction;
        this.pointer = keyEntryPointer;
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
