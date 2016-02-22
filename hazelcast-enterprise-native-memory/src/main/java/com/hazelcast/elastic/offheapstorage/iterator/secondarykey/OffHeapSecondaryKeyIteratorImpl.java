package com.hazelcast.elastic.offheapstorage.iterator.secondarykey;

import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;
import com.hazelcast.elastic.offheapstorage.sorted.secondarykey.OffHeapSecondaryKeyValueSortedStorage;

import java.util.NoSuchElementException;

/**
 * Iterator over secondary keys stored in a red-black tree.
 */
public class OffHeapSecondaryKeyIteratorImpl implements OffHeapSecondaryKeyIterator {

    private long pointer;
    private long keyEntryAddress;
    private OrderingDirection direction;
    private final OffHeapSecondaryKeyValueSortedStorage storage;

    public OffHeapSecondaryKeyIteratorImpl(OffHeapSecondaryKeyValueSortedStorage storage) {
        this.storage = storage;
    }

    @Override
    public void setKeyEntry(long keyEntryAddress) {
        this.keyEntryAddress = keyEntryAddress;
        setDirection(OrderingDirection.ASC);
    }

    @Override
    public boolean hasNext() {
        return keyEntryAddress != 0L && pointer != 0L;
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

    @Override
    public void setDirection(OrderingDirection direction) {
        this.direction = direction;

        pointer = storage.firstSecondaryKeyEntry(direction, keyEntryAddress);
    }
}
