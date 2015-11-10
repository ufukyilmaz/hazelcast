package com.hazelcast.elastic.offheapstorage.iterator.secondarykey;

import java.util.NoSuchElementException;

import com.hazelcast.elastic.offheapstorage.sorted.OrderingDirection;
import com.hazelcast.elastic.offheapstorage.sorted.secondarykey.OffHeapSecondaryKeyValueSortedStorage;

public class OffHeapSecondaryKeyIteratorImpl implements OffHeapSecondaryKeyIterator {
    private long pointer = 0L;
    private long keyEntryAddress;
    private OrderingDirection direction;
    private final OffHeapSecondaryKeyValueSortedStorage storage;

    public OffHeapSecondaryKeyIteratorImpl(
            OffHeapSecondaryKeyValueSortedStorage storage
    ) {
        this.storage = storage;
    }

    @Override
    public void setKeyEntry(long keyEntryAddress) {
        this.keyEntryAddress = keyEntryAddress;
        setDirection(OrderingDirection.ASC);
    }

    @Override
    public boolean hasNext() {
        return this.keyEntryAddress != 0L && this.pointer != 0L;
    }

    @Override
    public long next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        long result = this.pointer;
        this.pointer = this.storage.getNext(this.pointer, this.direction);
        return result;
    }

    @Override
    public void setDirection(OrderingDirection direction) {
        this.direction = direction;
        this.pointer = this.storage.firstSecondaryKeyEntry(direction, this.keyEntryAddress);
    }
}
