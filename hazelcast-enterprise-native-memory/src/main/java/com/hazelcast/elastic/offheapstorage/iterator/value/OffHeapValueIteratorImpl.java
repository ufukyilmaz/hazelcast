package com.hazelcast.elastic.offheapstorage.iterator.value;

import java.util.NoSuchElementException;

import com.hazelcast.elastic.offheapstorage.OffHeapKeyValueStorage;

/**
 * Iterator over values in off-heap storage.
 */
public class OffHeapValueIteratorImpl implements OffHeapValueIterator {
    private final OffHeapKeyValueStorage offHeapRedBlackTree;

    private long nextValueEntryPointer;

    public OffHeapValueIteratorImpl(OffHeapKeyValueStorage offHeapRedBlackTree) {
        this.offHeapRedBlackTree = offHeapRedBlackTree;
    }

    @Override
    public boolean hasNext() {
        if (this.offHeapRedBlackTree == null) {
            return false;
        }

        return this.nextValueEntryPointer != 0L;
    }

    @Override
    public long next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No next element for iteration");
        }

        long nextPointer = this.nextValueEntryPointer;
        this.nextValueEntryPointer = this.offHeapRedBlackTree.getNextValueEntryAddress(this.nextValueEntryPointer);
        return nextPointer;
    }

    @Override
    public void reset(long keyEntryPointer) {
        if (this.offHeapRedBlackTree == null) {
            this.nextValueEntryPointer = 0L;
            return;
        }

        if (keyEntryPointer == 0L) {
            this.nextValueEntryPointer = 0L;
        } else {
            this.nextValueEntryPointer = this.offHeapRedBlackTree.getValueEntryAddress(keyEntryPointer);
        }
    }
}
