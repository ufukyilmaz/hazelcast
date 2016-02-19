package com.hazelcast.elastic.offheapstorage.iterator.value;

import com.hazelcast.elastic.offheapstorage.OffHeapKeyValueStorage;

import java.util.NoSuchElementException;

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
        if (offHeapRedBlackTree == null) {
            return false;
        }

        return nextValueEntryPointer != 0L;
    }

    @Override
    public long next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No next element for iteration");
        }

        long nextPointer = nextValueEntryPointer;
        nextValueEntryPointer = offHeapRedBlackTree.getNextValueEntryAddress(nextValueEntryPointer);
        return nextPointer;
    }

    @Override
    public void reset(long keyEntryPointer) {
        if (offHeapRedBlackTree == null) {
            nextValueEntryPointer = 0L;
            return;
        }

        if (keyEntryPointer == 0L) {
            nextValueEntryPointer = 0L;
        } else {
            nextValueEntryPointer = offHeapRedBlackTree.getValueEntryAddress(keyEntryPointer);
        }
    }
}
