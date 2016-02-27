package com.hazelcast.elastic.binarystorage.iterator.value;

import com.hazelcast.elastic.binarystorage.BinaryKeyValueStorage;

import java.util.NoSuchElementException;

/**
 * Iterator over values in off-heap storage.
 */
public class BinaryValueIteratorImpl implements BinaryValueIterator {

    private final BinaryKeyValueStorage binaryRedBlackTree;

    private long nextValueEntryPointer;

    public BinaryValueIteratorImpl(BinaryKeyValueStorage binaryRedBlackTree) {
        this.binaryRedBlackTree = binaryRedBlackTree;
    }

    @Override
    public boolean hasNext() {
        if (binaryRedBlackTree == null) {
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
        nextValueEntryPointer = binaryRedBlackTree.getNextValueEntryAddress(nextValueEntryPointer);
        return nextPointer;
    }

    @Override
    public void reset(long keyEntryPointer) {
        if (binaryRedBlackTree == null) {
            nextValueEntryPointer = 0L;
            return;
        }

        if (keyEntryPointer == 0L) {
            nextValueEntryPointer = 0L;
        } else {
            nextValueEntryPointer = binaryRedBlackTree.getValueEntryAddress(keyEntryPointer);
        }
    }
}
