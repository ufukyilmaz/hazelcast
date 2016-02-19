package com.hazelcast.elastic.offheapstorage.iterator.value;

/**
 * API to iterate over values in off-heap storage
 */
public interface OffHeapValueIterator {

    /**
     * @return true if there are more elements to fetch, false otherwise
     */
    boolean hasNext();

    /**
     * @return next pointer to value entry
     */
    long next();

    /**
     * Set up key entry address to iterate over entries
     *
     * @param keyEntryPointer - address of the key entry
     */
    void reset(long keyEntryPointer);
}
