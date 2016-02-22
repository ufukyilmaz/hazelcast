package com.hazelcast.elastic.offheapstorage.iterator.secondarykey;

import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyIterator;

/**
 * Iterator over secondary keys in off-heap storage sorted using primary and secondary
 * sort key.
 */
public interface OffHeapSecondaryKeyIterator extends OffHeapKeyIterator {

    /**
     * Sets address of rootAddress to iterate over its keys.
     *
     * @param keyEntryAddress - address of the corresponding key address;
     */
    void setKeyEntry(long keyEntryAddress);
}
