package com.hazelcast.elastic.binarystorage.iterator.secondarykey;

import com.hazelcast.elastic.binarystorage.iterator.BinaryKeyIterator;

/**
 * Iterator over secondary keys in off-heap storage sorted using primary and secondary
 * sort key.
 */
public interface BinarySecondaryKeyIterator extends BinaryKeyIterator {

    /**
     * Sets address of rootAddress to iterate over its keys.
     *
     * @param keyEntryAddress - address of the corresponding key address;
     */
    void setKeyEntry(long keyEntryAddress);
}
