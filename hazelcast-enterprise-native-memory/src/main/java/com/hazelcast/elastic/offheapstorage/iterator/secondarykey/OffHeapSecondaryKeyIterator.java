package com.hazelcast.elastic.offheapstorage.iterator.secondarykey;

import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyIterator;

/**
 * Iterator over secondary keys stored in a red-black tree.
 */
public interface OffHeapSecondaryKeyIterator extends OffHeapKeyIterator {
    /***
     * Set RB-tree rootAddress's address to iterate over it's keys;
     *
     * @param keyEntryAddress - address of the corresponding key address;
     */
    void setKeyEntry(long keyEntryAddress);
}
