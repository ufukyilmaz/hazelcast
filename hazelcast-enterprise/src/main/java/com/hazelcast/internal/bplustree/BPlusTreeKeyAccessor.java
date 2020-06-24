package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.serialization.Data;

/**
 * An accessor for the index key component in the B+tree index.
 * Can be used to abstract interpretation of the 8 bytes index key address value.
 */
public interface BPlusTreeKeyAccessor {


    /**
     * @param address the index key address
     * @return the Comparable instance referenced by the provided address
     */
    Comparable convertToObject(long address);

    /**
     *
     * @param address the index key address
     * @return the cloned on-heap Data instance represented by the provided address
     */
    Data convertToHeapData(long address);

    /**
     * @param indexKey the index key
     * @return the 8 bytes value which can be interpreted as off-heap key address or any other meaning.
     */
    long convertToNativeData(Comparable indexKey);

    /**
     *
     * @param address the index key address
     * DataType type the data type to convert to
     * @return the address of the cloned off-heap data
     */
    long convertToNativeData(long address);

    /**
     * Disposes the address to the pooled memory manager.
     * @param address the address
     */
    void disposeNativeData(long address);

}
