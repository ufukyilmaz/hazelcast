package com.hazelcast.internal.bplustree;

import com.hazelcast.internal.memory.MemoryAllocator;
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
     * @param memoryAllocator the memory allocator to use for conversion
     * @return the 8 bytes value which can be interpreted as off-heap key address or any other meaning.
     */
    long convertToNativeData(Comparable indexKey, MemoryAllocator memoryAllocator);

    /**
     *
     * @param address the index key address
     * @param memoryAllocator the memory allocator to use for conversion
     * @return the address of the cloned off-heap data
     */
    long convertToNativeData(long address, MemoryAllocator memoryAllocator);

    /**
     * Disposes the address to the pooled memory manager.
     * @param address the address
     * @param memoryAllocator the memory allocator to dispose the address to
     */
    void disposeNativeData(long address, MemoryAllocator memoryAllocator);

}
