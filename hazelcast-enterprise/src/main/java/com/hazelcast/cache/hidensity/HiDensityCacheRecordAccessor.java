package com.hazelcast.cache.hidensity;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * Record accessor implementation for:
 *
 * <ul>
 * <li>
 * Creating a new {@link HiDensityCacheRecord}
 * </li>
 * <li>
 * Accessing the data and value of {@link HiDensityCacheRecord}
 * </li>
 * <li>
 * Disposing the data and value of {@link HiDensityCacheRecord}
 * </li>
 * </ul>
 *
 * Implementations of {@link HiDensityCacheRecordAccessor} are used by related
 * {@link HiDensityCacheRecordStore} and {@link HiDensityCacheRecordMap} for doing operations on records.
 *
 * @param <R> Type of the cache record to be accessed
 *
 * @see com.hazelcast.memory.MemoryBlockAccessor
 * @see com.hazelcast.nio.serialization.NativeMemoryData
 * @see HiDensityCacheRecord
 *
 * @author sozal 18/10/14
 */
public interface HiDensityCacheRecordAccessor<R extends HiDensityCacheRecord>
        extends MemoryBlockAccessor<R> {

    /**
     * Creates an empty {@link HiDensityCacheRecord}
     *
     * @return the created {@link HiDensityCacheRecord}
     */
    R newRecord();

    /**
     * Reads an off-heap based data from given <code>address</code>.
     *
     * @param valueAddress The address of the data stored as off-heap
     * @return the data stored as off-heap
     */
    NativeMemoryData readData(long valueAddress);

    /**
     * Reads the value of specified {@link HiDensityCacheRecord}.
     *
     * @param record The {@link HiDensityCacheRecord} whose value will be read
     * @param enqueueDataOnFinish condition about data is enqueued or not for future uses
     * @return the value of specified {@link HiDensityCacheRecord}
     */
    Object readValue(R record, boolean enqueueDataOnFinish);

    /**
     * Disposes (frees) the value of the specified {@link HiDensityCacheRecord}.
     *
     * @param record The {@link HiDensityCacheRecord} whose value will be disposed
     * @return size of disposed {@link HiDensityCacheRecord}
     */
    long disposeValue(R record);

    /**
     * Disposes (frees) the specified {@link NativeMemoryData}.
     *
     * @param data The {@link NativeMemoryData} whose value will be disposed
     * @return size of disposed data
     */
    long disposeData(NativeMemoryData data);

    /**
     * Disposes (frees) the data at the specified <code>address</code>.
     *
     * @param address the address of the {@link NativeMemoryData} whose value will be disposed
     * @return size of disposed data at given <code>address</code>
     */
    long disposeData(long address);

    /**
     * Enqueues (caches) the specified {@link HiDensityCacheRecord} for later usages.
     *
     * @param record The {@link HiDensityCacheRecord} that will be enqueued
     */
    void enqueueRecord(R record);

    /**
     * Enqueues (caches) the specified {@link NativeMemoryData} for later usages.
     *
     * @param data The {@link NativeMemoryData} that will be enqueued
     */
    void enqueueData(NativeMemoryData data);

    /**
     * Gets the allocated memory size for given <code>address</code>.
     *
     * @param address       the address whose allocated memory size will be calculated
     * @param expectedSize  the expected memory size for given <code>address</code>
     *                      if it is not associated with any existing memory managers
     * @return the allocated memory size for given <code>address</code>
     */
    long getSize(long address, long expectedSize);

    /**
     * Gets the allocated memory size for given <code>memoryBlock</code>.
     *
     * @param memoryBlock the {@link MemoryBlock} whose allocated memory size will be calculated
     * @return the allocated memory size for given <code>memoryBlock</code>
     */
    long getSize(MemoryBlock memoryBlock);

}
