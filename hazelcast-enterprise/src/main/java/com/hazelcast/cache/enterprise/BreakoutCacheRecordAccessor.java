package com.hazelcast.cache.enterprise;

import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * Record accessor implementation for:
 *
 * <ul>
 * <li>
 * Creating a new {@link BreakoutCacheRecord}
 * </li>
 * <li>
 * Accessing the data and value of {@link BreakoutCacheRecord}
 * </li>
 * <li>
 * Disposing the data and value of {@link BreakoutCacheRecord}
 * </li>
 * </ul>
 *
 * Implementations of {@link BreakoutCacheRecordAccessor} are used by related
 * {@link BreakoutCacheRecordStore} and {@link BreakoutCacheRecordMap} for doing operations on records.
 *
 * @param <R> Type of the cache record to be accessed
 *
 * @see com.hazelcast.memory.MemoryBlockAccessor
 * @see com.hazelcast.nio.serialization.NativeMemoryData
 * @see BreakoutCacheRecord
 *
 * @author sozal 18/10/14
 */
public interface BreakoutCacheRecordAccessor<R extends BreakoutCacheRecord>
        extends MemoryBlockAccessor<R> {

    /**
     * Creates an empty {@link BreakoutCacheRecord}
     *
     * @return the created {@link BreakoutCacheRecord}
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
     * Reads the value of specified {@link BreakoutCacheRecord}.
     *
     * @param record The {@link BreakoutCacheRecord} whose value will be read
     * @param enqueueDataOnFinish condition about data is enqueued or not for future uses
     * @return the value of specified {@link BreakoutCacheRecord}
     */
    Object readValue(R record, boolean enqueueDataOnFinish);

    /**
     * Disposes (frees) the value of the specified {@link BreakoutCacheRecord}.
     *
     * @param record The {@link BreakoutCacheRecord} whose value will be disposed
     */
    void disposeValue(R record);

    /**
     * Disposes (frees) the data of the specified {@link BreakoutCacheRecord}.
     *
     * @param data The {@link com.hazelcast.nio.serialization.NativeMemoryData} whose value will be disposed
     */
    void disposeData(NativeMemoryData data);

    /**
     * Disposes (frees) the data at the specified <code>address</code>.
     *
     * @param address the address of the {@link com.hazelcast.nio.serialization.NativeMemoryData} whose value will be disposed
     */
    void disposeData(long address);

}
