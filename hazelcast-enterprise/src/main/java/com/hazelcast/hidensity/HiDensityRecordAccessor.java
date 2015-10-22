package com.hazelcast.hidensity;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Record accessor implementation for:
 *
 * <ul>
 * <li>
 * Creating a new {@link HiDensityRecord}
 * </li>
 * <li>
 * Accessing the data and value of {@link HiDensityRecord}
 * </li>
 * <li>
 * Disposing the data and value of {@link HiDensityRecord}
 * </li>
 * </ul>
 *
 * @param <R> Type of the hi-density record to be accessed
 *
 * @see com.hazelcast.memory.MemoryBlockAccessor
 * @see NativeMemoryData
 * @see com.hazelcast.hidensity.HiDensityRecord
 *
 * @author sozal 18/02/15
 */
public interface HiDensityRecordAccessor<R extends HiDensityRecord>
        extends MemoryBlockAccessor<R> {

    /**
     * Creates an empty {@link HiDensityRecord}
     *
     * @return the created {@link HiDensityRecord}
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
     * Reads the value of specified {@link HiDensityRecord}.
     *
     * @param record The {@link HiDensityRecord} whose value will be read
     * @return the value of specified {@link HiDensityRecord}
     */
    Object readValue(R record);

    /**
     * Disposes (frees) the value of the specified {@link HiDensityRecord}.
     *
     * @param record The {@link HiDensityRecord} whose value will be disposed
     * @return size of disposed {@link HiDensityRecord}
     */
    long disposeValue(R record);

    /**
     * Gets the size of given <code>memoryBlock</code>.
     *
     * @param memoryBlock {@link MemoryBlock} whose size will be calculated
     * @return the size of memory block
     */
    int getSize(MemoryBlock memoryBlock);

    /**
     * Gets the size of given <code>address</code>.
     *
     * @param address       the address whose size will be calculated
     * @param expectedSize  the expected size of address
     * @return the size of address
     */
    long getSize(long address, long expectedSize);

}
