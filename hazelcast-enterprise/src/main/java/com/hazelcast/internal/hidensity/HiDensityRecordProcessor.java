package com.hazelcast.internal.hidensity;

import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.MemoryBlock;

/**
 * Record processor implementation for:
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
 * @param <R> type of the Hi-Density record to be accessed
 * @see com.hazelcast.internal.memory.MemoryBlockAccessor
 * @see com.hazelcast.internal.hidensity.HiDensityRecordAccessor
 * @see NativeMemoryData
 * @see com.hazelcast.internal.hidensity.HiDensityRecord
 */
public interface HiDensityRecordProcessor<R extends HiDensityRecord>
        extends MemoryBlockProcessor<R>, HiDensityRecordAccessor<R> {

    /**
     * Returns the used memory for the underlying Hi-Density storage (cache, ...).
     *
     * @return the used memory for the underlying Hi-Density storage (cache, ...)
     */
    long getUsedMemory();

    /**
     * Increases the used memory for the underlying Hi-Density storage (cache, ...) as the {@code increasedMemory}.
     *
     * @param increasedMemory the memory size to be added to used memory
     * @return the used memory for the underlying Hi-Density storage (cache, ...)
     */
    long increaseUsedMemory(long increasedMemory);

    /**
     * Decreases the used memory for the underlying Hi-Density storage (cache, ...) as the {@code decreasedMemory}.
     *
     * @param decreasedMemory the memory size to be removed from used memory
     * @return the used memory for the underlying Hi-Density storage (cache, ...)
     */
    long decreaseUsedMemory(long decreasedMemory);

    /**
     * Adds given {@link com.hazelcast.internal.memory.MemoryBlock}
     * to this {@link com.hazelcast.internal.hidensity.HiDensityRecordProcessor} to be disposed later.
     * Possibly at the end of (succeeded or failed) operation.
     *
     * @param memoryBlock the {@link com.hazelcast.internal.memory.MemoryBlock} to be disposed later
     */
    void addDeferredDispose(MemoryBlock memoryBlock);

    /**
     * Disposes added {@link com.hazelcast.internal.memory.MemoryBlock} instances.
     */
    void disposeDeferredBlocks();
}
