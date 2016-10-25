package com.hazelcast.internal.hidensity;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Record processor implementation for:
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
 * @param <R> Type of the Hi-Density record to be accessed.
 *
 * @see com.hazelcast.memory.MemoryBlockAccessor
 * @see com.hazelcast.internal.hidensity.HiDensityRecordAccessor
 * @see NativeMemoryData
 * @see com.hazelcast.internal.hidensity.HiDensityRecord
 *
 * @author sozal 18/02/15
 */
public interface HiDensityRecordProcessor<R extends HiDensityRecord>
        extends MemoryBlockProcessor<R>, HiDensityRecordAccessor<R> {

    /**
     * Returns the used memory for the underlying Hi-Density storage (cache, ...).
     *
     * @return the used memory for the underlying Hi-Density storage (cache, ...).
     */
    long getUsedMemory();

    /**
     * Increases the used memory for the underlying Hi-Density storage (cache, ...) as the <code>increasedMemory</code>.
     *
     * @param increasedMemory the memory size to be added to used memory.
     * @return the used memory for the underlying Hi-Density storage (cache, ...).
     */
    long increaseUsedMemory(long increasedMemory);

    /**
     * Decreases the used memory for the underlying Hi-Density storage (cache, ...) as the <code>decreasedMemory</code>.
     *
     * @param decreasedMemory the memory size to be removed from used memory.
     * @return the used memory for the underlying Hi-Density storage (cache, ...).
     */
    long decreaseUsedMemory(long decreasedMemory);

    /**
     * Adds given {@link com.hazelcast.memory.MemoryBlock}
     * to this {@link com.hazelcast.internal.hidensity.HiDensityRecordProcessor} to be disposed later.
     * Possibly at the end of (succeeded or failed) operation.
     *
     * @param memoryBlock the {@link com.hazelcast.memory.MemoryBlock} to be disposed later.
     */
    void addDeferredDispose(MemoryBlock memoryBlock);

    /**
     * Disposes added {@link com.hazelcast.memory.MemoryBlock} instances.
     */
    void disposeDeferredBlocks();

}
