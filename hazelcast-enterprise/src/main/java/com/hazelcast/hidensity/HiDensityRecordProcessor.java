package com.hazelcast.hidensity;

import com.hazelcast.memory.MemoryBlockProcessor;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;

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
 * @param <R> Type of the hi-density record to be accessed
 *
 * @see com.hazelcast.memory.MemoryBlockAccessor
 * @see com.hazelcast.hidensity.HiDensityRecordAccessor
 * @see NativeMemoryData
 * @see com.hazelcast.hidensity.HiDensityRecord
 *
 * @author sozal 18/02/15
 */
public interface HiDensityRecordProcessor<R extends HiDensityRecord>
        extends MemoryBlockProcessor<R>, HiDensityRecordAccessor<R> {

    long getUsedMemory();

}
