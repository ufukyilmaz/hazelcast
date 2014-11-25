package com.hazelcast.cache.hidensity;

import com.hazelcast.memory.MemoryBlockProcessor;

/**
 * Record processor implementation for:
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
 * @author sozal 17/11/14
 */
public interface HiDensityCacheRecordProcessor<R extends HiDensityCacheRecord>
        extends MemoryBlockProcessor<R>, HiDensityCacheRecordAccessor<R> {

    long getUsedMemory();

}
