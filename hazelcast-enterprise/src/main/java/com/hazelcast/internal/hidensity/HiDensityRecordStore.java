package com.hazelcast.internal.hidensity;

import com.hazelcast.internal.memory.HazelcastMemoryManager;

/**
 * {@link HiDensityRecordStore} is the contract for Hi-Density specific record store operations.
 *
 * @param <R> Type of the Hi-Density record to be stored.
 * @see com.hazelcast.internal.hidensity.HiDensityRecord
 */
public interface HiDensityRecordStore<R extends HiDensityRecord> {

    /**
     * Constant value for hundred percent.
     */
    int HUNDRED_PERCENT = 100;

    /**
     * Constant value for default forced eviction percentage.
     */
    int DEFAULT_FORCED_EVICTION_PERCENTAGE = 20;

    /**
     * Constant value for forced eviction try count.
     */
    int FORCE_EVICTION_TRY_COUNT = HUNDRED_PERCENT / DEFAULT_FORCED_EVICTION_PERCENTAGE;

    /**
     * Constant value for representing the empty address.
     */
    long NULL_PTR = HazelcastMemoryManager.NULL_ADDRESS;

    /**
     * Forcefully evict records.
     *
     * @return evicted entry count.
     */
    int forceEvict();
}
