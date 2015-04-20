package com.hazelcast.hidensity;

import com.hazelcast.memory.MemoryManager;

/**
 * <p>
 * {@link HiDensityRecordStore} is the contract for Hi-Density specific record store operations.
 * </p>
 * <p>
 *
 * @param <R> Type of the hi-density record to be stored
 *
 * @see com.hazelcast.hidensity.HiDensityRecord
 *
 * @author sozal 18/02/15
 */
public interface HiDensityRecordStore<R extends HiDensityRecord> {

    /**
     * Constant value for hundred percent
     */
    int HUNDRED_PERCENT = 100;

    /**
     * Constant value for default forced eviction percentage
     */
    int DEFAULT_FORCED_EVICTION_PERCENTAGE = 20;

    /**
     * Constant value for force eviction try count
     */
    int FORCE_EVICTION_TRY_COUNT = HUNDRED_PERCENT / DEFAULT_FORCED_EVICTION_PERCENTAGE;

    /**
     * Constant value for representing the empty address
     */
    long NULL_PTR = MemoryManager.NULL_ADDRESS;

    /**
     * Forcefully evict records.
     *
     * @return evicted entry count
     */
    int forceEvict();

}
