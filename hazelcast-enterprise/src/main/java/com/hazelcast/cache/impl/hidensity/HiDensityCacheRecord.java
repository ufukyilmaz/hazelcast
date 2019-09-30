package com.hazelcast.cache.impl.hidensity;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.memory.GlobalMemoryAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Base class of all {@link CacheRecord} managed by {@link HiDensityCacheRecordStore}.
 *
 * @see com.hazelcast.internal.hidensity.HiDensityRecord
 * @see com.hazelcast.cache.impl.record.CacheRecord
 * @see NativeMemoryData
 * @see com.hazelcast.cache.impl.hidensity.HiDensityCacheRecordStore
 */
public abstract class HiDensityCacheRecord
        extends HiDensityRecord
        implements CacheRecord<NativeMemoryData, NativeMemoryData> {

    protected HiDensityCacheRecord(GlobalMemoryAccessor memoryAccessor) {
        super(memoryAccessor);
    }

    protected HiDensityCacheRecord(GlobalMemoryAccessor memoryAccessor, long address, int size) {
        super(memoryAccessor, address, size);
    }

    /**
     * Gets the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     */
    public abstract long getTtlMillis();

    /**
     * Sets the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param ttl the TTL value for this {@link HiDensityCacheRecord} in milliseconds.
     */
    public abstract void setTtlMillis(long ttl);


    public abstract long getExpiryPolicyAddress();

    public abstract void setExpiryPolicyAddress(long address);
}
