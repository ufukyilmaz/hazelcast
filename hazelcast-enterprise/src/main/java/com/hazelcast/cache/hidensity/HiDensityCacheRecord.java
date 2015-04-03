package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * Base class of all {@link CacheRecord} managed by {@link HiDensityCacheRecordStore}.
 *
 * @see com.hazelcast.hidensity.HiDensityRecord
 * @see com.hazelcast.cache.impl.record.CacheRecord
 * @see com.hazelcast.nio.serialization.NativeMemoryData
 * @see com.hazelcast.cache.hidensity.HiDensityCacheRecordStore
 *
 * @author sozal 26/10/14
 */
public abstract class HiDensityCacheRecord
        extends HiDensityRecord
        implements CacheRecord<NativeMemoryData> {

    protected HiDensityCacheRecord() {
    }

    protected HiDensityCacheRecord(long address, int size) {
        super(address, size);
    }

    /**
     * Gets the access time difference which represents the passed time since previous access
     * of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the access time difference of this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract int getAccessTimeDiff();

    /**
     * Sets the access time difference which represents the passed time since previous access
     * of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param time the access time difference of this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract void setAccessTimeDiff(int time);

    /**
     * Gets the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     */
    public abstract int getTtlMillis();

    /**
     * Sets the TTL value of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param ttl the TTL value for this {@link HiDensityCacheRecord} in milliseconds.
     */
    public abstract void setTtlMillis(int ttl);

}
