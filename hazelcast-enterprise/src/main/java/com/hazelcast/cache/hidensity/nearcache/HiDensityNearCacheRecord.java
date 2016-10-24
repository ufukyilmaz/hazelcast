package com.hazelcast.cache.hidensity.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCacheRecord;
import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * Represents {@link NearCacheRecord} implementations for Hi-Density Near Cache.
 *
 * Record of {@link com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecordStore}.
 *
 * @see com.hazelcast.internal.hidensity.HiDensityRecord
 * @see com.hazelcast.cache.impl.nearcache.NearCacheRecord
 * @see NativeMemoryData
 * @see com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecordStore
 */
public abstract class HiDensityNearCacheRecord
        extends HiDensityRecord
        implements NearCacheRecord<NativeMemoryData> {

    protected HiDensityNearCacheRecord() {
    }

    /**
     * Gets the access time difference which represents the passed time since previous access
     * of this {@link HiDensityNearCacheRecord} in milliseconds.
     *
     * @return the access time difference of this {@link HiDensityNearCacheRecord} in milliseconds.
     */
    public abstract int getAccessTimeDiff();

    /**
     * Sets the access time difference which represents the passed time since previous access
     * of this {@link HiDensityNearCacheRecord} in milliseconds.
     *
     * @param time the access time difference of this {@link HiDensityNearCacheRecord} in milliseconds.
     */
    public abstract void setAccessTimeDiff(int time);

    /**
     * Gets the TTL value of this {@link HiDensityNearCacheRecord} in milliseconds.
     *
     * @return the TTL value of this {@link HiDensityNearCacheRecord} in milliseconds.
     */
    public abstract int getTtlMillis();

    /**
     * Sets the TTL value of this {@link HiDensityNearCacheRecord} in milliseconds.
     *
     * @param ttl the TTL value for this {@link HiDensityNearCacheRecord} in milliseconds.
     */
    public abstract void setTtlMillis(int ttl);
}
