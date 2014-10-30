package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.OffHeapData;

/**
 * Base class of all {@link CacheRecord} managed by {@link HiDensityCacheRecordStore}.
 *
 * @see com.hazelcast.cache.impl.record.CacheRecord
 * @see com.hazelcast.memory.MemoryBlock
 * @see com.hazelcast.nio.serialization.OffHeapData
 * @see com.hazelcast.cache.hidensity.HiDensityCacheRecordStore
 *
 * @author sozal 26/10/14
 */
public abstract class HiDensityCacheRecord
        extends MemoryBlock
        implements CacheRecord<OffHeapData> {

    public HiDensityCacheRecord() {
    }

    protected HiDensityCacheRecord(long address, int size) {
        super(address, size);
    }

    /**
     * Gets the creation time of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @return the creation time of this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract long getCreationTime();

    /**
     * Sets the creation time of this {@link HiDensityCacheRecord} in milliseconds.
     *
     * @param time the creation time for this {@link HiDensityCacheRecord} in milliseconds
     */
    public abstract void setCreationTime(long time);

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
     * Gets the access hit count of this {@link HiDensityCacheRecord}.
     *
     * @return the access hit count of this {@link HiDensityCacheRecord}
     */
    public abstract int getAccessHit();

    /**
     * Sets the access hit count of this {@link HiDensityCacheRecord}.
     *
     * @param hit the access hit count for this {@link HiDensityCacheRecord}
     */
    public abstract void setAccessHit(int hit);

    /**
     * Increases the access hit count of this {@link HiDensityCacheRecord} as <code>1</code>.
     */
    public abstract void incrementAccessHit();

    /**
     * Resets the access hit count of this {@link HiDensityCacheRecord} to <code>0</code>.
     */
    public abstract void resetAccessHit();

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

    /**
     * Gets the address of value for this {@link HiDensityCacheRecord}.
     *
     * @return the address of value for this {@link HiDensityCacheRecord}
     */
    public abstract long getValueAddress();

    /**
     * Sets the address of value for this {@link HiDensityCacheRecord}.
     *
     * @param valueAddress the address of value for this {@link HiDensityCacheRecord}
     */
    public abstract void setValueAddress(long valueAddress);

    /**
     * Resets the address of this {@link HiDensityCacheRecord} to specified address
     * and this {@link HiDensityCacheRecord} shows that allocated record area.
     *
     * @param address the address of allocated record area for this {@link HiDensityCacheRecord}
     * @return the reset {@link HiDensityCacheRecord} instance
     */
    public abstract HiDensityCacheRecord reset(long address);

    /**
     * Clears value (address), creation times, access times and etc ... of this {@link HiDensityCacheRecord}.
     */
    public abstract void clear();

}
