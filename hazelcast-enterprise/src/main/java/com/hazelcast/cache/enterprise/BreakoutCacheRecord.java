package com.hazelcast.cache.enterprise;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * Base class of all {@link CacheRecord} managed by {@link BreakoutCacheRecordStore}.
 *
 * @see com.hazelcast.cache.impl.record.CacheRecord
 * @see com.hazelcast.memory.MemoryBlock
 * @see com.hazelcast.nio.serialization.NativeMemoryData
 * @see BreakoutCacheRecordStore
 *
 * @author sozal 26/10/14
 */
public abstract class BreakoutCacheRecord
        extends MemoryBlock
        implements CacheRecord<NativeMemoryData> {

    public BreakoutCacheRecord() {
    }

    protected BreakoutCacheRecord(long address, int size) {
        super(address, size);
    }

    /**
     * Gets the creation time of this {@link BreakoutCacheRecord} in milliseconds.
     *
     * @return the creation time of this {@link BreakoutCacheRecord} in milliseconds
     */
    public abstract long getCreationTime();

    /**
     * Sets the creation time of this {@link BreakoutCacheRecord} in milliseconds.
     *
     * @param time the creation time for this {@link BreakoutCacheRecord} in milliseconds
     */
    public abstract void setCreationTime(long time);

    /**
     * Gets the access time difference which represents the passed time since previous access
     * of this {@link BreakoutCacheRecord} in milliseconds.
     *
     * @return the access time difference of this {@link BreakoutCacheRecord} in milliseconds
     */
    public abstract int getAccessTimeDiff();

    /**
     * Sets the access time difference which represents the passed time since previous access
     * of this {@link BreakoutCacheRecord} in milliseconds.
     *
     * @param time the access time difference of this {@link BreakoutCacheRecord} in milliseconds
     */
    public abstract void setAccessTimeDiff(int time);

    /**
     * Gets the access hit count of this {@link BreakoutCacheRecord}.
     *
     * @return the access hit count of this {@link BreakoutCacheRecord}
     */
    public abstract int getAccessHit();

    /**
     * Sets the access hit count of this {@link BreakoutCacheRecord}.
     *
     * @param hit the access hit count for this {@link BreakoutCacheRecord}
     */
    public abstract void setAccessHit(int hit);

    /**
     * Increases the access hit count of this {@link BreakoutCacheRecord} as <code>1</code>.
     */
    public abstract void incrementAccessHit();

    /**
     * Resets the access hit count of this {@link BreakoutCacheRecord} to <code>0</code>.
     */
    public abstract void resetAccessHit();

    /**
     * Gets the TTL value of this {@link BreakoutCacheRecord} in milliseconds.
     *
     * @return the TTL value of this {@link BreakoutCacheRecord} in milliseconds.
     */
    public abstract int getTtlMillis();

    /**
     * Sets the TTL value of this {@link BreakoutCacheRecord} in milliseconds.
     *
     * @param ttl the TTL value for this {@link BreakoutCacheRecord} in milliseconds.
     */
    public abstract void setTtlMillis(int ttl);

    /**
     * Gets the address of value for this {@link BreakoutCacheRecord}.
     *
     * @return the address of value for this {@link BreakoutCacheRecord}
     */
    public abstract long getValueAddress();

    /**
     * Sets the address of value for this {@link BreakoutCacheRecord}.
     *
     * @param valueAddress the address of value for this {@link BreakoutCacheRecord}
     */
    public abstract void setValueAddress(long valueAddress);

    /**
     * Resets the address of this {@link BreakoutCacheRecord} to specified address
     * and this {@link BreakoutCacheRecord} shows that allocated record area.
     *
     * @param address the address of allocated record area for this {@link BreakoutCacheRecord}
     * @return the reset {@link BreakoutCacheRecord} instance
     */
    public abstract BreakoutCacheRecord reset(long address);

    /**
     * Clears value (address), creation times, access times and etc ... of this {@link BreakoutCacheRecord}.
     */
    public abstract void clear();

}
