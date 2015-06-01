package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheRecord;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * @author sozal 26/10/14
 */
public class HiDensityNativeMemoryNearCacheRecord
        extends HiDensityNearCacheRecord {

    /**
     * Header size of native memory based near-cache record
     */
    public static final int HEADER_SIZE = 8;
    /**
     * Value offset of native memory based near-cache record
     */
    public static final int VALUE_OFFSET;
    /**
     * Size of native memory based near-cache record
     */
    public static final int SIZE;

    static final int CREATION_TIME_OFFSET = 0;
    static final int ACCESS_TIME_OFFSET = Bits.LONG_SIZE_IN_BYTES;
    // "ACCESS_HIT_OFFSET" and "ACCESS_TIME_OFFSET` is same for near-cache record
    // since these fields (access hit count and access time) are not used at same time.
    // Because their usage scenario is based on eviction type (LRU or LFU).
    static final int ACCESS_HIT_OFFSET = ACCESS_TIME_OFFSET;
    static final int TTL_OFFSET = ACCESS_HIT_OFFSET + Bits.INT_SIZE_IN_BYTES;

    static {
        VALUE_OFFSET = TTL_OFFSET + Bits.INT_SIZE_IN_BYTES;
        SIZE = VALUE_OFFSET + HEADER_SIZE;
    }

    private HiDensityRecordAccessor<HiDensityNativeMemoryNearCacheRecord> nearCacheRecordAccessor;

    public HiDensityNativeMemoryNearCacheRecord(
            HiDensityRecordAccessor<HiDensityNativeMemoryNearCacheRecord> nearCacheRecordAccessor) {
        this.nearCacheRecordAccessor = nearCacheRecordAccessor;
    }

    public HiDensityNativeMemoryNearCacheRecord(
            HiDensityRecordAccessor<HiDensityNativeMemoryNearCacheRecord> nearCacheRecordAccessor,
            long address) {
        super(address, SIZE);
        this.nearCacheRecordAccessor = nearCacheRecordAccessor;
    }

    @Override
    public long getCreationTime() {
        return readLong(CREATION_TIME_OFFSET);
    }

    @Override
    public void setCreationTime(long time) {
        writeLong(CREATION_TIME_OFFSET, time);
    }

    @Override
    public long getAccessTime() {
        int accessTimeDiff = getAccessTimeDiff();
        // Not accessed yet
        if (accessTimeDiff <= 0) {
            return -1;
        }
        return getCreationTime() + accessTimeDiff;
    }

    @Override
    public void setAccessTime(long time) {
        long accessTimeDiff = time - getCreationTime();
        if (accessTimeDiff > Integer.MAX_VALUE) {
            accessTimeDiff = Integer.MAX_VALUE;
        }
        setAccessTimeDiff((int) accessTimeDiff);
    }

    @Override
    public int getAccessTimeDiff() {
        return readInt(ACCESS_TIME_OFFSET);
    }

    @Override
    public void setAccessTimeDiff(int time) {
        writeInt(ACCESS_TIME_OFFSET, time);
    }

    @Override
    public int getAccessHit() {
        return readInt(ACCESS_HIT_OFFSET);
    }

    @Override
    public void setAccessHit(int hit) {
        writeInt(ACCESS_HIT_OFFSET, hit);
    }

    @Override
    public void incrementAccessHit() {
        int hit = getAccessHit();
        writeInt(ACCESS_HIT_OFFSET, hit + 1);
    }

    @Override
    public void resetAccessHit() {
        writeInt(ACCESS_HIT_OFFSET, 0);
    }

    @Override
    public int getTtlMillis() {
        return readInt(TTL_OFFSET);
    }

    @Override
    public void setTtlMillis(int ttl) {
        writeInt(TTL_OFFSET, ttl);
    }

    @Override
    public long getValueAddress() {
        return readLong(VALUE_OFFSET);
    }

    @Override
    public void setValueAddress(long valueAddress) {
        writeLong(VALUE_OFFSET, valueAddress);
    }

    @Override
    public HiDensityNativeMemoryNearCacheRecord reset(long address) {
        setAddress(address);
        setSize(SIZE);
        return this;
    }

    @Override
    public void clear() {
        writeLong(CREATION_TIME_OFFSET, 0L);
        setAccessTimeDiff(0);
        setTtlMillis(0);
        setValueAddress(HiDensityNativeMemoryCacheRecordStore.NULL_PTR);
    }

    @Override
    public NativeMemoryData getValue() {
        long valueAddress = getValueAddress();
        if (valueAddress == HiDensityNativeMemoryCacheRecordStore.NULL_PTR) {
            return null;
        } else {
            return nearCacheRecordAccessor.readData(valueAddress);
        }
    }

    @Override
    public void setValue(NativeMemoryData value) {
        if (value != null) {
            setValueAddress(value.address());
        } else {
            setValueAddress(HiDensityNativeMemoryCacheRecordStore.NULL_PTR);
        }
    }

    @Override
    public long getExpirationTime() {
        int ttlMillis = getTtlMillis();
        if (ttlMillis <= 0) {
            return Long.MAX_VALUE;
        }
        return getCreationTime() + ttlMillis;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        long creationTime = getCreationTime();
        long timeDiff = expirationTime - creationTime;
        int newTtl =
                expirationTime >= creationTime
                        ? (timeDiff > Integer.MAX_VALUE ? -1 : (int) timeDiff)
                        : -1;
        setTtlMillis(newTtl);
    }

    @Override
    public boolean isExpiredAt(long now) {
        long expirationTime = getExpirationTime();
        return (expirationTime > TIME_NOT_SET) && (expirationTime <= now);
    }

    @Override
    public boolean isIdleAt(long maxIdleMilliSeconds, long now) {
        long accessTime = getAccessTime();
        if (maxIdleMilliSeconds > 0) {
            if (accessTime > TIME_NOT_SET) {
                return accessTime + maxIdleMilliSeconds < now;
            } else {
                long creationTime = getCreationTime();
                return creationTime + maxIdleMilliSeconds < now;
            }
        } else {
            return false;
        }
    }

    public static long getCreationTime(long address) {
        return UnsafeHelper.UNSAFE.getLong(address + CREATION_TIME_OFFSET);
    }

    public static int getAccessTimeDiff(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + ACCESS_TIME_OFFSET);
    }

    public static int getAccessHit(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + ACCESS_HIT_OFFSET);
    }

    public static int getTtlMillis(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + TTL_OFFSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HiDensityNativeMemoryNearCacheRecord record = (HiDensityNativeMemoryNearCacheRecord) o;

        if (address != record.address) {
            return false;
        }
        if (size != record.size) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (address ^ (address >>> 32));
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        if (address() > HiDensityNativeMemoryCacheRecordStore.NULL_PTR) {
            return "HiDensityNearCacheNativeMemoryRecord{creationTime: " + getCreationTime()
                    + ", lastAccessTime: " + getAccessTimeDiff()
                    + ", ttl: " + getTtlMillis()
                    + ", valueAddress: " + getValueAddress()
                    + " }";
        } else {
            return "HiDensityNearCacheNativeMemoryRecord{ NULL }";
        }
    }

}
