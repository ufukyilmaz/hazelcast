package com.hazelcast.cache.enterprise.impl.nativememory;

import com.hazelcast.cache.enterprise.BreakoutCacheRecord;
import com.hazelcast.cache.enterprise.BreakoutCacheRecordAccessor;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * @author sozal 14/10/14
 */
public final class BreakoutNativeMemoryCacheRecord extends BreakoutCacheRecord {

    /**
     * Header size of native memory based cache record
     */
    public static final int HEADER_SIZE = 8;
    /**
     * Value offset of native memory based cache record
     */
    public static final int VALUE_OFFSET = 16;
    /**
     * Size of native memory based cache record
     */
    public static final int SIZE = VALUE_OFFSET + HEADER_SIZE;

    static final int CREATION_TIME_OFFSET = 0;
    static final int ACCESS_TIME_OFFSET = 8;
    static final int ACCESS_HIT_OFFSET = ACCESS_TIME_OFFSET;
    static final int TTL_OFFSET = 12;

    private BreakoutCacheRecordAccessor<BreakoutNativeMemoryCacheRecord> cacheRecordAccessor;

    public BreakoutNativeMemoryCacheRecord(
            BreakoutCacheRecordAccessor<BreakoutNativeMemoryCacheRecord> cacheRecordAccessor) {
        this.cacheRecordAccessor = cacheRecordAccessor;
    }

    public BreakoutNativeMemoryCacheRecord(
            BreakoutCacheRecordAccessor<BreakoutNativeMemoryCacheRecord> cacheRecordAccessor, long address) {
        super(address, SIZE);
        this.cacheRecordAccessor = cacheRecordAccessor;
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
    public BreakoutNativeMemoryCacheRecord reset(long address) {
        setAddress(address);
        setSize(SIZE);
        return this;
    }

    @Override
    public void clear() {
        writeLong(CREATION_TIME_OFFSET, 0L);
        setAccessTimeDiff(0);
        setTtlMillis(0);
        setValueAddress(BreakoutNativeMemoryCacheRecordStore.NULL_PTR);
    }

    @Override
    public NativeMemoryData getValue() {
        if (address == BreakoutNativeMemoryCacheRecordStore.NULL_PTR) {
            return null;
        } else {
            return cacheRecordAccessor.readData(address);
        }
    }

    @Override
    public void setValue(NativeMemoryData value) {
        if (value != null) {
            setValueAddress(value.address());
        } else {
            setValueAddress(BreakoutNativeMemoryCacheRecordStore.NULL_PTR);
        }
    }

    @Override
    public long getExpirationTime() {
        int ttlMillis = getTtlMillis();
        if (ttlMillis < 0) {
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
        return expirationTime > -1 && expirationTime <= now;
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

        BreakoutNativeMemoryCacheRecord record = (BreakoutNativeMemoryCacheRecord) o;

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
        if (address() >= BreakoutNativeMemoryCacheRecordStore.NULL_PTR) {
            return "BreakoutNativeMemoryCacheRecord{creationTime: " + getCreationTime()
                    + ", lastAccessTime: " + getAccessTimeDiff()
                    + ", ttl: " + getTtlMillis()
                    + ", valueAddress: " + getValueAddress()
                    + " }";
        } else {
            return "BreakoutNativeMemoryCacheRecord{ NULL }";
        }
    }
}
