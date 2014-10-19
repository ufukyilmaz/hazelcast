package com.hazelcast.cache.enterprise.impl.offheap;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * @author sozal 14/10/14
 */
public final class CacheOffHeapRecord extends MemoryBlock implements CacheRecord<OffHeapData> {

    static final int CREATION_TIME_OFFSET = 0;
    static final int ACCESS_TIME_OFFSET = 8;
    static final int ACCESS_HIT_OFFSET = ACCESS_TIME_OFFSET;
    static final int TTL_OFFSET = 12;
    public static final int VALUE_OFFSET = 16;

    public static final int SIZE = VALUE_OFFSET + 8;

    public CacheOffHeapRecord() {
    }

    public CacheOffHeapRecord(long address) {
        super(address, SIZE);
    }

    public long getCreationTime() {
        return readLong(CREATION_TIME_OFFSET);
    }

    public int getAccessTimeDiff() {
        return readInt(ACCESS_TIME_OFFSET);
    }

    public int getAccessHit() {
        return readInt(ACCESS_HIT_OFFSET);
    }

    public int getTtlMillis() {
        return readInt(TTL_OFFSET);
    }

    public long getValueAddress() {
        return readLong(VALUE_OFFSET);
    }

    public void setCreationTime(long time) {
        writeLong(CREATION_TIME_OFFSET, time);
    }

    public void setAccessTimeDiff(int time) {
        writeInt(ACCESS_TIME_OFFSET, time);
    }

    public void resetAccessHit() {
        writeInt(ACCESS_HIT_OFFSET, 0);
    }

    public void incrementAccessHit() {
        int hit = getAccessHit();
        writeInt(ACCESS_HIT_OFFSET, hit + 1);
    }

    public void setTtlMillis(int ttl) {
        writeInt(TTL_OFFSET, ttl);
    }

    public void setValueAddress(long valueAddress) {
        writeLong(VALUE_OFFSET, valueAddress);
    }

    public CacheOffHeapRecord reset(long address) {
        setAddress(address);
        setSize(SIZE);
        return this;
    }

    public void clear() {
        writeLong(CREATION_TIME_OFFSET, 0L);
        setAccessTimeDiff(0);
        setTtlMillis(0);
        setValueAddress(OffHeapCacheRecordStore.NULL_PTR);
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
    public OffHeapData getValue() {
        // TODO
        //      Maybe "cacheRecordService.readData(address);" can be used.
        //      But in that case, "cacheRecordService" may be passed as argument to constructor
        //      or it may be taken from a singleton context
        throw new UnsupportedOperationException(
                "\"<V extends OffHeapData> V getValue()\" is not supported !");

    }

    @Override
    public void setValue(OffHeapData value) {
        if (value != null) {
            setValueAddress(value.address());
        } else {
            setValueAddress(OffHeapCacheRecordStore.NULL_PTR);
        }
    }

    @Override
    public long getExpirationTime() {
        return getCreationTime() + getTtlMillis();
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        long creationTime = getCreationTime();
        int newTtl =
                expirationTime > creationTime
                        ? (int) (expirationTime - creationTime)
                        : -1;
        if (newTtl > 0) {
            setTtlMillis(newTtl);
        }
    }

    @Override
    public boolean isExpiredAt(long now) {
        long expirationTime = getExpirationTime();
        return expirationTime > -1 && expirationTime <= now;
    }

    @Override
    public String toString() {
        if (address() >= OffHeapCacheRecordStore.NULL_PTR) {
            return "EnterpriseOffHeapCacheRecord{creationTime: " + getCreationTime()
                    + ", lastAccessTime: " + getAccessTimeDiff()
                    + ", ttl: " + getTtlMillis()
                    + ", valueAddress: " + getValueAddress()
                    + " }";
        } else {
            return "EnterpriseOffHeapCacheRecord{ NULL }";
        }
    }
}
