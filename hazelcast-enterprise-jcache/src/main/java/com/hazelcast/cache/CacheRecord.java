package com.hazelcast.cache;

import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.nio.UnsafeHelper;

/**
 * @author mdogan 11/02/14
 */
public final class CacheRecord extends MemoryBlock {

    static final int CREATION_TIME_OFFSET = 0;
    static final int ACCESS_TIME_OFFSET = 8;
    static final int ACCESS_HIT_OFFSET = ACCESS_TIME_OFFSET;
    static final int TTL_OFFSET = 12;
    public static final int VALUE_OFFSET = 16;

    public static final int SIZE = VALUE_OFFSET + 8;

    public CacheRecord() {
    }

    CacheRecord(long address) {
        super(address, SIZE);
    }

    long getCreationTime() {
        return readLong(CREATION_TIME_OFFSET);
    }

    int getAccessTimeDiff() {
        return readInt(ACCESS_TIME_OFFSET);
    }

    int getAccessHit() {
        return readInt(ACCESS_HIT_OFFSET);
    }

    int getTtlMillis() {
        return readInt(TTL_OFFSET);
    }

    public long getValueAddress() {
        return readLong(VALUE_OFFSET);
    }

    public void setCreationTime(long time) {
        writeLong(CREATION_TIME_OFFSET, time);
    }

    void setAccessTimeDiff(int time) {
        writeInt(ACCESS_TIME_OFFSET, time);
    }

    void resetAccessHit() {
        writeInt(ACCESS_HIT_OFFSET, 0);
    }

    void incrementAccessHit() {
        int hit = getAccessHit();
        writeInt(ACCESS_HIT_OFFSET, hit + 1);
    }

    void setTtlMillis(int ttl) {
        writeInt(TTL_OFFSET, ttl);
    }

    public void setValueAddress(long valueAddress) {
        writeLong(VALUE_OFFSET, valueAddress);
    }

    public CacheRecord reset(long address) {
        setAddress(address);
        setSize(SIZE);
        return this;
    }

    public void clear() {
        writeLong(CREATION_TIME_OFFSET, 0L);
        setAccessTimeDiff(0);
        setTtlMillis(0);
        setValueAddress(CacheRecordStore.NULL_PTR);
    }

    static long getCreationTime(long address) {
        return UnsafeHelper.UNSAFE.getLong(address + CREATION_TIME_OFFSET);
    }

    static int getAccessTimeDiff(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + ACCESS_TIME_OFFSET);
    }

    static int getAccessHit(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + ACCESS_HIT_OFFSET);
    }

    static int getTtlMillis(long address) {
        return UnsafeHelper.UNSAFE.getInt(address + TTL_OFFSET);
    }

    @Override
    public String toString() {
        if (address() > 0L) {
            return "CacheRecord{creationTime: " + getCreationTime()
                    + ", lastAccessTime: " + getAccessTimeDiff()
                    + ", ttl: " + getTtlMillis()
                    + ", valueAddress: " + getValueAddress()
                    + " }";
        } else {
            return "CacheRecord{ NULL }";
        }
    }
}
