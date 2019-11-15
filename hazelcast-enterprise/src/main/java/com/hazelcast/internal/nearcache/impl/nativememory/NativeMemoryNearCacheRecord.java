package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.nearcache.HiDensityNearCacheRecord;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import java.util.UUID;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * Implementation of {@link HiDensityNearCacheRecord} to store {@link NativeMemoryData} type objects.
 */
@SuppressWarnings("checkstyle:methodcount")
public class NativeMemoryNearCacheRecord extends HiDensityNearCacheRecord {

    /*
     * Structure:
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Value Address            |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Creation Time            |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | UUID                     | 2 x 8 bytes (long) |
     * +--------------------------+--------------------+
     * | Invalidation Sequence    |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Record State             |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Access Time/Hit          |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | TTL                      |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Partition Id             |   4 bytes (int)    |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * Total size = 60 bytes
     *
     * All fields are aligned.
     */

    /**
     * Size of a Hi-Density Near Cache record.
     */
    public static final int SIZE;
    /**
     * Location of the value pointer in a Hi-Density Near Cache record.
     */
    public static final int VALUE_OFFSET = 0;

    static final int CREATION_TIME_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int UUID_MOST_SIG_BITS_OFFSET = CREATION_TIME_OFFSET + LONG_SIZE_IN_BYTES;
    static final int UUID_LEAST_SIG_BITS_OFFSET = UUID_MOST_SIG_BITS_OFFSET + LONG_SIZE_IN_BYTES;
    static final int INVALIDATION_SEQUENCE_OFFSET = UUID_LEAST_SIG_BITS_OFFSET + LONG_SIZE_IN_BYTES;
    static final int RECORD_STATE_OFFSET = INVALIDATION_SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int ACCESS_TIME_OFFSET = RECORD_STATE_OFFSET + LONG_SIZE_IN_BYTES;
    // "ACCESS_HIT_OFFSET" and "ACCESS_TIME_OFFSET` is the same for NearCacheRecord
    // since these fields (access hit count and access time) are not used at same time
    // (their usage scenario is based on eviction type (LRU or LFU))
    static final int ACCESS_HIT_OFFSET = ACCESS_TIME_OFFSET;
    static final int TTL_OFFSET = ACCESS_HIT_OFFSET + INT_SIZE_IN_BYTES;
    static final int PARTITION_ID_OFFSET = TTL_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES;
    }

    private HiDensityRecordAccessor<NativeMemoryNearCacheRecord> nearCacheRecordAccessor;

    public NativeMemoryNearCacheRecord(HiDensityRecordAccessor<NativeMemoryNearCacheRecord> accessor) {
        super(AMEM);
        this.nearCacheRecordAccessor = accessor;
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
    public long getLastAccessTime() {
        int accessTimeDiff = getAccessTimeDiff();
        // not accessed yet
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
    public long getHits() {
        return readInt(ACCESS_HIT_OFFSET);
    }

    @Override
    public void setHits(int hit) {
        writeInt(ACCESS_HIT_OFFSET, hit);
    }

    @Override
    public void incrementHits() {
        long accessHit = getHits() + 1;
        writeInt(ACCESS_HIT_OFFSET, accessHit > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) accessHit);
    }

    @Override
    public void resetHits() {
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
    public NativeMemoryNearCacheRecord reset(long address) {
        setAddress(address);
        setSize(SIZE);
        return this;
    }

    @Override
    public void clear() {
        zero();
    }

    @Override
    public NativeMemoryData getValue() {
        long valueAddress = getValueAddress();
        if (valueAddress == NULL_PTR) {
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
            setValueAddress(NULL_PTR);
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
        int newTtl = expirationTime >= creationTime
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
        long accessTime = getLastAccessTime();
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
        return MEM.getLong(address + CREATION_TIME_OFFSET);
    }

    @Override
    public long getSequence() {
        return 0;
    }

    @Override
    public long getRecordState() {
        return readLong(RECORD_STATE_OFFSET);
    }

    @Override
    public boolean casRecordState(long expected, long update) {
        long existing = getRecordState();
        if (expected == existing) {
            writeLong(RECORD_STATE_OFFSET, update);
            return true;
        }
        return false;
    }

    @Override
    public int getPartitionId() {
        return readInt(PARTITION_ID_OFFSET);
    }

    @Override
    public void setPartitionId(int partitionId) {
        writeInt(PARTITION_ID_OFFSET, partitionId);
    }

    @Override
    public long getInvalidationSequence() {
        return readLong(INVALIDATION_SEQUENCE_OFFSET);
    }

    @Override
    public void setInvalidationSequence(long sequence) {
        writeLong(INVALIDATION_SEQUENCE_OFFSET, sequence);
    }

    @Override
    public void setUuid(UUID uuid) {
        writeLong(UUID_MOST_SIG_BITS_OFFSET, uuid == null ? 0 : uuid.getMostSignificantBits());
        writeLong(UUID_LEAST_SIG_BITS_OFFSET, uuid == null ? 0 : uuid.getLeastSignificantBits());
    }

    @Override
    public boolean hasSameUuid(UUID uuid) {
        return uuid != null
                && readLong(UUID_MOST_SIG_BITS_OFFSET) == uuid.getMostSignificantBits()
                && readLong(UUID_LEAST_SIG_BITS_OFFSET) == uuid.getLeastSignificantBits();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NativeMemoryNearCacheRecord record = (NativeMemoryNearCacheRecord) o;
        return address == record.address && size == record.size;
    }

    @Override
    public int hashCode() {
        int result = (int) (address ^ (address >>> 32));
        result = 31 * result + size;
        return result;
    }

    @Override
    public String toString() {
        if (address() > NULL_PTR) {
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
