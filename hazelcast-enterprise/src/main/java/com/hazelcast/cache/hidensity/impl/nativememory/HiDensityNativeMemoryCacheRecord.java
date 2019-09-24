package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.impl.HiDensityCacheRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * Structure:
 * <pre>
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Creation Time        |   8 bytes (long) |
 * +----------------------+------------------+
 * | Access Time          |   8 bytes (long) |
 * +----------------------+------------------+
 * | Time-to-Live         |   8 bytes (long) |
 * +----------------------+------------------+
 * | Record Sequence      |   8 bytes (long) |
 * +----------------------+------------------+
 * | Value Address        |   8 bytes (long) |
 * +----------------------+------------------+
 * | ExpiryPolicy Address |   8 bytes (long) |
 * +----------------------+------------------+
 * | Hit Count            |   4 bytes (int)  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * </pre>
 * Total size = 52 bytes
 * <p>
 * All fields are aligned.
 * <p>
 * Note: In the current buddy memory allocator design, this is going to use a
 * 64 bytes memory block.
 */
public final class HiDensityNativeMemoryCacheRecord extends HiDensityCacheRecord {

    /**
     * Size of record in bytes.
     */
    public static final int SIZE;


    static final int CREATION_TIME_OFFSET = 0;
    static final int ACCESS_TIME_OFFSET = LONG_SIZE_IN_BYTES;
    static final int TTL_OFFSET = ACCESS_TIME_OFFSET + LONG_SIZE_IN_BYTES;
    static final int SEQUENCE_OFFSET = TTL_OFFSET + LONG_SIZE_IN_BYTES;
    /**
     * Offset of value address in the record.
     */
    static final int VALUE_OFFSET = SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int EXPIRY_POLICY_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int ACCESS_HIT_OFFSET = EXPIRY_POLICY_OFFSET + LONG_SIZE_IN_BYTES;

    static {
        SIZE = ACCESS_HIT_OFFSET + INT_SIZE_IN_BYTES;
    }

    private final HiDensityRecordAccessor<HiDensityNativeMemoryCacheRecord> recordAccessor;

    public HiDensityNativeMemoryCacheRecord(HiDensityRecordAccessor<HiDensityNativeMemoryCacheRecord> recordAccessor,
                                            long address) {
        super(AMEM, address, SIZE);
        this.recordAccessor = recordAccessor;
    }

    public HiDensityNativeMemoryCacheRecord(HiDensityRecordAccessor<HiDensityNativeMemoryCacheRecord> recordAccessor) {
        super(AMEM);
        this.recordAccessor = recordAccessor;
    }

    public HiDensityNativeMemoryCacheRecord(long address) {
        this(null, address);
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
        return readLong(ACCESS_TIME_OFFSET);
    }

    @Override
    public void setAccessTime(long time) {
        writeLong(ACCESS_TIME_OFFSET, time);
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
        setAccessHit(0);
    }

    @Override
    public long getTtlMillis() {
        return readLong(TTL_OFFSET);
    }

    @Override
    public void setTtlMillis(long ttl) {
        writeLong(TTL_OFFSET, ttl);
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
    public void setExpiryPolicyAddress(long policyAddress) {
        writeLong(EXPIRY_POLICY_OFFSET, policyAddress);
    }

    @Override
    public long getExpiryPolicyAddress() {
        return readLong(EXPIRY_POLICY_OFFSET);
    }

    @Override
    public HiDensityNativeMemoryCacheRecord reset(long address) {
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
        if (address == NULL_PTR) {
            return null;
        } else {
            long valueAddress = getValueAddress();
            if (valueAddress == NULL_PTR) {
                return null;
            }
            if (recordAccessor != null) {
                return recordAccessor.readData(valueAddress);
            } else {
                return new NativeMemoryData().reset(valueAddress);
            }
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
    public void setExpiryPolicy(NativeMemoryData expiryPolicy) {
        if (expiryPolicy == null) {
            setExpiryPolicyAddress(NULL_PTR);
        } else {
            setExpiryPolicyAddress(expiryPolicy.address());
        }
    }

    @Override
    public NativeMemoryData getExpiryPolicy() {
        if (address == NULL_PTR) {
            return null;
        } else {
            long policyAddress = getExpiryPolicyAddress();
            if (policyAddress == NULL_PTR) {
                return null;
            }
            if (recordAccessor != null) {
                return recordAccessor.readData(policyAddress);
            } else {
                return new NativeMemoryData().reset(policyAddress);
            }
        }
    }

    @Override
    public long getExpirationTime() {
        long ttlMillis = getTtlMillis();
        if (ttlMillis < 0L) {
            return Long.MAX_VALUE;
        }

        long creationTime = getCreationTime();
        long expirationTime = creationTime + ttlMillis;
        if (expirationTime < creationTime) {
            return Long.MAX_VALUE;
        }
        return expirationTime;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        long creationTime = getCreationTime();
        long timeDiff = expirationTime - creationTime;
        long newTtl = timeDiff >= 0 ? timeDiff : -1L;
        setTtlMillis(newTtl);
    }

    @Override
    public long getSequence() {
        return readLong(SEQUENCE_OFFSET);
    }

    public void setSequence(long seq) {
        writeLong(SEQUENCE_OFFSET, seq);
    }

    @Override
    public boolean isExpiredAt(long now) {
        long expirationTime = getExpirationTime();
        return expirationTime > -1 && expirationTime <= now;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiDensityNativeMemoryCacheRecord record = (HiDensityNativeMemoryCacheRecord) o;
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
        if (address() != NULL_PTR) {
            return "HiDensityNativeMemoryCacheRecord{creationTime: " + getCreationTime()
                    + ", lastAccessTime: " + getLastAccessTime()
                    + ", hits: " + getAccessHit()
                    + ", ttl: " + getTtlMillis()
                    + ", sequence: " + getSequence()
                    + ", valueAddress: " + getValueAddress()
                    + " }";
        } else {
            return "HiDensityNativeMemoryCacheRecord{NULL}";
        }
    }
}
