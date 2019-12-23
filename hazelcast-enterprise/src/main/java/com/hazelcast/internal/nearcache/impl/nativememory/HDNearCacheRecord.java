package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import java.util.UUID;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * HD backed near cache record implementation.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDNearCacheRecord extends HiDensityRecord
        implements NearCacheRecord<NativeMemoryData> {
    /*
     * Structure:
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Value Address            |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | UUID                     | 2 x 8 bytes (long) |
     * +--------------------------+--------------------+
     * | Invalidation Sequence    |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Reservation Id           |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Creation Time            |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Last Access Time         |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Expiration Time          |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Hits                     |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Partition Id             |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Cached as Null Flag      |   1 byte (boolean) |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * Total size = 61 bytes
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

    static final int UUID_MOST_SIG_BITS_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int UUID_LEAST_SIG_BITS_OFFSET = UUID_MOST_SIG_BITS_OFFSET + LONG_SIZE_IN_BYTES;
    static final int INVALIDATION_SEQUENCE_OFFSET = UUID_LEAST_SIG_BITS_OFFSET + LONG_SIZE_IN_BYTES;
    static final int RESERVATION_ID_OFFSET = INVALIDATION_SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int CREATION_TIME_OFFSET = RESERVATION_ID_OFFSET + LONG_SIZE_IN_BYTES;
    static final int LAST_ACCESS_TIME_OFFSET = CREATION_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int EXPIRATION_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int HITS_OFFSET = EXPIRATION_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int PARTITION_ID_OFFSET = HITS_OFFSET + INT_SIZE_IN_BYTES;
    static final int CACHED_AS_NULL_OFFSET = PARTITION_ID_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = CACHED_AS_NULL_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    }

    private HiDensityRecordAccessor<HDNearCacheRecord> nearCacheRecordAccessor;

    public HDNearCacheRecord(HiDensityRecordAccessor<HDNearCacheRecord> accessor) {
        super(AMEM);
        this.nearCacheRecordAccessor = accessor;
        setSize(SIZE);
    }

    @Override
    public long getCreationTime() {
        return recomputeWithBaseTime(readInt(CREATION_TIME_OFFSET));
    }

    @Override
    public void setCreationTime(long creationTime) {
        writeInt(CREATION_TIME_OFFSET, stripBaseTime(creationTime));
    }

    @Override
    public long getLastAccessTime() {
        return recomputeWithBaseTime(readInt(LAST_ACCESS_TIME_OFFSET));
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        writeInt(LAST_ACCESS_TIME_OFFSET, stripBaseTime(lastAccessTime));
    }

    @Override
    public long getExpirationTime() {
        return recomputeWithBaseTime(readInt(EXPIRATION_TIME_OFFSET));
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        writeInt(EXPIRATION_TIME_OFFSET, stripBaseTime(expirationTime));
    }

    @Override
    public long getHits() {
        return readInt(HITS_OFFSET);
    }

    @Override
    public void setHits(int hit) {
        writeInt(HITS_OFFSET, hit);
    }

    @Override
    public void incrementHits() {
        long hits = getHits() + 1;
        writeInt(HITS_OFFSET, hits > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : (int) hits);
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
    public HDNearCacheRecord reset(long address) {
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
        return valueAddress == NULL_PTR
                ? null : nearCacheRecordAccessor.readData(valueAddress);
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
    public long getSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReservationId() {
        return readLong(RESERVATION_ID_OFFSET);
    }

    @Override
    public void setReservationId(long reservationId) {
        writeLong(RESERVATION_ID_OFFSET, reservationId);
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
    public boolean isCachedAsNull() {
        return readByte(CACHED_AS_NULL_OFFSET) == 1;
    }

    @Override
    public void setCachedAsNull(boolean valueCachedAsNull) {
        writeByte(CACHED_AS_NULL_OFFSET, (byte) (valueCachedAsNull ? 1 : 0));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HDNearCacheRecord record = (HDNearCacheRecord) o;
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
        return address() == NULL_PTR ? "HDNearCacheRecord{NULL}"
                : "HDNearCacheRecord{creationTime=" + getCreationTime()
                + ", sequence=" + getInvalidationSequence()
                + ", expirationTime=" + getExpirationTime()
                + ", lastAccessTime=" + getLastAccessTime()
                + ", hits=" + getHits()
                + ", recordState=" + getPartitionId()
                + ", value=" + getValueAddress();
    }
}
