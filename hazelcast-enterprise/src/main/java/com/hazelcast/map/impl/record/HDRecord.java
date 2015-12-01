package com.hazelcast.map.impl.record;

import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.map.impl.record.RecordStatistics.EMPTY_STATS;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.util.Preconditions.checkInstanceOf;

/**
 * Represents simple HiDensity backed {@link Record} implementation for {@link com.hazelcast.core.IMap IMap}.
 */
public class HDRecord extends HiDensityRecord implements Record<Data> {

    /*
     * Structure:
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Key Address          |   8 bytes (long) |
     * +----------------------+------------------+
     * | Value Address        |   8 bytes (long) |
     * +----------------------+------------------+
     * | Version              |   8 bytes (long) |
     * +----------------------+------------------+
     * | Eviction Criteria    |   8 bytes (long) |
     * +----------------------+------------------+
     * | Time To live         |   8 bytes (long) |
     * +----------------------+------------------+
     * | Last Access Time     |   4 bytes (int)  |
     * +----------------------+------------------+
     * | Last Update Time     |   4 bytes (int)  |
     * +----------------------+------------------+
     * | Tombstone Sequence   |   8 bytes (long) |
     * +----------------------+------------------+
     * | Record Sequence      |   8 bytes (long) |
     * +----------------------+------------------+
     * | Creation Time        |   8 bytes (long) |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * Total size = 72 bytes
     */


    /**
     * Gives the minimum size of an {@link HDRecord}
     */
    public static final int BASE_SIZE;

    protected static final int KEY_OFFSET = 0;
    protected static final int VALUE_OFFSET = KEY_OFFSET + LONG_SIZE_IN_BYTES;
    protected static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    protected static final int EVICTION_CRITERIA_NUMBER_OFFSET = VERSION_OFFSET + LONG_SIZE_IN_BYTES;
    protected static final int TTL_OFFSET = EVICTION_CRITERIA_NUMBER_OFFSET + LONG_SIZE_IN_BYTES;
    protected static final int LAST_ACCESS_TIME_OFFSET = TTL_OFFSET + LONG_SIZE_IN_BYTES;
    protected static final int LAST_UPDATE_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + INT_SIZE_IN_BYTES;
    protected static final int TOMBSTONE_SEQUENCE_OFFSET = LAST_UPDATE_TIME_OFFSET + INT_SIZE_IN_BYTES;
    protected static final int SEQUENCE_OFFSET = TOMBSTONE_SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES;
    protected static final int CREATION_TIME_OFFSET = SEQUENCE_OFFSET + LONG_SIZE_IN_BYTES;

    static {
        BASE_SIZE = CREATION_TIME_OFFSET + LONG_SIZE_IN_BYTES;
    }

    protected HiDensityRecordAccessor<HDRecord> recordAccessor;

    public HDRecord() {
        this(null);
    }

    public HDRecord(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        this.recordAccessor = recordAccessor;
        setSize(getSize());
    }

    protected int getSize() {
        return BASE_SIZE;
    }

    @Override
    public Data getValue() {
        if (address == NULL_PTR) {
            return null;
        }
        long valueAddress = getValueAddress();
        if (valueAddress == NULL_PTR) {
            return null;
        }
        return recordAccessor.readData(valueAddress);
    }

    @Override
    public void setValue(Data value) {
        if (value == null) {
            setValueAddress(NULL_PTR);
            return;
        }
        checkInstanceOf(NativeMemoryData.class, value,
                "Parameter `value` should be a type of [" + NativeMemoryData.class + "], but found [" + value + "]");

        setValueAddress(((NativeMemoryData) value).address());
    }

    public boolean isTombstone() {
        long valueAddress = getValueAddress();
        return valueAddress == NULL_PTR;
    }

    @Override
    public Data getKey() {
        if (address == NULL_PTR) {
            return null;
        } else {
            return recordAccessor.readData(getKeyAddress());
        }
    }

    @Override
    public void invalidate() {
        recordAccessor.dispose(this.address());
    }

    @Override
    public RecordStatistics getStatistics() {
        return EMPTY_STATS;
    }

    @Override
    public void setStatistics(RecordStatistics stats) {

    }

    @Override
    public void onAccess() {

    }

    @Override
    public void onUpdate() {
        setVersion(getVersion() + 1L);
    }

    @Override
    public void onStore() {

    }

    // TODO Add heap cost.
    @Override
    public long getCost() {
        // This is heap cost. For NATIVE we are not calculating this cost now.
        return 0L;
    }

    @Override
    public long getVersion() {
        return readLong(VERSION_OFFSET);
    }

    @Override
    public void setVersion(long version) {
        writeLong(VERSION_OFFSET, version);
    }

    @Override
    public long getSequence() {
        return readLong(SEQUENCE_OFFSET);
    }

    public void setSequence(long sequence) {
        writeLong(SEQUENCE_OFFSET, sequence);
    }

    public long getTombstoneSequence() {
        return readLong(TOMBSTONE_SEQUENCE_OFFSET);
    }

    public void setTombstoneSequence(long tombstoneSequence) {
        writeLong(TOMBSTONE_SEQUENCE_OFFSET, tombstoneSequence);
    }

    @Override
    public long getEvictionCriteriaNumber() {
        return readLong(EVICTION_CRITERIA_NUMBER_OFFSET);
    }

    @Override
    public void setEvictionCriteriaNumber(long evictionCriteriaNumber) {
        writeLong(EVICTION_CRITERIA_NUMBER_OFFSET, evictionCriteriaNumber);
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTtl() {
        return readLong(TTL_OFFSET);
    }

    @Override
    public void setTtl(long ttl) {
        writeLong(TTL_OFFSET, ttl);
    }

    @Override
    public long getLastAccessTime() {
        return readInt(LAST_ACCESS_TIME_OFFSET) + getCreationTime();
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        int diff = (int) (lastAccessTime - getCreationTime());
        // handles overflow
        diff = diff < 0 ? Integer.MAX_VALUE : diff;
        writeInt(LAST_ACCESS_TIME_OFFSET, diff);
    }

    @Override
    public long getLastUpdateTime() {
        return readInt(LAST_UPDATE_TIME_OFFSET) + getCreationTime();
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {
        int diff = (int) (lastUpdatedTime - getCreationTime());
        // handles overflow
        diff = diff < 0 ? Integer.MAX_VALUE : diff;
        writeInt(LAST_UPDATE_TIME_OFFSET, diff);
    }

    @Override
    public long getCreationTime() {
        return readLong(CREATION_TIME_OFFSET);
    }

    @Override
    public void setCreationTime(long creationTime) {
        writeLong(CREATION_TIME_OFFSET, creationTime);
    }

    public long getKeyAddress() {
        return readLong(KEY_OFFSET);
    }

    public void setKeyAddress(long address) {
        writeLong(KEY_OFFSET, address);
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
    public HiDensityRecord reset(long address) {
        setAddress(address);
        setSize(size());
        return this;
    }

    @Override
    public void clear() {
        zero();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}

