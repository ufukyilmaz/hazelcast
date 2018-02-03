package com.hazelcast.map.impl.record;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents simple Hi-Density backed {@link Record} implementation for {@link com.hazelcast.core.IMap IMap}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDRecord extends HiDensityRecord implements Record<Data> {

    /*
     * Structure:
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Key Address              |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Value Address            |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Version                  |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Creation Time            |   8 bytes (long)   |
     * +--------------------------+--------------------+
     * | Time To live             |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Last Access Time         |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Last Update Time         |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Hits                     |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Last Stored Time         |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Expiration Time          |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Sequence                 |   4 bytes (int)    |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     * Total size = 60 bytes
     */


    /**
     * Gives the size of an {@link HDRecord}.
     */
    public static final int SIZE;

    static final int KEY_OFFSET = 0;
    static final int VALUE_OFFSET = KEY_OFFSET + LONG_SIZE_IN_BYTES;
    static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int CREATION_TIME_OFFSET = VERSION_OFFSET + LONG_SIZE_IN_BYTES;
    static final int TTL_OFFSET = CREATION_TIME_OFFSET + LONG_SIZE_IN_BYTES;
    static final int LAST_ACCESS_TIME_OFFSET = TTL_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_UPDATE_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int HITS = LAST_UPDATE_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_STORED_TIME_OFFSET = HITS + INT_SIZE_IN_BYTES;
    static final int EXPIRATION_TIME_OFFSET = LAST_STORED_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int SEQUENCE_OFFSET = EXPIRATION_TIME_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = SEQUENCE_OFFSET + INT_SIZE_IN_BYTES;
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
        return SIZE;
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

        assert value instanceof NativeMemoryData
                : "Parameter `value` should be a type of ["
                + NativeMemoryData.class + "], but found [" + value + ']';

        setValueAddress(((NativeMemoryData) value).address());
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
    public void onAccess(long now) {
        setHits(getHits() + 1);

        setLastAccessTime(now);
    }

    @Override
    public void onUpdate(long now) {
        setVersion(getVersion() + 1L);
        setLastUpdateTime(now);
    }

    @Override
    public void onStore() {
        setLastStoredTime(Clock.currentTimeMillis());
    }

    @Override
    public long getCost() {
        // this is heap cost. For NATIVE we are not calculating this cost
        return 0L;
    }

    @Override
    public long getSequence() {
        return readInt(SEQUENCE_OFFSET);
    }

    /**
     * Since the address can be re-used, sequence provides a unique number () for the pointer.
     * Sequence is used for Hot Restart.
     *
     * @param sequence
     */
    @Override
    public void setSequence(long sequence) {
        writeInt(SEQUENCE_OFFSET, (int) sequence);
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
    public HiDensityRecord reset(long address) {
        setAddress(address);
        setSize(size());
        return this;
    }

    @Override
    public void clear() {
        zero();
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

    /**
     * Version field is used as tombstone-sequence, after removing the record.
     *
     * @return
     */
    @Override
    public long getVersion() {
        return readLong(VERSION_OFFSET);
    }

    /**
     * Version field is used as tombstone-sequence, after removing the record.
     *
     * @param version
     */
    @Override
    public void setVersion(long version) {
        writeLong(VERSION_OFFSET, version);
    }

    @Override
    public long getCreationTime() {
        return readLong(CREATION_TIME_OFFSET);
    }

    @Override
    public void setCreationTime(long creationTime) {
        writeLong(CREATION_TIME_OFFSET, creationTime);
    }

    @Override
    public long getTtl() {
        return SECONDS.toMillis(readInt(TTL_OFFSET));
    }

    @Override
    public void setTtl(long ttl) {
        long ttlSeconds = MILLISECONDS.toSeconds(ttl);
        if (ttlSeconds == 0 && ttl != 0) {
            ttlSeconds = 1;
        }
        int value = ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds;
        writeInt(TTL_OFFSET, value);
    }

    @Override
    public long getLastAccessTime() {
        return getWithCreationTime(LAST_ACCESS_TIME_OFFSET);
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        setWithCreationTime(LAST_ACCESS_TIME_OFFSET, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return getWithCreationTime(LAST_UPDATE_TIME_OFFSET);
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {
        setWithCreationTime(LAST_UPDATE_TIME_OFFSET, lastUpdatedTime);
    }

    @Override
    public long getHits() {
        return readInt(HITS);
    }

    @Override
    public void setHits(long hits) {
        writeInt(HITS, (int) hits);
    }

    @Override
    public long getLastStoredTime() {
        return getWithCreationTime(LAST_STORED_TIME_OFFSET);
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        setWithCreationTime(LAST_STORED_TIME_OFFSET, lastStoredTime);
    }

    @Override
    public long getExpirationTime() {
        return getWithCreationTime(EXPIRATION_TIME_OFFSET);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        setWithCreationTime(EXPIRATION_TIME_OFFSET, expirationTime);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    private long getWithCreationTime(int offset) {
        int value = readInt(offset);
        if (value == NOT_AVAILABLE) {
            return 0L;
        }
        return value + getCreationTime();
    }

    private void setWithCreationTime(int offset, long value) {
        int diff = NOT_AVAILABLE;
        if (value > 0) {
            diff = (int) (value - getCreationTime());
            // handles overflow
            diff = diff < 0 ? Integer.MAX_VALUE : diff;
        }
        writeInt(offset, diff);
    }
}

