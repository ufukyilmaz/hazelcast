package com.hazelcast.map.impl.record;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;
import com.hazelcast.internal.util.Clock;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.map.impl.record.AbstractRecord.EPOCH_TIME;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents simple Hi-Density backed {@link
 * Record} implementation for {@link IMap IMap}.
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
     * | Creation Time            |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Time To live             |   4 bytes (int)    |
     * +--------------------------+--------------------+
     * | Max Idle                 |   4 bytes (int)    |
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
     *
     * All fields are aligned.
     */

    /**
     * Gives the size of an {@link HDRecord}.
     */
    public static final int SIZE;

    static final int KEY_OFFSET = 0;
    static final int VALUE_OFFSET = KEY_OFFSET + LONG_SIZE_IN_BYTES;
    static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int CREATION_TIME_OFFSET = VERSION_OFFSET + LONG_SIZE_IN_BYTES;
    static final int TTL_OFFSET = CREATION_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int MAX_IDLE_OFFSET = TTL_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_ACCESS_TIME_OFFSET = MAX_IDLE_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_UPDATE_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int HITS = LAST_UPDATE_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_STORED_TIME_OFFSET = HITS + INT_SIZE_IN_BYTES;
    static final int EXPIRATION_TIME_OFFSET = LAST_STORED_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int SEQUENCE_OFFSET = EXPIRATION_TIME_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = SEQUENCE_OFFSET + INT_SIZE_IN_BYTES;
    }

    protected final HiDensityRecordAccessor<HDRecord> recordAccessor;

    public HDRecord(HiDensityRecordAccessor<HDRecord> recordAccessor) {
        super(AMEM);
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
    public void setKey(Data key) {
        throw new UnsupportedOperationException();
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
    public void onAccessSafe(long now) {
        onAccess(now);
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
        return getWithBaseTime(CREATION_TIME_OFFSET);
    }

    @Override
    public void setCreationTime(long creationTime) {
        setWithBaseTime(CREATION_TIME_OFFSET, creationTime);
    }

    @Override
    public long getTtl() {
        int value = readInt(TTL_OFFSET);
        return value == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(value);
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
    public void setMaxIdle(long maxIdle) {
        long maxIdleSeconds = MILLISECONDS.toSeconds(maxIdle);
        if (maxIdleSeconds == 0 && maxIdle != 0) {
            maxIdleSeconds = 1;
        }
        int value = maxIdleSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxIdleSeconds;
        writeInt(MAX_IDLE_OFFSET, value);
    }

    @Override
    public long getMaxIdle() {
        int value = readInt(MAX_IDLE_OFFSET);
        return value == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(value);
    }

    @Override
    public long getLastAccessTime() {
        return getWithBaseTime(LAST_ACCESS_TIME_OFFSET);
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        setWithBaseTime(LAST_ACCESS_TIME_OFFSET, lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return getWithBaseTime(LAST_UPDATE_TIME_OFFSET);
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {
        setWithBaseTime(LAST_UPDATE_TIME_OFFSET, lastUpdatedTime);
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
        return getWithBaseTime(LAST_STORED_TIME_OFFSET);
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        setWithBaseTime(LAST_STORED_TIME_OFFSET, lastStoredTime);
    }

    @Override
    public long getExpirationTime() {
        int value = readInt(EXPIRATION_TIME_OFFSET);
        if (value == Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return recomputeWithBaseTime(value);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        if (expirationTime == Long.MAX_VALUE) {
            writeInt(EXPIRATION_TIME_OFFSET, Integer.MAX_VALUE);
            return;
        }

        setWithBaseTime(EXPIRATION_TIME_OFFSET, expirationTime);
    }

    /**
     * {@inheritDoc}
     *
     * Note: Metadata is stored on-heap
     *
     * @param metadata
     */
    @Override
    public void setMetadata(Metadata metadata) {
        // no-op
    }

    /**
     * {@inheritDoc}
     *
     * Note: Metadata is stored on-heap
     *
     * @return
     */
    @Override
    public Metadata getMetadata() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    private long getWithBaseTime(int offset) {
        int value = readInt(offset);
        return recomputeWithBaseTime(value);
    }

    private long recomputeWithBaseTime(int value) {
        if (value == NOT_AVAILABLE) {
            return 0L;
        }
        return SECONDS.toMillis(value) + EPOCH_TIME;
    }

    private void setWithBaseTime(int offset, long value) {
        int diff = NOT_AVAILABLE;
        if (value > 0) {
            diff = (int) MILLISECONDS.toSeconds(value - EPOCH_TIME);
        }
        writeInt(offset, diff);
    }

}

