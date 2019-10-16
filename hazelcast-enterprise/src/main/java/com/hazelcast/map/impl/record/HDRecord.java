package com.hazelcast.map.impl.record;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Metadata;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.DATA_RECORD_WITH_STATS_READER_WRITER;

/**
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
 *
 * Represents simple Hi-Density backed {@link
 * Record} implementation for {@link com.hazelcast.map.IMap IMap}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDRecord extends HiDensityRecord implements Record<Data> {
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
    static final int HITS_OFFSET = LAST_UPDATE_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_STORED_TIME_OFFSET = HITS_OFFSET + INT_SIZE_IN_BYTES;
    static final int EXPIRATION_TIME_OFFSET = LAST_STORED_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int SEQUENCE_OFFSET = EXPIRATION_TIME_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = SEQUENCE_OFFSET + INT_SIZE_IN_BYTES;
    }

    public HDRecord() {
        super(AMEM);
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
        return new NativeMemoryData().reset(valueAddress);
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
            long keyAddress = getKeyAddress();
            if (keyAddress == NULL_PTR) {
                return null;
            }
            return new NativeMemoryData().reset(keyAddress);
        }
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
     * Since the address can be re-used, sequence provides a unique
     * number () for the pointer. Sequence is used for Hot Restart.
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
    public long getLastUpdateTime() {
        return recomputeWithBaseTime(readInt(LAST_UPDATE_TIME_OFFSET));
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {
        writeInt(LAST_UPDATE_TIME_OFFSET, stripBaseTime(lastUpdatedTime));
    }

    @Override
    public int getHits() {
        return readInt(HITS_OFFSET);
    }

    @Override
    public void setHits(int hits) {
        writeInt(HITS_OFFSET, hits);
    }

    @Override
    public long getLastStoredTime() {
        return recomputeWithBaseTime(readInt(LAST_STORED_TIME_OFFSET));
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        writeInt(LAST_STORED_TIME_OFFSET, stripBaseTime(lastStoredTime));
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

        writeInt(EXPIRATION_TIME_OFFSET, stripBaseTime(expirationTime));
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
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return DATA_RECORD_WITH_STATS_READER_WRITER;
    }

    @Override
    public int getRawTtl() {
        return readInt(TTL_OFFSET);
    }

    @Override
    public int getRawMaxIdle() {
        return readInt(MAX_IDLE_OFFSET);
    }

    @Override
    public void setRawTtl(int ttl) {
        writeInt(TTL_OFFSET, ttl);
    }

    @Override
    public void setRawMaxIdle(int maxIdle) {
        writeInt(MAX_IDLE_OFFSET, maxIdle);
    }

    @Override
    public int getRawCreationTime() {
        return readInt(CREATION_TIME_OFFSET);
    }

    @Override
    public void setRawCreationTime(int time) {
        writeInt(CREATION_TIME_OFFSET, time);
    }

    @Override
    public int getRawLastAccessTime() {
        return readInt(LAST_ACCESS_TIME_OFFSET);
    }

    @Override
    public void setRawLastAccessTime(int time) {
        writeInt(LAST_ACCESS_TIME_OFFSET, time);
    }

    @Override
    public int getRawLastUpdateTime() {
        return readInt(LAST_UPDATE_TIME_OFFSET);
    }

    @Override
    public void setRawLastUpdateTime(int time) {
        writeInt(LAST_UPDATE_TIME_OFFSET, time);
    }

    @Override
    public int getRawLastStoredTime() {
        return readInt(LAST_STORED_TIME_OFFSET);
    }

    @Override
    public void setRawLastStoredTime(int time) {
        writeInt(LAST_STORED_TIME_OFFSET, time);
    }

    @Override
    public int getRawExpirationTime() {
        return readInt(EXPIRATION_TIME_OFFSET);
    }

    @Override
    public void setRawExpirationTime(int time) {
        writeInt(EXPIRATION_TIME_OFFSET, time);
    }

    @Override
    public String toString() {
        return address() == NULL_PTR
                ? "HDRecord{NULL}"
                : "HDRecord{version: " + getVersion()
                + ", creationTime: " + getCreationTime()
                + ", lastAccessTime: " + getLastAccessTime()
                + ", lastUpdateTime: " + getLastUpdateTime()
                + ", lastStoredTime: " + getLastStoredTime()
                + ", expirationTime: " + getExpirationTime()
                + ", hits: " + getHits()
                + ", ttl: " + getTtl()
                + ", maxIdle: " + getMaxIdle()
                + ", sequence: " + getSequence()
                + ", valueAddress: " + getValueAddress()
                + " }";
    }
}
