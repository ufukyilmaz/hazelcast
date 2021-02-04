package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.DATA_RECORD_WITH_STATS_READER_WRITER;

/**
 * Represents simple HD memory backed {@link Record}
 * implementation for {@link com.hazelcast.map.IMap IMap}.
 * Structure:
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Value Address            |   8 bytes (long)   |
 * +--------------------------+--------------------+
 * | Version           (E)    |   4 bytes (int)   |
 * +--------------------------+--------------------+
 * | Creation Time     (E)    |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Last Access Time  (E)    |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Last Update Time  (E)    |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Hits              (E)    |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Last Stored Time  (E)    |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Sequence          (E)    |   4 bytes (int)    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * Total size = 36 bytes (with buddy allocator it is 64 bytes)
 *
 * All fields are aligned.
 *
 * Note: In current buddy memory allocator design,
 * this record is going to use a 64 bytes memory block.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDRecordWithStats extends HDRecord implements Record<Data> {
    /**
     * Gives the size of an {@link HDRecordWithStats}.
     */
    public static final int SIZE;

    static final int VALUE_OFFSET = 0;
    static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int CREATION_TIME_OFFSET = VERSION_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_ACCESS_TIME_OFFSET = CREATION_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_UPDATE_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int HITS_OFFSET = LAST_UPDATE_TIME_OFFSET + INT_SIZE_IN_BYTES;
    static final int LAST_STORED_TIME_OFFSET = HITS_OFFSET + INT_SIZE_IN_BYTES;
    static final int SEQUENCE_OFFSET = LAST_STORED_TIME_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = SEQUENCE_OFFSET + INT_SIZE_IN_BYTES;
    }

    public HDRecordWithStats() {
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
    public int getVersion() {
        return readInt(VERSION_OFFSET);
    }

    /**
     * Version field is used as tombstone-sequence, after removing the record.
     *
     * @param version
     */
    @Override
    public void setVersion(int version) {
        writeInt(VERSION_OFFSET, version);
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
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return DATA_RECORD_WITH_STATS_READER_WRITER;
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
}
