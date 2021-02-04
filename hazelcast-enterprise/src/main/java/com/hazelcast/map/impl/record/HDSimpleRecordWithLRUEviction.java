package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_READER_WRITER;

/**
 * This HD record is created when stats are disabled
 * and eviction is configured.
 *
 * Structure:
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Value Address            |   8 bytes (long)   |
 * +--------------------------+--------------------+
 * | VErsion                  |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Used By Eviction         |   4 bytes (int)    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * Total size = 16 bytes (with buddy allocator it is 32 bytes)
 *
 * All fields are aligned.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDSimpleRecordWithLRUEviction extends HDRecord implements Record<Data> {
    /**
     * Gives the size of an {@link HDSimpleRecordWithLRUEviction}.
     */
    public static final int SIZE;

    static final int VALUE_OFFSET = 0;
    static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int LAST_ACCESS_TIME_OFFSET = VERSION_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = LAST_ACCESS_TIME_OFFSET + INT_SIZE_IN_BYTES;
    }

    public HDSimpleRecordWithLRUEviction() {
        super(AMEM);
        setSize(getSize());
    }

    @Override
    public int getVersion() {
        return readInt(VERSION_OFFSET);
    }

    @Override
    public void setVersion(int version) {
        writeInt(VERSION_OFFSET, version);
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_WITH_LRU_EVICTION_READER_WRITER;
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
    public long getValueAddress() {
        return readLong(VALUE_OFFSET);
    }

    @Override
    public void setValueAddress(long valueAddress) {
        writeLong(VALUE_OFFSET, valueAddress);
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
    public int getRawLastAccessTime() {
        return readInt(LAST_ACCESS_TIME_OFFSET);
    }

    @Override
    public void setRawLastAccessTime(int time) {
        writeInt(LAST_ACCESS_TIME_OFFSET, time);
    }

    @Override
    public void onAccess(long now) {
        onAccessSafe(now);
    }

    @Override
    public void onAccessSafe(long now) {
        setLastAccessTime(now);
    }
}
