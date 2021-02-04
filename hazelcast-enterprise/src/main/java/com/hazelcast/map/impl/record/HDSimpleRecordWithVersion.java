package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_READER_WRITER;

/**
 * This HD record is created when stats are disabled
 * and eviction is configured.
 *
 * Structure:
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Value Address            |   8 bytes (long)   |
 * +--------------------------+--------------------+
 * | Version                  |   4 bytes (int)    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * Total size = 12 bytes (with buddy allocator it is 16 bytes)
 *
 * All fields are aligned.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDSimpleRecordWithVersion extends HDRecord implements Record<Data> {
    /**
     * Gives the size of an {@link HDSimpleRecordWithVersion}.
     */
    public static final int SIZE;

    static final int VALUE_OFFSET = 0;
    static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;

    static {
        SIZE = VERSION_OFFSET + INT_SIZE_IN_BYTES;
    }

    public HDSimpleRecordWithVersion() {
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
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_READER_WRITER;
    }

    @Override
    public long getValueAddress() {
        return readLong(VALUE_OFFSET);
    }

    @Override
    public void setValueAddress(long valueAddress) {
        writeLong(VALUE_OFFSET, valueAddress);
    }
}
