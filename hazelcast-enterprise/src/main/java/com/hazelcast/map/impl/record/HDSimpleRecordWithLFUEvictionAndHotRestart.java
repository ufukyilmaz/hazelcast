package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_READER_WRITER;

/**
 * This HD record is created when stats are disabled
 * and hot restart is configured.
 *
 * Structure:
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * | Value Address            |   8 bytes (long)   |
 * +--------------------------+--------------------+
 * | Version                  |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Used By Eviction         |   4 bytes (int)    |
 * +--------------------------+--------------------+
 * | Sequence                 |   4 bytes (int)    |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 * Total size = 20 bytes (with buddy allocator it is still 32 bytes)
 *
 * All fields are aligned.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDSimpleRecordWithLFUEvictionAndHotRestart extends HDRecord implements Record<Data> {
    /**
     * Gives the size of an {@link HDSimpleRecordWithLFUEvictionAndHotRestart}.
     */
    public static final int SIZE;

    static final int VALUE_OFFSET = 0;
    static final int VERSION_OFFSET = VALUE_OFFSET + LONG_SIZE_IN_BYTES;
    static final int HITS_OFFSET = VERSION_OFFSET + INT_SIZE_IN_BYTES;
    static final int SEQUENCE_OFFSET = HITS_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = SEQUENCE_OFFSET + INT_SIZE_IN_BYTES;
    }

    public HDSimpleRecordWithLFUEvictionAndHotRestart() {
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

    @Override
    public int getHits() {
        return readInt(HITS_OFFSET);
    }

    @Override
    public void setHits(int hits) {
        writeInt(HITS_OFFSET, hits);
    }

    @Override
    public void onAccess(long now) {
        int hits = getHits();
        if (hits < Integer.MAX_VALUE) {
            // protect against potential overflow
            setHits(hits + 1);
        }
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_WITH_LFU_EVICTION_READER_WRITER;
    }
}
