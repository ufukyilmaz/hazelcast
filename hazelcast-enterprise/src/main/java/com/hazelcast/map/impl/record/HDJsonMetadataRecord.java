package com.hazelcast.map.impl.record;

import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.query.impl.JsonMetadata;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * The HD Json metadata record that keeps references to the metadata's key and value.
 * It always takes 16 bytes.
 */
public class HDJsonMetadataRecord extends MemoryBlock implements JsonMetadata {
    /**
     * Gives the size of an {@link HDJsonMetadataRecord}.
     */
    public static final int SIZE = 16;

    public static final int KEY_OFFSET = 0;
    public static final int VALUE_OFFSET = KEY_OFFSET + LONG_SIZE_IN_BYTES;

    private final EnterpriseSerializationService ess;

    public HDJsonMetadataRecord(EnterpriseSerializationService ess) {
        super(AMEM);
        this.ess = ess;
    }

    public void setValue(Data value) {
        if (value == null) {
            setValueAddress(NULL_PTR);
            return;
        }

        assert value instanceof NativeMemoryData;

        setValueAddress(((NativeMemoryData) value).address());
    }

    public void setValueAddress(long valueAddress) {
        writeLong(VALUE_OFFSET, valueAddress);
    }

    public NativeMemoryData getValue() {
        if (address == NULL_PTR) {
            return null;
        }
        long valueAddress = getValueAddress();
        if (valueAddress == NULL_PTR) {
            return null;
        }
        return new NativeMemoryData().reset(valueAddress);
    }

    public long getValueAddress() {
        return readLong(VALUE_OFFSET);
    }

    public void setKey(Data value) {
        if (value == null) {
            setKeyAddress(NULL_PTR);
            return;
        }

        assert value instanceof NativeMemoryData;

        setKeyAddress(((NativeMemoryData) value).address());
    }

    public void setKeyAddress(long valueAddress) {
        writeLong(KEY_OFFSET, valueAddress);
    }

    public NativeMemoryData getKey() {
        if (address == NULL_PTR) {
            return null;
        }
        long keyAddress = getKeyAddress();
        if (keyAddress == NULL_PTR) {
            return null;
        }
        return new NativeMemoryData().reset(keyAddress);
    }

    public long getKeyAddress() {
        return readLong(KEY_OFFSET);
    }

    public HDJsonMetadataRecord reset(long address) {
        setAddress(address);
        setSize(getSize());
        return this;
    }

    public int getSize() {
        return SIZE;
    }

    @Override
    public Object getKeyMetadata() {
        return ess.toObject(getKey());
    }

    @Override
    public Object getValueMetadata() {
        return ess.toObject(getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HDJsonMetadataRecord record = (HDJsonMetadataRecord) o;
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
        return address() == NULL_PTR ? "HDJsonMetadataRecord{NULL}"
            : "HDJsonMetadataRecord{"
            + "keyAddress=" + getKeyAddress()
            + "valueAddress=" + getValueAddress()
            + "}";
    }
}
