package com.hazelcast.map.impl.record;

import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.memory.MemoryBlockAccessor;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.map.impl.record.HDJsonMetadataRecord.KEY_OFFSET;
import static com.hazelcast.map.impl.record.HDJsonMetadataRecord.VALUE_OFFSET;

/**
 * HD Json metadata record accessor.
 */
public class HDJsonMetadataRecordAccessor implements MemoryBlockAccessor<HDJsonMetadataRecord> {

    private final EnterpriseSerializationService ess;

    public HDJsonMetadataRecordAccessor(EnterpriseSerializationService ess) {
        this.ess = ess;
    }

    public HDJsonMetadataRecord newRecord() {
        return new HDJsonMetadataRecord(ess);
    }

    @Override
    public boolean isEqual(long address, HDJsonMetadataRecord value) {
        return isEqual(address, value.address());
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }

        long keyAddress1 = AMEM.getLong(address1 + KEY_OFFSET);
        long keyAddress2 = AMEM.getLong(address2 + KEY_OFFSET);


        long valueAddress1 = AMEM.getLong(address1 + VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(keyAddress1, keyAddress2)
            && NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

    @Override
    public HDJsonMetadataRecord read(long address) {
        if (address == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        HDJsonMetadataRecord record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public long dispose(HDJsonMetadataRecord record) {
        if (record.address() == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        long size = 0L;
        size += disposeMetadataKeyValue(record);
        size += getSize(record);
        ess.getMemoryManager().free(record.address(), record.size());
        record.reset(HazelcastMemoryManager.NULL_ADDRESS);
        return size;
    }

    public long disposeMetadataKeyValue(HDJsonMetadataRecord record) {
        long size = 0L;

        NativeMemoryData keyData = record.getKey();
        if (keyData != null) {
            size += disposeData(keyData);
            record.setKeyAddress(HazelcastMemoryManager.NULL_ADDRESS);
        }

        NativeMemoryData valueData = record.getValue();
        if (valueData != null) {
            size += disposeData(valueData);
            record.setValueAddress(HazelcastMemoryManager.NULL_ADDRESS);
        }

        return size;
    }

    public long disposeMetadataKeyValue(HDJsonMetadataRecord record, boolean isKey) {
        long size = 0L;

        if (isKey) {
            NativeMemoryData keyData = record.getKey();
            if (keyData != null) {
                size += disposeData(keyData);
                record.setKeyAddress(HazelcastMemoryManager.NULL_ADDRESS);
            }
        } else {
            NativeMemoryData valueData = record.getValue();
            if (valueData != null) {
                size += disposeData(valueData);
                record.setValueAddress(HazelcastMemoryManager.NULL_ADDRESS);
            }
        }

        return size;
    }


    public long disposeData(NativeMemoryData value) {
        if (value.address() == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + value.address());
        }
        long size = getSize(value);

        ess.disposeData(value, ess.getMemoryManager());
        return size;
    }

    @Override
    public long dispose(long address) {
        return dispose(read(address));
    }

    public long getSize(MemoryBlock memoryBlock) {
        if (memoryBlock == null || memoryBlock.address() == HazelcastMemoryManager.NULL_ADDRESS) {
            return 0;
        }
        long size = ess.getMemoryManager().getAllocatedSize(memoryBlock.address());
        if (size == HazelcastMemoryManager.SIZE_INVALID) {
            size = memoryBlock.size();
        }

        return size;
    }

    public long getSize(long address, long expectedSize) {
        if (address == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        long size = ess.getMemoryManager().getAllocatedSize(address);
        if (size == HazelcastMemoryManager.SIZE_INVALID) {
            size = expectedSize;
        }
        return size;
    }
}
