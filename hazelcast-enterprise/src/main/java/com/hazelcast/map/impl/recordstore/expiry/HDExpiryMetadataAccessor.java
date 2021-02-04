package com.hazelcast.map.impl.recordstore.expiry;

import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

public class HDExpiryMetadataAccessor
        implements HiDensityRecordAccessor<HDExpiryMetadata> {

    protected final EnterpriseSerializationService ess;
    protected final HazelcastMemoryManager memoryManager;

    public HDExpiryMetadataAccessor(EnterpriseSerializationService ess) {
        this.ess = ess;
        this.memoryManager = ess.getMemoryManager();
    }

    @Override
    public boolean isEqual(long address, HDExpiryMetadata hdExpiryMetaData) {
        return isEqual(address, hdExpiryMetaData.address());
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        return NativeMemoryDataUtil.equals(address1, address2);
    }

    @Override
    public HDExpiryMetadata read(long address) {
        return new HDExpiryMetadata(address);
    }

    @Override
    public long dispose(HDExpiryMetadata hdExpiryMetaData) {
        return dispose(hdExpiryMetaData.address());
    }

    @Override
    public long dispose(long address) {
        if (address == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        long size = HDExpiryMetadata.SIZE;
        memoryManager.free(address, size);
        return size;
    }

    @Override
    public HDExpiryMetadata newRecord() {
        return new HDExpiryMetadata();
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object readValue(HDExpiryMetadata record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long disposeData(NativeMemoryData data) {
        return dispose(data.address());
    }

    @Override
    public long disposeData(long address) {
        return dispose(address);
    }

    @Override
    public long disposeValue(HDExpiryMetadata record) {
        return dispose(record);
    }

    @Override
    public long getSize(MemoryBlock memoryBlock) {
        if (memoryBlock == null || memoryBlock.address() == HazelcastMemoryManager.NULL_ADDRESS) {
            return 0;
        }
        long size = memoryManager.getAllocatedSize(memoryBlock.address());
        if (size == HazelcastMemoryManager.SIZE_INVALID) {
            size = memoryBlock.size();
        }

        return size;
    }

    @Override
    public long getSize(long address, long expectedSize) {
        if (address == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        long size = memoryManager.getAllocatedSize(address);
        if (size == HazelcastMemoryManager.SIZE_INVALID) {
            size = expectedSize;
        }
        return size;
    }
}
