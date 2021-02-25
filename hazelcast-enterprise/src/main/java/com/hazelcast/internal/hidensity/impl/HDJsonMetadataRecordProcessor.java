package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlockProcessor;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.record.HDJsonMetadataRecord;
import com.hazelcast.map.impl.record.HDJsonMetadataRecordAccessor;

/**
 * The record processor for {@code HDJsonMetadataRecord}.
 */
public class HDJsonMetadataRecordProcessor implements MemoryBlockProcessor<HDJsonMetadataRecord> {

    protected final HazelcastMemoryManager memoryManager;
    protected final HiDensityStorageInfo storageInfo;

    private final EnterpriseSerializationService ess;
    private final HDJsonMetadataRecordAccessor recordAccessor;

    public HDJsonMetadataRecordProcessor(EnterpriseSerializationService ess,
                                         HDJsonMetadataRecordAccessor recordAccessor,
                                         HiDensityStorageInfo storageInfo) {
        this.ess = ess;
        this.recordAccessor = recordAccessor;
        this.memoryManager = ess.getMemoryManager();
        this.storageInfo = storageInfo;

    }

    public HDJsonMetadataRecord newRecord() {
        return recordAccessor.newRecord();
    }

    @Override
    public boolean isEqual(long address, HDJsonMetadataRecord value) {
        return recordAccessor.isEqual(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return recordAccessor.isEqual(address1, address2);
    }

    @Override
    public HDJsonMetadataRecord read(long address) {
        return recordAccessor.read(address);
    }

    @Override
    public long dispose(HDJsonMetadataRecord record) {
        long size = recordAccessor.dispose(record);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long dispose(long address) {
        long size = recordAccessor.dispose(address);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    public long disposeMetadataKeyValue(HDJsonMetadataRecord record) {
        long size = recordAccessor.disposeMetadataKeyValue(record);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    public long disposeMetadataKeyValue(HDJsonMetadataRecord record, boolean isKey) {
        long size = recordAccessor.disposeMetadataKeyValue(record, isKey);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public Data toData(Object obj, DataType dataType) {
        Data data;
        if (dataType == DataType.NATIVE) {
            data = ess.toNativeData(obj, memoryManager);
        } else {
            data = ess.toData(obj, dataType);
        }
        if (data instanceof NativeMemoryData && data != obj) {
            storageInfo.addUsedMemory(recordAccessor.getSize((NativeMemoryData) data));
        }
        return data;
    }

    @Override
    public Object toObject(Object data) {
        return ess.toObject(data, memoryManager);
    }

    @Override
    public Data convertData(Data data, DataType dataType) {
        Data convertedData;
        if (dataType == DataType.NATIVE) {
            convertedData = ess.convertToNativeData(data, memoryManager);
        } else {
            convertedData = ess.convertData(data, dataType);
        }
        if (convertedData instanceof NativeMemoryData && convertedData != data) {
            storageInfo.addUsedMemory(recordAccessor.getSize((NativeMemoryData) convertedData));
        }
        return convertedData;
    }

    @Override
    public void disposeData(Data data) {
        long size = 0L;
        if (data instanceof NativeMemoryData) {
            size = recordAccessor.getSize((NativeMemoryData) data);
        }
        ess.disposeData(data, memoryManager);
        storageInfo.removeUsedMemory(size);
    }

    @Override
    public long allocate(long size) {
        long address = memoryManager.allocate(size);
        storageInfo.addUsedMemory(recordAccessor.getSize(address, size));
        return address;
    }

    @Override
    public void free(long address, long size) {
        long disposedSize = recordAccessor.getSize(address, size);
        memoryManager.free(address, size);
        storageInfo.removeUsedMemory(disposedSize);
    }

    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return memoryManager.getSystemAllocator();
    }
}
