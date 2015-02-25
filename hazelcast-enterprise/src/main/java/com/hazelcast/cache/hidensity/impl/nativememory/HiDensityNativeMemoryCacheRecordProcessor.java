package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordProcessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;

/**
 * @author sozal 17/11/14
 */
public class HiDensityNativeMemoryCacheRecordProcessor
        implements HiDensityCacheRecordProcessor<HiDensityNativeMemoryCacheRecord> {

    private final EnterpriseSerializationService serializationService;
    private final HiDensityNativeMemoryCacheRecordAccessor memoryBlockAccessor;
    private final MemoryManager memoryManager;
    private final HiDensityCacheInfo cacheInfo;

    public HiDensityNativeMemoryCacheRecordProcessor(EnterpriseSerializationService serializationService,
            HiDensityNativeMemoryCacheRecordAccessor memoryBlockAccessor, MemoryManager memoryManager,
            HiDensityCacheInfo cacheInfo) {
        this.serializationService = serializationService;
        this.memoryBlockAccessor = memoryBlockAccessor;
        this.memoryManager = memoryManager;
        this.cacheInfo = cacheInfo;
    }

    @Override
    public boolean isEqual(long address, HiDensityNativeMemoryCacheRecord value) {
        return memoryBlockAccessor.isEqual(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return memoryBlockAccessor.isEqual(address1, address2);
    }

    @Override
    public HiDensityNativeMemoryCacheRecord read(long address) {
        return memoryBlockAccessor.read(address);
    }

    @Override
    public long dispose(long address) {
        long size = memoryBlockAccessor.dispose(address);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long dispose(HiDensityNativeMemoryCacheRecord block) {
        long size = memoryBlockAccessor.dispose(block);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord newRecord() {
        return memoryBlockAccessor.newRecord();
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        return memoryBlockAccessor.readData(valueAddress);
    }

    @Override
    public Object readValue(HiDensityNativeMemoryCacheRecord record, boolean enqueueDataOnFinish) {
        return memoryBlockAccessor.readValue(record, enqueueDataOnFinish);
    }

    @Override
    public long disposeValue(HiDensityNativeMemoryCacheRecord record) {
        long size = memoryBlockAccessor.disposeValue(record);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long disposeData(NativeMemoryData data) {
        long size = memoryBlockAccessor.disposeData(data);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long disposeData(long address) {
        long size = memoryBlockAccessor.disposeData(address);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public void enqueueRecord(HiDensityNativeMemoryCacheRecord record) {
        memoryBlockAccessor.enqueueRecord(record);
    }

    @Override
    public void enqueueData(NativeMemoryData data) {
        memoryBlockAccessor.enqueueData(data);
    }

    @Override
    public Data toData(Object obj, DataType dataType) {
        Data data = serializationService.toData(obj, dataType);
        if (data instanceof NativeMemoryData && data != obj) {
            cacheInfo.addUsedMemory(memoryBlockAccessor.getSize((NativeMemoryData) data));
        }
        return data;
    }

    @Override
    public Data convertData(Data data, DataType dataType) {
        Data convertedData = serializationService.convertData(data, dataType);
        if (convertedData instanceof NativeMemoryData && convertedData != data) {
            cacheInfo.addUsedMemory(memoryBlockAccessor.getSize((NativeMemoryData) convertedData));
        }
        return convertedData;
    }

    @Override
    public void disposeData(Data data) {
        long size = 0L;
        if (data instanceof NativeMemoryData) {
            size = memoryBlockAccessor.getSize((NativeMemoryData) data);
        }
        serializationService.disposeData(data);
        cacheInfo.removeUsedMemory(size);
    }

    @Override
    public long allocate(long size) {
        long address = memoryManager.allocate(size);
        cacheInfo.addUsedMemory(getSize(address, size));
        return address;
    }

    @Override
    public void free(long address, long size) {
        long disposedSize = getSize(address, size);
        memoryManager.free(address, size);
        cacheInfo.removeUsedMemory(disposedSize);
    }

    @Override
    public long getUsedMemory() {
        return cacheInfo.getUsedMemory();
    }

    private long getSize(long address, long expectedSize) {
        long size = memoryManager.getSize(address);
        if (size == MemoryManager.SIZE_INVALID) {
            size = expectedSize;
        }
        return size;
    }

}
