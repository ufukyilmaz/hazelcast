package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.HiDensityCacheInfo;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordProcessor;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
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
    private final HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor;
    private final MemoryManager memoryManager;
    private final HiDensityCacheInfo cacheInfo;

    public HiDensityNativeMemoryCacheRecordProcessor(EnterpriseSerializationService serializationService,
            HiDensityNativeMemoryCacheRecordAccessor cacheRecordAccessor, MemoryManager memoryManager,
            HiDensityCacheInfo cacheInfo) {
        this.serializationService = serializationService;
        this.cacheRecordAccessor = cacheRecordAccessor;
        this.memoryManager = memoryManager;
        this.cacheInfo = cacheInfo;
    }

    @Override
    public boolean isEqual(long address, HiDensityNativeMemoryCacheRecord value) {
        return cacheRecordAccessor.isEqual(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return cacheRecordAccessor.isEqual(address1, address2);
    }

    @Override
    public HiDensityNativeMemoryCacheRecord read(long address) {
        return cacheRecordAccessor.read(address);
    }

    @Override
    public long dispose(long address) {
        long size = cacheRecordAccessor.dispose(address);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long dispose(HiDensityNativeMemoryCacheRecord block) {
        long size = cacheRecordAccessor.dispose(block);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord newRecord() {
        return cacheRecordAccessor.newRecord();
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        return cacheRecordAccessor.readData(valueAddress);
    }

    @Override
    public Object readValue(HiDensityNativeMemoryCacheRecord record, boolean enqueueDataOnFinish) {
        return cacheRecordAccessor.readValue(record, enqueueDataOnFinish);
    }

    @Override
    public long disposeValue(HiDensityNativeMemoryCacheRecord record) {
        long size = cacheRecordAccessor.disposeValue(record);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long disposeData(NativeMemoryData data) {
        long size = cacheRecordAccessor.disposeData(data);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long disposeData(long address) {
        long size = cacheRecordAccessor.disposeData(address);
        cacheInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public void enqueueRecord(HiDensityNativeMemoryCacheRecord record) {
        cacheRecordAccessor.enqueueRecord(record);
    }

    @Override
    public void enqueueData(NativeMemoryData data) {
        cacheRecordAccessor.enqueueData(data);
    }

    @Override
    public Data toData(Object obj, DataType dataType) {
        Data data = serializationService.toData(obj, dataType);
        if (data instanceof NativeMemoryData && data != obj) {
            cacheInfo.addUsedMemory(cacheRecordAccessor.getSize((NativeMemoryData) data));
        }
        return data;
    }

    @Override
    public Data convertData(Data data, DataType dataType) {
        Data convertedData = serializationService.convertData(data, dataType);
        if (convertedData instanceof NativeMemoryData && convertedData != data) {
            cacheInfo.addUsedMemory(cacheRecordAccessor.getSize((NativeMemoryData) convertedData));
        }
        return convertedData;
    }

    @Override
    public void disposeData(Data data) {
        long size = 0L;
        if (data instanceof NativeMemoryData) {
            size = cacheRecordAccessor.getSize((NativeMemoryData) data);
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

    @Override
    public long getSize(long address, long expectedSize) {
        return cacheRecordAccessor.getSize(address, expectedSize);
    }

    @Override
    public long getSize(MemoryBlock memoryBlock) {
        return cacheRecordAccessor.getSize(memoryBlock);
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return memoryManager.unwrapMemoryAllocator();
    }
}
