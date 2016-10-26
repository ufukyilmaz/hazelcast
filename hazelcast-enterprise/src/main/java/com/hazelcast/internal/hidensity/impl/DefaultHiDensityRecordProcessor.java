package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @param <R> type of the {@link HiDensityRecord} to be processed
 * @author sozal 18/02/15
 */
public class DefaultHiDensityRecordProcessor<R extends HiDensityRecord>
        implements HiDensityRecordProcessor<R> {

    protected final EnterpriseSerializationService serializationService;
    protected final HiDensityRecordAccessor<R> recordAccessor;
    protected final HazelcastMemoryManager memoryManager;
    protected final HiDensityStorageInfo storageInfo;

    protected final Queue<MemoryBlock> deferredBlocksQueue = new ArrayDeque<MemoryBlock>(8);

    public DefaultHiDensityRecordProcessor(EnterpriseSerializationService serializationService,
                                           HiDensityRecordAccessor<R> recordAccessor,
                                           HazelcastMemoryManager memoryManager,
                                           HiDensityStorageInfo storageInfo) {
        this.serializationService = serializationService;
        this.recordAccessor = recordAccessor;
        this.memoryManager = memoryManager;
        this.storageInfo = storageInfo;
    }

    @Override
    public boolean isEqual(long address, R value) {
        return recordAccessor.isEqual(address, value);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        return recordAccessor.isEqual(address1, address2);
    }

    @Override
    public R read(long address) {
        return recordAccessor.read(address);
    }

    @Override
    public long dispose(long address) {
        long size = recordAccessor.dispose(address);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long dispose(R block) {
        long size = recordAccessor.dispose(block);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public R newRecord() {
        return recordAccessor.newRecord();
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        return recordAccessor.readData(valueAddress);
    }

    @Override
    public Object readValue(R record) {
        return recordAccessor.readValue(record);
    }

    @Override
    public long disposeValue(R record) {
        long size = recordAccessor.disposeValue(record);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long disposeData(NativeMemoryData data) {
        long size = recordAccessor.disposeData(data);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public long disposeData(long address) {
        long size = recordAccessor.disposeData(address);
        storageInfo.removeUsedMemory(size);
        return size;
    }

    @Override
    public Data toData(Object obj, DataType dataType) {
        Data data;
        if (dataType == DataType.NATIVE) {
            data = serializationService.toNativeData(obj, memoryManager);
        } else {
            data = serializationService.toData(obj, dataType);
        }
        if (data instanceof NativeMemoryData && data != obj) {
            storageInfo.addUsedMemory(recordAccessor.getSize((NativeMemoryData) data));
        }
        return data;
    }

    @Override
    public Object toObject(Object data) {
        return serializationService.toObject(data, memoryManager);
    }

    @Override
    public Data convertData(Data data, DataType dataType) {
        Data convertedData;
        if (dataType == DataType.NATIVE) {
            convertedData = serializationService.convertToNativeData(data, memoryManager);
        } else {
            convertedData = serializationService.convertData(data, dataType);
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
        serializationService.disposeData(data, memoryManager);
        storageInfo.removeUsedMemory(size);
    }

    @Override
    public long allocate(long size) {
        long address = memoryManager.allocate(size);
        storageInfo.addUsedMemory(getSize(address, size));
        return address;
    }

    @Override
    public void free(long address, long size) {
        long disposedSize = getSize(address, size);
        memoryManager.free(address, size);
        storageInfo.removeUsedMemory(disposedSize);
    }


    @Override
    public void addDeferredDispose(MemoryBlock memoryBlock) {
        if (memoryBlock.address() == MemoryAllocator.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal address!");
        }
        deferredBlocksQueue.add(memoryBlock);
    }

    @Override
    public void disposeDeferredBlocks() {
        MemoryBlock block;
        while ((block = deferredBlocksQueue.poll()) != null) {
            if (block.address() == MemoryAllocator.NULL_ADDRESS) {
                // already disposed
                continue;
            }

            if (block instanceof NativeMemoryData) {
                disposeData((NativeMemoryData) block);
            } else if (block instanceof HiDensityRecord) {
                dispose((R) block);
            } else {
                memoryManager.free(block.address(), block.size());
            }
        }
    }

    @Override
    public long getSize(MemoryBlock memoryBlock) {
        return recordAccessor.getSize(memoryBlock);
    }

    @Override
    public long getSize(long address, long expectedSize) {
        return recordAccessor.getSize(address, expectedSize);
    }

    @Override
    public MemoryAllocator unwrapMemoryAllocator() {
        return memoryManager.getSystemAllocator();
    }

    @Override
    public long getUsedMemory() {
        return storageInfo.getUsedMemory();
    }

    @Override
    public long increaseUsedMemory(long size) {
        return storageInfo.addUsedMemory(size);
    }

    @Override
    public long decreaseUsedMemory(long size) {
        return storageInfo.removeUsedMemory(size);
    }

    public HazelcastMemoryManager getMemoryManager() {
        return memoryManager;
    }

    public EnterpriseSerializationService getSerializationService() {
        return serializationService;
    }
}
