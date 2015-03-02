package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.hidensity.HiDensityCacheRecordAccessor;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.nio.serialization.NativeMemoryDataUtil;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * for creating, reading, disposing record or its data.
 */
public class HiDensityNativeMemoryCacheRecordAccessor
        implements HiDensityCacheRecordAccessor<HiDensityNativeMemoryCacheRecord> {

    private final EnterpriseSerializationService ss;
    private final MemoryManager memoryManager;
    private final Queue<HiDensityNativeMemoryCacheRecord> recordQ = new ArrayDeque<HiDensityNativeMemoryCacheRecord>(1024);
    private final Queue<NativeMemoryData> dataQ = new ArrayDeque<NativeMemoryData>(1024);

    public HiDensityNativeMemoryCacheRecordAccessor(EnterpriseSerializationService ss) {
        this.ss = ss;
        this.memoryManager = ss.getMemoryManager();
    }

    @Override
    public boolean isEqual(long address, HiDensityNativeMemoryCacheRecord value) {
        return isEqual(address, value.address());
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

    @Override
    public HiDensityNativeMemoryCacheRecord newRecord() {
        HiDensityNativeMemoryCacheRecord record = recordQ.poll();
        if (record == null) {
            record = new HiDensityNativeMemoryCacheRecord(this);
        }
        return record;
    }

    @Override
    public HiDensityNativeMemoryCacheRecord read(long address) {
        if (address <= HiDensityCacheRecordStore.NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        HiDensityNativeMemoryCacheRecord record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public long dispose(HiDensityNativeMemoryCacheRecord record) {
        if (record.address() <= HiDensityCacheRecordStore.NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        long size = 0L;
        size += disposeValue(record);
        record.clear();
        size += getSize(record);
        memoryManager.free(record.address(), record.size());
        recordQ.offer(record.reset(HiDensityCacheRecordStore.NULL_PTR));
        return size;
    }

    @Override
    public long dispose(long address) {
        return dispose(read(address));
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        if (valueAddress <= HiDensityCacheRecordStore.NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
        }
        NativeMemoryData value = dataQ.poll();
        if (value == null) {
            value = new NativeMemoryData();
        }
        return value.reset(valueAddress);
    }

    @Override
    public Object readValue(HiDensityNativeMemoryCacheRecord record,
                            boolean enqueeDataOnFinish) {
        NativeMemoryData nativeMemoryData = readData(record.getValueAddress());
        try {
            return ss.toObject(nativeMemoryData);
        } finally {
            if (enqueeDataOnFinish) {
                enqueueData(nativeMemoryData);
            }
        }
    }

    @Override
    public long disposeValue(HiDensityNativeMemoryCacheRecord record) {
        long valueAddress = record.getValueAddress();
        long size = 0L;
        if (valueAddress != HiDensityCacheRecordStore.NULL_PTR) {
            size = disposeData(valueAddress);
            record.setValueAddress(HiDensityCacheRecordStore.NULL_PTR);
        }
        return size;
    }

    @Override
    public long disposeData(NativeMemoryData value) {
        long size = getSize(value);
        ss.disposeData(value);
        dataQ.offer(value);
        return size;
    }

    @Override
    public long disposeData(long address) {
        return disposeData(readData(address));
    }

    @Override
    public void enqueueRecord(HiDensityNativeMemoryCacheRecord record) {
        recordQ.offer(record.reset(HiDensityCacheRecordStore.NULL_PTR));
    }

    @Override
    public void enqueueData(NativeMemoryData data) {
        data.reset(HiDensityCacheRecordStore.NULL_PTR);
        dataQ.offer(data);
    }

    @Override
    public long getSize(long address, long expectedSize) {
        long size = memoryManager.getSize(address);
        if (size == MemoryManager.SIZE_INVALID) {
            size = expectedSize;
        }
        return size;
    }

    @Override
    public long getSize(MemoryBlock memoryBlock) {
        if (memoryBlock == null) {
            return  0;
        }
        int size = memoryManager.getSize(memoryBlock.address());
        if (size == MemoryManager.SIZE_INVALID) {
            size = memoryBlock.size();
        }
        return size;
    }

}
