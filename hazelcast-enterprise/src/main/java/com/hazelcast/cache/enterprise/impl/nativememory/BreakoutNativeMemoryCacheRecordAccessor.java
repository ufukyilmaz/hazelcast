package com.hazelcast.cache.enterprise.impl.nativememory;

import com.hazelcast.cache.enterprise.BreakoutCacheRecordStore;
import com.hazelcast.cache.enterprise.BreakoutCacheRecordAccessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;
import com.hazelcast.nio.serialization.NativeMemoryDataUtil;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Cache record accessor for {@link BreakoutNativeMemoryCacheRecord}
 * for creating, reading, disposing record or its data.
 */
public class BreakoutNativeMemoryCacheRecordAccessor
        implements BreakoutCacheRecordAccessor<BreakoutNativeMemoryCacheRecord> {

    private final EnterpriseSerializationService ss;
    private final MemoryManager memoryManager;
    private final Queue<BreakoutNativeMemoryCacheRecord> recordQ = new ArrayDeque<BreakoutNativeMemoryCacheRecord>(1024);
    private final Queue<NativeMemoryData> dataQ = new ArrayDeque<NativeMemoryData>(1024);

    public BreakoutNativeMemoryCacheRecordAccessor(EnterpriseSerializationService ss) {
        this.ss = ss;
        this.memoryManager = ss.getMemoryManager();
    }

    @Override
    public boolean isEqual(long address, BreakoutNativeMemoryCacheRecord value) {
        return isEqual(address, value.address());
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + BreakoutNativeMemoryCacheRecord.VALUE_OFFSET);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + BreakoutNativeMemoryCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

    @Override
    public BreakoutNativeMemoryCacheRecord newRecord() {
        BreakoutNativeMemoryCacheRecord record = recordQ.poll();
        if (record == null) {
            record = new BreakoutNativeMemoryCacheRecord(this);
        }
        return record;
    }

    @Override
    public BreakoutNativeMemoryCacheRecord read(long address) {
        if (address <= BreakoutCacheRecordStore.NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        BreakoutNativeMemoryCacheRecord record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public void dispose(BreakoutNativeMemoryCacheRecord record) {
        if (record.address() <= BreakoutCacheRecordStore.NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        disposeValue(record);
        record.clear();
        memoryManager.free(record.address(), record.size());
        recordQ.offer(record.reset(BreakoutCacheRecordStore.NULL_PTR));
    }

    @Override
    public void dispose(long address) {
        dispose(read(address));
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        if (valueAddress <= BreakoutCacheRecordStore.NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
        }
        NativeMemoryData value = dataQ.poll();
        if (value == null) {
            value = new NativeMemoryData();
        }
        return value.reset(valueAddress);
    }

    @Override
    public Object readValue(BreakoutNativeMemoryCacheRecord record,
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
    public void disposeValue(BreakoutNativeMemoryCacheRecord record) {
        long valueAddress = record.getValueAddress();
        if (valueAddress != BreakoutCacheRecordStore.NULL_PTR) {
            disposeData(valueAddress);
            record.setValueAddress(BreakoutCacheRecordStore.NULL_PTR);
        }
    }

    @Override
    public void disposeData(NativeMemoryData value) {
        ss.disposeData(value);
        dataQ.offer(value);
    }

    @Override
    public void disposeData(long address) {
        NativeMemoryData data = readData(address);
        disposeData(data);
    }

    public void enqueueRecord(BreakoutNativeMemoryCacheRecord record) {
        recordQ.offer(record.reset(BreakoutCacheRecordStore.NULL_PTR));
    }

    public void enqueueData(NativeMemoryData data) {
        data.reset(BreakoutCacheRecordStore.NULL_PTR);
        dataQ.offer(data);
    }

}
