package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.cache.HiDensityCacheRecordAccessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.OffHeapData;
import com.hazelcast.nio.serialization.OffHeapDataUtil;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.hazelcast.cache.HiDensityCacheRecordStore.NULL_PTR;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * for creating, reading, disposing record or its data.
 */
public class HiDensityNativeMemoryCacheRecordAccessor
        implements HiDensityCacheRecordAccessor<HiDensityNativeMemoryCacheRecord> {

    private final EnterpriseSerializationService ss;
    private final MemoryManager memoryManager;
    private final Queue<HiDensityNativeMemoryCacheRecord> recordQ = new ArrayDeque<HiDensityNativeMemoryCacheRecord>(1024);
    private final Queue<OffHeapData> dataQ = new ArrayDeque<OffHeapData>(1024);

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
        return OffHeapDataUtil.equals(valueAddress1, valueAddress2);
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
        if (address <= NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        HiDensityNativeMemoryCacheRecord record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public void dispose(HiDensityNativeMemoryCacheRecord record) {
        if (record.address() <= NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        disposeValue(record);
        record.clear();
        memoryManager.free(record.address(), record.size());
        recordQ.offer(record.reset(NULL_PTR));
    }

    @Override
    public void dispose(long address) {
        dispose(read(address));
    }

    @Override
    public OffHeapData readData(long valueAddress) {
        if (valueAddress <= NULL_PTR) {
            throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
        }
        OffHeapData value = dataQ.poll();
        if (value == null) {
            value = new OffHeapData();
        }
        return value.reset(valueAddress);
    }

    @Override
    public Object readValue(HiDensityNativeMemoryCacheRecord record,
                            boolean enqueeDataOnFinish) {
        OffHeapData offHeapData = readData(record.getValueAddress());
        try {
            return ss.toObject(offHeapData);
        } finally {
            if (enqueeDataOnFinish) {
                enqueueData(offHeapData);
            }
        }
    }

    @Override
    public void disposeValue(HiDensityNativeMemoryCacheRecord record) {
        long valueAddress = record.getValueAddress();
        if (valueAddress != NULL_PTR) {
            disposeData(valueAddress);
            record.setValueAddress(NULL_PTR);
        }
    }

    @Override
    public void disposeData(OffHeapData value) {
        ss.disposeData(value);
        dataQ.offer(value);
    }

    @Override
    public void disposeData(long address) {
        OffHeapData data = readData(address);
        disposeData(data);
    }

    public void enqueueRecord(HiDensityNativeMemoryCacheRecord record) {
        recordQ.offer(record.reset(NULL_PTR));
    }

    public void enqueueData(OffHeapData data) {
        data.reset(NULL_PTR);
        dataQ.offer(data);
    }

}
