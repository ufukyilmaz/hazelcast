package com.hazelcast.hidensity.impl;

import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryData;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @author sozal 18/02/15
 *
 * @param <R> the type of the {@link HiDensityRecord} to be accessed
 */
public abstract class AbstractHiDensityRecordAccessor<R extends HiDensityRecord>
        implements HiDensityRecordAccessor<R> {

    protected final EnterpriseSerializationService ss;
    protected final MemoryManager memoryManager;
    protected final Queue<R> recordQ = new ArrayDeque<R>(1024);
    protected final Queue<NativeMemoryData> dataQ = new ArrayDeque<NativeMemoryData>(1024);

    public AbstractHiDensityRecordAccessor(EnterpriseSerializationService ss,
                                           MemoryManager memoryManager) {
        this.ss = ss;
        this.memoryManager = memoryManager;
    }

    protected abstract R createRecord();
    public abstract boolean isEqual(long address1, long address2);

    @Override
    public boolean isEqual(long address, R value) {
        return isEqual(address, value.address());
    }

    @Override
    public R newRecord() {
        R record = recordQ.poll();
        if (record == null) {
            record = createRecord(this);
        }
        return record;
    }

    @Override
    public R read(long address) {
        if (address <= MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        R record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public long dispose(R record) {
        if (record.address() <= MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        long size = 0L;
        size += disposeValue(record);
        record.clear();
        size += getSize(record);
        memoryManager.free(record.address(), record.size());
        record.reset(MemoryManager.NULL_ADDRESS);
        recordQ.offer(record);
        return size;
    }

    @Override
    public long dispose(long address) {
        return dispose(read(address));
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        if (valueAddress <= MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
        }
        NativeMemoryData value = dataQ.poll();
        if (value == null) {
            value = new NativeMemoryData();
        }
        return value.reset(valueAddress);
    }

    @Override
    public Object readValue(R record, boolean enqueueDataOnFinish) {
        NativeMemoryData nativeMemoryData = readData(record.getValueAddress());
        try {
            return ss.toObject(nativeMemoryData, memoryManager);
        } finally {
            if (enqueueDataOnFinish) {
                enqueueData(nativeMemoryData);
            }
        }
    }

    @Override
    public long disposeValue(R record) {
        long valueAddress = record.getValueAddress();
        long size = 0L;
        if (valueAddress != MemoryManager.NULL_ADDRESS) {
            size = disposeData(valueAddress);
            record.setValueAddress(MemoryManager.NULL_ADDRESS);
        }
        return size;
    }

    @Override
    public long disposeData(NativeMemoryData value) {
        long size = getSize(value);
        ss.disposeData(value, memoryManager);
        dataQ.offer(value);
        return size;
    }

    @Override
    public long disposeData(long address) {
        return disposeData(readData(address));
    }

    @Override
    public void enqueueRecord(R record) {
        record.reset(MemoryManager.NULL_ADDRESS);
        recordQ.offer(record);
    }

    @Override
    public void enqueueData(NativeMemoryData data) {
        data.reset(MemoryManager.NULL_ADDRESS);
        dataQ.offer(data);
    }

    @Override
    public int getSize(MemoryBlock memoryBlock) {
        if (memoryBlock == null || memoryBlock.address() == MemoryManager.NULL_ADDRESS) {
            return  0;
        }
        int size = memoryManager.getSize(memoryBlock.address());
        if (size == MemoryManager.SIZE_INVALID) {
            size = memoryBlock.size();
        }

        return size;
    }

    @Override
    public long getSize(long address, long expectedSize) {
        long size = memoryManager.getSize(address);
        if (size == MemoryManager.SIZE_INVALID) {
            size = expectedSize;
        }
        return size;
    }

}
