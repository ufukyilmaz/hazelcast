package com.hazelcast.hidensity.impl;

import com.hazelcast.hidensity.HiDensityRecord;
import com.hazelcast.hidensity.HiDensityRecordAccessor;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

/**
 * @author sozal 18/02/15
 *
 * @param <R> the type of the {@link HiDensityRecord} to be accessed
 */
public abstract class AbstractHiDensityRecordAccessor<R extends HiDensityRecord>
        implements HiDensityRecordAccessor<R> {

    protected final EnterpriseSerializationService ss;
    protected final MemoryManager memoryManager;

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
        return createRecord();
    }

    @Override
    public R read(long address) {
        if (address == MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        R record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public long dispose(R record) {
        if (record.address() == MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        long size = 0L;
        size += disposeValue(record);
        record.clear();
        size += getSize(record);
        memoryManager.free(record.address(), record.size());
        record.reset(MemoryManager.NULL_ADDRESS);
        return size;
    }

    @Override
    public long dispose(long address) {
        return dispose(read(address));
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        if (valueAddress == MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + valueAddress);
        }
        return new NativeMemoryData().reset(valueAddress);
    }

    @Override
    public Object readValue(R record) {
        NativeMemoryData nativeMemoryData = readData(record.getValueAddress());
        return ss.toObject(nativeMemoryData, memoryManager);
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
        if (value.address() == MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + value.address());
        }
        long size = getSize(value);
        ss.disposeData(value, memoryManager);
        return size;
    }

    @Override
    public long disposeData(long address) {
        return disposeData(readData(address));
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
        if (address == MemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        long size = memoryManager.getSize(address);
        if (size == MemoryManager.SIZE_INVALID) {
            size = expectedSize;
        }
        return size;
    }

}
