package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.internal.hidensity.HiDensityRecord;
import com.hazelcast.internal.hidensity.HiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

/**
 * @param <R> Type of the {@link HiDensityRecord} to be accessed.
 */
public abstract class AbstractHiDensityRecordAccessor<R extends HiDensityRecord>
        implements HiDensityRecordAccessor<R> {

    protected final EnterpriseSerializationService ss;
    protected final HazelcastMemoryManager memoryManager;

    public AbstractHiDensityRecordAccessor(EnterpriseSerializationService ss,
                                           HazelcastMemoryManager memoryManager) {
        this.ss = ss;
        this.memoryManager = memoryManager;
    }

    protected abstract R createRecord();

    @Override
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
        if (address == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + address);
        }
        R record = newRecord();
        record.reset(address);
        return record;
    }

    @Override
    public long dispose(R record) {
        if (record.address() == HazelcastMemoryManager.NULL_ADDRESS) {
            throw new IllegalArgumentException("Illegal memory address: " + record.address());
        }
        long size = 0L;
        size += disposeValue(record);
        record.clear();
        size += getSize(record);
        memoryManager.free(record.address(), record.size());
        record.reset(HazelcastMemoryManager.NULL_ADDRESS);
        return size;
    }

    @Override
    public long dispose(long address) {
        return dispose(read(address));
    }

    @Override
    public NativeMemoryData readData(long valueAddress) {
        if (valueAddress == HazelcastMemoryManager.NULL_ADDRESS) {
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
        if (valueAddress != HazelcastMemoryManager.NULL_ADDRESS) {
            size = disposeData(valueAddress);
            record.setValueAddress(HazelcastMemoryManager.NULL_ADDRESS);
        }
        return size;
    }

    @Override
    public long disposeData(NativeMemoryData value) {
        if (value.address() == HazelcastMemoryManager.NULL_ADDRESS) {
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
