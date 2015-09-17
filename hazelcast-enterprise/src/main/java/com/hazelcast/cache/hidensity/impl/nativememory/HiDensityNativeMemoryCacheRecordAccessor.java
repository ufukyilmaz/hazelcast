package com.hazelcast.cache.hidensity.impl.nativememory;

import com.hazelcast.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HiDensityNativeMemoryCacheRecordAccessor
        extends AbstractHiDensityRecordAccessor<HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheRecordAccessor(EnterpriseSerializationService ss,
                                                    MemoryManager memoryManager) {
        super(ss, memoryManager);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord() {
        return new HiDensityNativeMemoryCacheRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS) {
            return false;
        }
        if (address2 == NULL_ADDRESS) {
            return false;
        }
        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
