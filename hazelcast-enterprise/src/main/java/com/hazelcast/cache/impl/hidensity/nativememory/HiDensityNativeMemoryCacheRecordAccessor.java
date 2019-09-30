package com.hazelcast.cache.impl.hidensity.nativememory;

import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HiDensityNativeMemoryCacheRecordAccessor
        extends AbstractHiDensityRecordAccessor<HiDensityNativeMemoryCacheRecord> {

    public HiDensityNativeMemoryCacheRecordAccessor(
            EnterpriseSerializationService ss, HazelcastMemoryManager memoryManager) {
        super(ss, memoryManager);
    }

    @Override
    protected HiDensityNativeMemoryCacheRecord createRecord() {
        return new HiDensityNativeMemoryCacheRecord(this);
    }

    @Override
    public long dispose(HiDensityNativeMemoryCacheRecord record) {
        NativeMemoryData expiryPolicyData = record.getExpiryPolicy();
        long size = super.dispose(record);
        if (expiryPolicyData != null) {
            size += disposeData(expiryPolicyData);
        }
        return size;
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        long valueAddress1 = AMEM.getLong(address1 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + HiDensityNativeMemoryCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
