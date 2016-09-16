package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * {@link com.hazelcast.map.impl.nearcache.NearCacheRecord} accessor for {@link HiDensityNativeMemoryNearCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HiDensityNativeMemoryNearCacheRecordAccessor
        extends AbstractHiDensityRecordAccessor<HiDensityNativeMemoryNearCacheRecord> {

    public HiDensityNativeMemoryNearCacheRecordAccessor(EnterpriseSerializationService ss,
                                                        HazelcastMemoryManager memoryManager) {
        super(ss, memoryManager);
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord createRecord() {
        return new HiDensityNativeMemoryNearCacheRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        long valueAddress1 = AMEM.getLong(address1 + HiDensityNativeMemoryNearCacheRecord.VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + HiDensityNativeMemoryNearCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }
}
