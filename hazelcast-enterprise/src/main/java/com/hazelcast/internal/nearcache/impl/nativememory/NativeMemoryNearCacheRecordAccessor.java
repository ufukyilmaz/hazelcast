package com.hazelcast.internal.nearcache.impl.nativememory;

import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;

/**
 * Accessor for {@link NativeMemoryNearCacheRecord} to create, read, dispose record or its data.
 */
public class NativeMemoryNearCacheRecordAccessor
        extends AbstractHiDensityRecordAccessor<NativeMemoryNearCacheRecord> {

    NativeMemoryNearCacheRecordAccessor(EnterpriseSerializationService ss,
                                        HazelcastMemoryManager memoryManager) {
        super(ss, memoryManager);
    }

    @Override
    protected NativeMemoryNearCacheRecord createRecord() {
        return new NativeMemoryNearCacheRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        long valueAddress1 = AMEM.getLong(address1 + NativeMemoryNearCacheRecord.VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + NativeMemoryNearCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }
}
