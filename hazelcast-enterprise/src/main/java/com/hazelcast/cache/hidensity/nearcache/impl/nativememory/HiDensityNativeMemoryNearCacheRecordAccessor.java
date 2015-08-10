package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.NativeMemoryDataUtil;

/**
 * Near-Cache record accessor for {@link HiDensityNativeMemoryNearCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HiDensityNativeMemoryNearCacheRecordAccessor
        extends AbstractHiDensityRecordAccessor<HiDensityNativeMemoryNearCacheRecord> {

    public HiDensityNativeMemoryNearCacheRecordAccessor(EnterpriseSerializationService ss,
                                                        MemoryManager memoryManager) {
        super(ss, memoryManager);
    }

    @Override
    protected HiDensityNativeMemoryNearCacheRecord createRecord() {
        return new HiDensityNativeMemoryNearCacheRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + HiDensityNativeMemoryNearCacheRecord.VALUE_OFFSET);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + HiDensityNativeMemoryNearCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
