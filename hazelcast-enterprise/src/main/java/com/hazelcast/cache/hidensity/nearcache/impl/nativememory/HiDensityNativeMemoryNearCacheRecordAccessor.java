package com.hazelcast.cache.hidensity.nearcache.impl.nativememory;

import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.memory.HazelcastMemoryManager;
import static com.hazelcast.spi.memory.GlobalMemoryAccessorRegistry.AMEM;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;

/**
 * Near-Cache record accessor for {@link HiDensityNativeMemoryNearCacheRecord}
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
        long valueAddress1 = AMEM.getLong(address1 + HiDensityNativeMemoryNearCacheRecord.VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + HiDensityNativeMemoryNearCacheRecord.VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
