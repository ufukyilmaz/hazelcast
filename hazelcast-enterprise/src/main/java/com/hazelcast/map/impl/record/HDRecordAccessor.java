package com.hazelcast.map.impl.record;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.map.impl.record.HDRecord.VALUE_OFFSET;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HDRecordAccessor
        extends AbstractHiDensityRecordAccessor<HDRecord> {

    private final boolean optimizeQuery;

    public HDRecordAccessor(EnterpriseSerializationService serializationService, boolean optimizeQuery) {
        super(serializationService, serializationService.getMemoryManager());
        this.optimizeQuery = optimizeQuery;
    }

    @Override
    protected HDRecord createRecord() {
        return optimizeQuery ? new HDRecordWithCachedValue(this) : new HDRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        long valueAddress1 = AMEM.getLong(address1 + VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
