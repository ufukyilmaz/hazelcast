package com.hazelcast.map.impl.record;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HDRecordAccessor
        extends AbstractHiDensityRecordAccessor<HDRecord> {

    private final boolean statisticEnabled;
    private final boolean optimizeQuery;

    public HDRecordAccessor(boolean statisticEnabled, boolean optimizeQuery,
                            EnterpriseSerializationService serializationService) {
        super(serializationService, serializationService.getMemoryManager());
        this.statisticEnabled = statisticEnabled;
        this.optimizeQuery = optimizeQuery;
    }

    @Override
    protected HDRecord createRecord() {
        if (optimizeQuery) {
            return statisticEnabled ? new StatsAwareHDRecordWithCachedValue(this)
                    : new SimpleHDRecordWithCachedValue(this);
        } else {
            return statisticEnabled ? new StatsAwareHDRecord(this) : new SimpleHDRecord(this);
        }
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        int valueOffset = statisticEnabled ? StatsAwareHDRecord.VALUE_OFFSET : SimpleHDRecord.VALUE_OFFSET;

        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + valueOffset);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + valueOffset);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
