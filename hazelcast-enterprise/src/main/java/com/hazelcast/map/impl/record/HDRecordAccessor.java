package com.hazelcast.map.impl.record;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.nio.UnsafeHelper;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import static com.hazelcast.map.impl.record.HDRecord.VALUE_OFFSET;

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
            return statisticEnabled ? new CachedHDRecordWithStats(this)
                    : new HDRecordWithCachedValue(this);
        }
        return statisticEnabled ? new HDRecordWithStats(this) : new HDRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + VALUE_OFFSET);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
