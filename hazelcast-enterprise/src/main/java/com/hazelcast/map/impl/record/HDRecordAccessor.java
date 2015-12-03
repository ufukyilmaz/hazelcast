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

    private final boolean optimizeQuery;

    private final boolean hotRestartEnabled;

    public HDRecordAccessor(EnterpriseSerializationService serializationService,
                            boolean optimizeQuery, boolean hotRestartEnabled) {
        super(serializationService, serializationService.getMemoryManager());
        this.optimizeQuery = optimizeQuery;
        this.hotRestartEnabled = hotRestartEnabled;
    }

    @Override
    protected HDRecord createRecord() {
        if (hotRestartEnabled) {
            return optimizeQuery ? new HotRestartHDRecordWithCachedValue(this) : new HotRestartHDRecord(this);
        }
        return optimizeQuery ? new HDRecordWithCachedValue(this) : new HDRecord(this);
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        long valueAddress1 = UnsafeHelper.UNSAFE.getLong(address1 + VALUE_OFFSET);
        long valueAddress2 = UnsafeHelper.UNSAFE.getLong(address2 + VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
