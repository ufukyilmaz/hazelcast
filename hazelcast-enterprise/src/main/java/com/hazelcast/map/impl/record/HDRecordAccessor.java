package com.hazelcast.map.impl.record;

import com.hazelcast.cache.impl.hidensity.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.map.impl.record.HDRecord.VALUE_OFFSET;

/**
 * Cache record accessor for {@link HiDensityNativeMemoryCacheRecord}
 * to create, read, dispose record or its data.
 */
public class HDRecordAccessor
        extends AbstractHiDensityRecordAccessor<HDRecord> {

    public HDRecordAccessor(EnterpriseSerializationService serializationService) {
        super(serializationService, serializationService.getMemoryManager());
    }

    @Override
    protected HDRecord createRecord() {
        return new HDRecord();
    }

    @Override
    public boolean isEqual(long address1, long address2) {
        if (address1 == address2) {
            return true;
        }
        if (address1 == NULL_ADDRESS || address2 == NULL_ADDRESS) {
            return false;
        }
        long valueAddress1 = AMEM.getLong(address1 + VALUE_OFFSET);
        long valueAddress2 = AMEM.getLong(address2 + VALUE_OFFSET);
        return NativeMemoryDataUtil.equals(valueAddress1, valueAddress2);
    }

}
