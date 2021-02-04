package com.hazelcast.map.impl.record;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.hidensity.impl.AbstractHiDensityRecordAccessor;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;
import com.hazelcast.map.impl.MapContainer;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.map.impl.record.HDRecordWithStats.VALUE_OFFSET;

/**
 * Helps to create, read, dispose record or its data.
 */
public class HDMapRecordAccessor
        extends AbstractHiDensityRecordAccessor<HDRecord> {

    private final MapContainer mapContainer;

    public HDMapRecordAccessor(EnterpriseSerializationService ss,
                               MapContainer mapContainer) {
        super(ss, ss.getMemoryManager());
        this.mapContainer = mapContainer;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity",
            "checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
    protected HDRecord createRecord() {
        MapConfig mapConfig = mapContainer.getMapConfig();

        boolean hasEviction = mapContainer.getEvictor() != NULL_EVICTOR;
        boolean hasHotRestart = mapConfig.getHotRestartConfig().isEnabled();

        // when stats are enabled, return full record
        if (mapConfig.isStatisticsEnabled()) {
            return new HDRecordWithStats();
        }

        // when stats are disabled, return record based on static config.
        // return record which has only value and version in it.
        if (!hasEviction && !hasHotRestart) {
            return new HDSimpleRecordWithVersion();
        }

        // return record with only eviction related fields
        if (hasEviction && !hasHotRestart) {
            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LRU) {
                return new HDSimpleRecordWithLRUEviction();
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LFU) {
                return new HDSimpleRecordWithLFUEviction();
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.RANDOM) {
                return new HDSimpleRecordWithVersion();
            }
        }

        // return record with only hot-restart related fields
        if (!hasEviction && hasHotRestart) {
            return new HDSimpleRecordWithHotRestart();
        }

        // return record both eviction and hot-restart related fields
        if (hasEviction && hasHotRestart) {
            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LRU) {
                return new HDRecordWithLRUEvictionAndHotRestart();
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LFU) {
                return new HDSimpleRecordWithLFUEvictionAndHotRestart();
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.RANDOM) {
                return new HDSimpleRecordWithHotRestart();
            }
        }

        throw new IllegalStateException("No HD record type found matching with the provided " + mapConfig);
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
