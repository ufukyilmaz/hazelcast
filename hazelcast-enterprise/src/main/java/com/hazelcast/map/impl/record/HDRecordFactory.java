package com.hazelcast.map.impl.record;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.DataType;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.memory.NativeOutOfMemoryError;

import static com.hazelcast.internal.hidensity.HiDensityRecordStore.NULL_PTR;
import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;
import static com.hazelcast.map.impl.record.Record.UNSET;

/**
 * Factory for creating Hi-Density backed records. Created for every
 * partition. The records will be created in Hi-Density memory storage.
 */
public class HDRecordFactory implements RecordFactory<Data> {

    private final HiDensityRecordProcessor<HDRecord> recordProcessor;
    private final MapContainer mapContainer;

    public HDRecordFactory(HiDensityRecordProcessor<HDRecord> recordProcessor,
                           MapContainer mapContainer) {
        this.recordProcessor = recordProcessor;
        this.mapContainer = mapContainer;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity",
            "checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
    public Record<Data> newRecord(Object value) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        boolean hasEviction = mapContainer.getEvictor() != NULL_EVICTOR;
        boolean hasHotRestart = mapConfig.getHotRestartConfig().isEnabled();

        // when stats are enabled, return full record
        if (mapConfig.isPerEntryStatsEnabled() || isClusterV41()) {
            return newHDRecordWithExtras(value, HDRecordWithStats.SIZE);
        }

        // when stats are disabled, return record based on static config.
        // return record which has only value in it.
        if (!hasEviction && !hasHotRestart) {
            return newHDRecordWithExtras(value, HDSimpleRecordWithVersion.SIZE);
        }

        // return record with only eviction related fields
        if (hasEviction && !hasHotRestart) {
            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LRU) {
                return newHDRecordWithExtras(value, HDSimpleRecordWithLRUEviction.SIZE);
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LFU) {
                return newHDRecordWithExtras(value, HDSimpleRecordWithLFUEviction.SIZE);
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.RANDOM) {
                return newHDRecordWithExtras(value, HDSimpleRecordWithVersion.SIZE);
            }

            return newHDRecordWithExtras(value, HDRecordWithStats.SIZE);
        }

        // return record with only hot-restart related fields
        if (!hasEviction && hasHotRestart) {
            return newHDRecordWithExtras(value, HDSimpleRecordWithHotRestart.SIZE);
        }

        // return record both eviction and hot-restart related fields
        if (hasEviction && hasHotRestart) {
            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LRU) {
                return newHDRecordWithExtras(value, HDRecordWithLRUEvictionAndHotRestart.SIZE);
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LFU) {
                return newHDRecordWithExtras(value, HDSimpleRecordWithLFUEvictionAndHotRestart.SIZE);
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.RANDOM) {
                return newHDRecordWithExtras(value, HDSimpleRecordWithHotRestart.SIZE);
            }

            return newHDRecordWithExtras(value, HDRecordWithStats.SIZE);
        }

        throw new IllegalStateException("No HD record type found matching with the provided " + mapConfig);
    }

    @Override
    public MapContainer geMapContainer() {
        return mapContainer;
    }

    private HDRecord newHDRecordWithExtras(Object value, long size) {
        long address = NULL_PTR;
        Data dataValue = null;
        try {
            address = recordProcessor.allocate(size);
            HDRecord record = recordProcessor.newRecord();
            record.reset(address);

            dataValue = recordProcessor.toData(value, DataType.NATIVE);
            record.setValue(dataValue);
            record.setLastAccessTime(UNSET);

            return record;
        } catch (NativeOutOfMemoryError error) {
            if (!isNull(dataValue)) {
                recordProcessor.disposeData(dataValue);
            }
            if (address != NULL_PTR) {
                recordProcessor.dispose(address);
            }
            throw error;
        }
    }

    public HiDensityRecordProcessor<HDRecord> getRecordProcessor() {
        return recordProcessor;
    }

    // only used in tests
    static boolean isNull(Object object) {
        if (object == null) {
            return false;
        }

        NativeMemoryData memoryBlock = (NativeMemoryData) object;
        return memoryBlock.address() == NULL_PTR;
    }
}
