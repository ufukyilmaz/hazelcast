package com.hazelcast.map.impl.query;

import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;

/**
 * Used only with Hi-Density memory.
 */
public class HDPartitionScanRunner extends PartitionScanRunner {

    public HDPartitionScanRunner(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    // difference between OS and EE with HD: EE version always caches values
    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer, int partitionId) {
        return true;
    }

    @Override
    // difference between OS and EE with HD: transform native memory data to heap data
    protected <T> Object toData(T input) {
        if (!(input instanceof NativeMemoryData)) {
            return input;
        }
        return mapServiceContext.toData(input);
    }

}
