package com.hazelcast.map.impl.query;

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
}
