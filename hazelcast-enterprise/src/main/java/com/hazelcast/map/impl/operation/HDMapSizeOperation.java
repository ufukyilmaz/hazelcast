package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

public class HDMapSizeOperation extends HDMapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private int size;

    public HDMapSizeOperation() {
    }

    public HDMapSizeOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        recordStore.checkIfLoaded();
        size = recordStore.size();
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
            LocalMapStatsImpl localMapStatsImpl = localMapStatsProvider.getLocalMapStatsImpl(name);
            localMapStatsImpl.incrementOtherOperations();
        }
    }

    @Override
    public Object getResponse() {
        return size;
    }

}
