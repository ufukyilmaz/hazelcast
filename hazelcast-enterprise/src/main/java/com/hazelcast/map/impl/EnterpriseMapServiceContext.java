package com.hazelcast.map.impl;

import com.hazelcast.map.impl.wan.MapFilterProvider;
import com.hazelcast.spi.hotrestart.HotRestartStore;

/**
 * Enterprise version of {@link com.hazelcast.map.impl.MapServiceContext}.
 */
public interface EnterpriseMapServiceContext extends MapServiceContext {

    MapFilterProvider getMapFilterProvider();

    HotRestartStore getOnHeapHotRestartStoreForPartition(int partitionId);

    HotRestartStore getOffHeapHotRestartStoreForPartition(int partitionId);
}
