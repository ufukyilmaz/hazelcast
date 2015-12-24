package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;

import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Enterprise extension of {@link MapOperationProviders} to provide
 * {@link HDMapOperationProvider} instances if needed.
 */
public class EnterpriseMapOperationProviders extends MapOperationProviders {

    private final MapOperationProvider hdWanAwareProvider;
    private final MapOperationProvider hdMapOperationProvider = new HDMapOperationProvider();

    public EnterpriseMapOperationProviders(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        this.hdWanAwareProvider = new WANAwareOperationProvider(mapServiceContext, hdMapOperationProvider);
    }

    @Override
    public MapOperationProvider getOperationProvider(String name) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        MapConfig mapConfig = mapContainer.getMapConfig();
        InMemoryFormat inMemoryFormat = mapConfig.getInMemoryFormat();

        if (NATIVE == inMemoryFormat) {
            return mapContainer.isWanReplicationEnabled() ? hdWanAwareProvider : hdMapOperationProvider;
        } else {
            return super.getOperationProvider(name);
        }
    }
}
