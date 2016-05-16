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

    /**
     * Returns a {@link MapOperationProvider} instance, depending on whether the provided {@code MapConfig} has a
     * WAN replication policy configured or not.
     *
     * @param mapConfig the map configuration to query whether WAN replication is configured
     * @return {@link DefaultMapOperationProvider} or {@link WANAwareOperationProvider} depending on the WAN replication
     * config of the map configuration provided as parameter.
     */
    @Override
    public MapOperationProvider getOperationProvider(MapConfig mapConfig) {
        InMemoryFormat inMemoryFormat = mapConfig.getInMemoryFormat();

        if (NATIVE == inMemoryFormat) {
            return mapConfig.getWanReplicationRef() == null ? hdMapOperationProvider : hdWanAwareProvider;
        } else {
            return super.getOperationProvider(mapConfig);
        }
    }
}
