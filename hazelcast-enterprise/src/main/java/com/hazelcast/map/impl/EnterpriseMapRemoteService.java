package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.eviction.HotRestartEvictionHelper;
import com.hazelcast.map.impl.proxy.EnterpriseMapProxyImpl;
import com.hazelcast.map.impl.proxy.EnterpriseNearCachedMapProxyImpl;

/**
 * Defines enterprise only remote service behavior for {@link MapService}.
 *
 * @see MapService
 */
class EnterpriseMapRemoteService extends MapRemoteService {

    private final HDMapConfigValidator hdMapConfigValidator;

    EnterpriseMapRemoteService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
        final HotRestartEvictionHelper hotRestartEvictionHelper = new HotRestartEvictionHelper(nodeEngine.getProperties());
        hdMapConfigValidator = new HDMapConfigValidator(hotRestartEvictionHelper);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        Config config = nodeEngine.getConfig();
        MapConfig mapConfig = config.findMapConfig(name);
        hdMapConfigValidator.checkHDConfig(mapConfig, config.getNativeMemoryConfig());

        if (mapConfig.isNearCacheEnabled()) {
            hdMapConfigValidator.checkHDConfig(mapConfig.getNearCacheConfig(), config.getNativeMemoryConfig(), false);

            return new EnterpriseNearCachedMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        } else {
            return new EnterpriseMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        }
    }
}
