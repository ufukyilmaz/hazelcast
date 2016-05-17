package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.proxy.EnterpriseMapProxyImpl;
import com.hazelcast.map.impl.proxy.EnterpriseNearCachedMapProxyImpl;

import static com.hazelcast.map.impl.HDMapConfigValidator.checkHDConfig;

/**
 * Defines enterprise only remote service behavior for {@link MapService}
 *
 * @see MapService
 */
class EnterpriseMapRemoteService extends MapRemoteService {

    EnterpriseMapRemoteService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        MapConfig mapConfig = nodeEngine.getConfig().findMapConfig(name);
        checkHDConfig(mapConfig);

        if (mapConfig.isNearCacheEnabled()) {
            checkHDConfig(mapConfig.getNearCacheConfig());

            return new EnterpriseNearCachedMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        } else {
            return new EnterpriseMapProxyImpl(name, mapServiceContext.getService(), nodeEngine, mapConfig);
        }
    }
}
