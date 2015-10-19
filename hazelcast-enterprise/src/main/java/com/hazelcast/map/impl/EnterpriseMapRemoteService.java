package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.proxy.EnterpriseMapProxyImpl;
import com.hazelcast.map.impl.proxy.EnterpriseNearCachedMapProxyImpl;
import com.hazelcast.spi.NodeEngine;

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
        MapService service = mapServiceContext.getService();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        if (mapConfig.isNearCacheEnabled()) {
            return new EnterpriseNearCachedMapProxyImpl(name, service, nodeEngine);
        } else {
            return new EnterpriseMapProxyImpl(name, service, nodeEngine);
        }
    }

}
