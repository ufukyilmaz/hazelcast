package com.hazelcast.map.impl;

import com.hazelcast.spi.NodeEngine;

/**
 * Defines enterprise only remote service behavior for {@link MapService}
 *
 * @see MapService
 */
class EnterpriseMapRemoteService extends MapRemoteService {

    public EnterpriseMapRemoteService(MapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    public EnterpriseMapProxyImpl createDistributedObject(String name) {
        MapServiceContext mapServiceContext = getMapServiceContext();
        MapService service = mapServiceContext.getService();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return new EnterpriseMapProxyImpl(name, service, nodeEngine);
    }

}
