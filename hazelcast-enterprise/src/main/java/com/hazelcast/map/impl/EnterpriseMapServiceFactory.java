package com.hazelcast.map.impl;

import com.hazelcast.map.impl.event.MapEventPublishingService;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;

/**
 * Enterprise implementation of {@link MapServiceFactory}.
 * Contains enterprise specific implementations of SPI services.
 *
 * @see DefaultMapServiceFactory
 */
class EnterpriseMapServiceFactory extends DefaultMapServiceFactory {

    EnterpriseMapServiceFactory(NodeEngine nodeEngine, EnterpriseMapServiceContext mapServiceContext) {
        super(nodeEngine, mapServiceContext);
    }

    @Override
    ReplicationSupportingService createReplicationSupportingService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapReplicationSupportingService(mapServiceContext);
    }

    @Override
    ClientAwareService createClientAwareService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new MapClientAwareService(mapServiceContext);
    }

    @Override
    EventPublishingService createEventPublishingService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new MapEventPublishingService(mapServiceContext);
    }

    private EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContextImpl) getMapServiceContext();
    }
}
