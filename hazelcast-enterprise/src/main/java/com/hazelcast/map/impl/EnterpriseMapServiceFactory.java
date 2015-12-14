package com.hazelcast.map.impl;

import com.hazelcast.map.impl.event.EnterpriseMapEventPublishingService;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainHandlerService;

/**
 * Enterprise implementation of {@link MapServiceFactory}.
 * Contains enterprise specific implementations of spi services.
 *
 * @see DefaultMapServiceFactory
 */
class EnterpriseMapServiceFactory extends DefaultMapServiceFactory {

    EnterpriseMapServiceFactory(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    ReplicationSupportingService createReplicationSupportingService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapReplicationSupportingService(mapServiceContext);
    }

    @Override
    MigrationAwareService createMigrationAwareService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapMigrationAwareService(mapServiceContext);
    }

    @Override
    ClientAwareService createClientAwareService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapClientAwareService(mapServiceContext);
    }

    @Override
    EventPublishingService createEventPublishingService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapEventPublishingService(mapServiceContext);
    }

    @Override
    PostJoinAwareService createPostJoinAwareService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapPostJoinAwareService(mapServiceContext);
    }

    @Override
    RemoteService createRemoteService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapRemoteService(mapServiceContext);
    }

    @Override
    SplitBrainHandlerService createSplitBrainHandlerService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapSplitBrainHandlerService(mapServiceContext);
    }

    private EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (EnterpriseMapServiceContextImpl) getMapServiceContext();
    }
}
