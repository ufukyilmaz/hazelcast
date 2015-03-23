package com.hazelcast.map.impl;

import com.hazelcast.spi.ReplicationSupportingService;

/**
 * Enterprise implementation of {@link MapServiceContextAwareFactory}.
 * Contains enterprise specific implementations of spi services.
 *
 * @see MapServiceContextAwareFactory
 */
class EnterpriseMapServiceContextAwareFactory extends DefaultMapServiceContextAwareFactory {

    public EnterpriseMapServiceContextAwareFactory(EnterpriseMapServiceContext mapServiceContext) {
        super(mapServiceContext);
    }

    @Override
    ReplicationSupportingService createReplicationSupportingService() {
        EnterpriseMapServiceContext mapServiceContext = getEnterpriseMapServiceContext();
        return new EnterpriseMapReplicationSupportingService(mapServiceContext);
    }

    private EnterpriseMapServiceContext getEnterpriseMapServiceContext() {
        return (DefaultEnterpriseMapServiceContext) getMapServiceContext();
    }
}
