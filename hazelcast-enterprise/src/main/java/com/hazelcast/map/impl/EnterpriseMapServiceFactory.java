package com.hazelcast.map.impl;

import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.map.impl.wan.WanEnterpriseMapSupportingService;
import com.hazelcast.spi.impl.NodeEngine;

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
    WanSupportingService createReplicationSupportingService() {
        return new WanEnterpriseMapSupportingService(getMapServiceContext());
    }
}
