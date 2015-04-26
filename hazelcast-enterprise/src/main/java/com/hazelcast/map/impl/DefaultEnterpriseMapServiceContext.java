package com.hazelcast.map.impl;

import com.hazelcast.spi.NodeEngine;


/**
 * Contains enterprise specific implementations of {@link MapServiceContext}
 * functionality.
 *
 * @see MapServiceContext
 */
class DefaultEnterpriseMapServiceContext extends MapServiceContextImpl implements EnterpriseMapServiceContext {

    public DefaultEnterpriseMapServiceContext(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    EnterpriseMapEventPublisher createMapEventPublisherSupport() {
        return new EnterpriseMapEventPublisher(this);
    }
}
