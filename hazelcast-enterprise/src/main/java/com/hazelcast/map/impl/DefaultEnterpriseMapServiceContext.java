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
    MapEventPublisherSupport createMapEventPublisherSupport() {
        return new EnterpriseMapEventPublisherSupport(this);
    }
}
