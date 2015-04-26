package com.hazelcast.map.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;

/**
 * Helper which is used to create a {@link MapService} object.
 */
public final class EnterpriseMapServiceConstructor {

    private static final ConstructorFunction<NodeEngine, MapService> ENTERPRISE_MAP_SERVICE_CONSTRUCTOR
            = new ConstructorFunction<NodeEngine, MapService>() {
        @Override
        public MapService createNew(NodeEngine nodeEngine) {
            EnterpriseMapServiceContext enterpriseMapServiceContext = new DefaultEnterpriseMapServiceContext(nodeEngine);
            MapServiceFactory factory
                    = new EnterpriseMapServiceFactory(enterpriseMapServiceContext);
            return factory.createMapService();
        }
    };

    private EnterpriseMapServiceConstructor() {
    }

    public static ConstructorFunction<NodeEngine, MapService> getEnterpriseMapServiceConstructor() {
        return ENTERPRISE_MAP_SERVICE_CONSTRUCTOR;
    }
}
