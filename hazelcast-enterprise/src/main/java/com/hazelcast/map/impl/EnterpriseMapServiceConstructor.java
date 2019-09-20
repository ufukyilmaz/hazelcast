package com.hazelcast.map.impl;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.util.ConstructorFunction;

/**
 * Helper which is used to create a {@link MapService} object.
 */
public final class EnterpriseMapServiceConstructor {

    private static final ConstructorFunction<NodeEngine, MapService> ENTERPRISE_MAP_SERVICE_CONSTRUCTOR
            = nodeEngine -> {
                EnterpriseMapServiceContext enterpriseMapServiceContext = new EnterpriseMapServiceContextImpl(nodeEngine);
                MapServiceFactory factory
                        = new EnterpriseMapServiceFactory(nodeEngine, enterpriseMapServiceContext);
                return factory.createMapService();
            };

    private EnterpriseMapServiceConstructor() {
    }

    public static ConstructorFunction<NodeEngine, MapService> getEnterpriseMapServiceConstructor() {
        return ENTERPRISE_MAP_SERVICE_CONSTRUCTOR;
    }
}
