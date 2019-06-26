package com.hazelcast.enterprise.wan.impl.discovery;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.enterprise.wan.impl.replication.WanReplicationProperties;

import static com.hazelcast.config.properties.PropertyTypeConverter.INTEGER;

/**
 * Configuration properties for the Hazelcast Discovery Plugin with statically defined addresses. For more information
 * see {@link StaticDiscoveryStrategy}
 */
public final class StaticDiscoveryProperties {

    /**
     * Default port for returned address if the port is not defined in the endpoint list
     */
    public static final PropertyDefinition PORT = property("port", INTEGER);

    /**
     * Comma separated list of addresses to be returned as discovered nodes. May contain only IP addresses or may
     * also contain ports for each address. Addresses without defined ports will use the value provided by the
     * {@code port} property.
     */
    public static final PropertyDefinition ENDPOINTS = WanReplicationProperties.ENDPOINTS;

    private StaticDiscoveryProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return new SimplePropertyDefinition(key, false, typeConverter, null);
    }
}
