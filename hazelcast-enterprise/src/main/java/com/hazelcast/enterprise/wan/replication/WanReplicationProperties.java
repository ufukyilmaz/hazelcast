package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;
import com.hazelcast.enterprise.wan.connection.WanConnectionManager;
import com.hazelcast.spi.discovery.DiscoveryNode;

import java.util.Map;

/**
 * Property definitions for {@link WanBatchReplication} implementation.
 */
public final class WanReplicationProperties {

    /**
     * Defines the maximum batch size that can be sent to target cluster.
     * Valid when used with {@link WanBatchReplication} implementation.
     */
    public static final PropertyDefinition BATCH_SIZE
            = property("batch.size", PropertyTypeConverter.INTEGER);

    /**
     * Defines the maximum amount of time to be waited before sending a batch of
     * events to target cluster, if {@link #BATCH_SIZE} of events have not arrived
     * within this duration.
     */
    public static final PropertyDefinition BATCH_MAX_DELAY_MILLIS
            = property("batch.max.delay.millis", PropertyTypeConverter.LONG);

    /**
     * This property is only valid when used with {@link WanBatchReplication}
     * implementation. When enabled, only the latest
     * {@link com.hazelcast.wan.WanReplicationEvent} of a key is sent to target.
     */
    public static final PropertyDefinition SNAPSHOT_ENABLED
            = property("snapshot.enabled", PropertyTypeConverter.BOOLEAN);

    /**
     * Duration in milliseconds to define waiting time before retrying to
     * send the events to target cluster again in case of acknowledgement
     * is not arrived.
     */
    public static final PropertyDefinition RESPONSE_TIMEOUT_MILLIS
            = property("response.timeout.millis", PropertyTypeConverter.LONG);

    /**
     * Determines acknowledgement waiting type of WAN replication operation
     * invocation.
     *
     * @see com.hazelcast.config.WanAcknowledgeType for valid values.
     */
    public static final PropertyDefinition ACK_TYPE
            = property("ack.type", PropertyTypeConverter.STRING);

    /**
     * Group name of target cluster.
     * If the group name is defined using this property, it takes precedence
     * over the value returned by the {@link WanPublisherConfig#getGroupName()}.
     */
    public static final PropertyDefinition GROUP_NAME = property("group.name", PropertyTypeConverter.STRING);

    /**
     * Group password of target cluster.
     */
    public static final PropertyDefinition GROUP_PASSWORD = property("group.password", PropertyTypeConverter.STRING);

    /**
     * Comma separated list of target cluster members,
     * e.g. {@code 127.0.0.1:5701, 127.0.0.1:5702}.
     */
    public static final PropertyDefinition ENDPOINTS = property("endpoints", PropertyTypeConverter.STRING);

    /**
     * Period in seconds in which WAN tries to discover new endpoints
     * and reestablish connections to failed endpoints.
     * The default is 10 (seconds).
     */
    public static final PropertyDefinition DISCOVERY_PERIOD = property("discovery.period", PropertyTypeConverter.INTEGER);

    /**
     * The maximum number of endpoints that WAN will connect to when
     * using a discovery mechanism to define endpoints.
     * Default is {@link WanConfigurationContext#DEFAULT_MAX_ENDPOINTS}.
     * This property has no effect when static endpoint IPs are defined
     * using the {@link #ENDPOINTS} property.
     */
    public static final PropertyDefinition MAX_ENDPOINTS = property("maxEndpoints", PropertyTypeConverter.INTEGER);

    /**
     * The number of threads that the {@link WanBatchReplication} executor will have.
     * The executor is used to send WAN events to the endpoints and ideally you want
     * to have one thread per endpoint. If this property is omitted and you have
     * specified the {@link #ENDPOINTS} property, this will be the case.
     * If, on the other hand, you are using WAN with the discovery SPI and you have
     * not specified this property, the executor will be sized to the initial number
     * of discovered endpoints. This can lead to performance issues if the number of
     * endpoints changes in the future - either contention on a too small number of
     * threads or wasted threads that will not be performing any work.
     */
    public static final PropertyDefinition EXECUTOR_THREAD_COUNT = property("executorThreadCount", PropertyTypeConverter.INTEGER);


    /**
     * Determines whether the WAN connection manager should connect to the
     * endpoint on the private address returned by the discovery SPI.
     * By default this property is {@code false} which means the WAN connection
     * manager will always use the public address.
     *
     * @see WanConnectionManager#discoverEndpointAddresses()
     * @see DiscoveryNode#getPublicAddress()
     * @see DiscoveryNode#getPrivateAddress()
     */
    public static final PropertyDefinition DISCOVERY_USE_ENDPOINT_PRIVATE_ADDRESS
            = property("discovery.useEndpointPrivateAddress", PropertyTypeConverter.BOOLEAN);

    private WanReplicationProperties() {
    }

    private static PropertyDefinition property(String key, PropertyTypeConverter typeConverter) {
        return property(key, true, typeConverter);
    }

    private static PropertyDefinition property(String key, boolean optional, PropertyTypeConverter typeConverter) {
        return property(key, optional, typeConverter, null);
    }

    private static PropertyDefinition property(String key, boolean optional, PropertyTypeConverter typeConverter,
                                               ValueValidator valueValidator) {
        return new SimplePropertyDefinition(key, optional, typeConverter, valueValidator);
    }

    public static <T extends Comparable> T getProperty(PropertyDefinition propertyDefinition,
                                                       Map<String, Comparable> propertyMap, T defaultValue) {
        Comparable value = propertyMap.get(propertyDefinition.key());
        if (value == null) {
            if (!propertyDefinition.optional()) {
                throw new InvalidConfigurationException(String.format("Config %s is needed in WanPublisherConfig",
                        propertyDefinition.key()));
            }
            return defaultValue;
        }
        return (T) propertyDefinition.typeConverter().convert(value.toString());
    }
}
