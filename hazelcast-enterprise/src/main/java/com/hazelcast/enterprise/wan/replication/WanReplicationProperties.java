package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.InvalidConfigurationException;
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
     * Deprecated: this property is not used anymore.
     *
     * @see #MAX_CONCURRENT_INVOCATIONS
     */
    @Deprecated
    public static final PropertyDefinition EXECUTOR_THREAD_COUNT = property("executorThreadCount", PropertyTypeConverter.INTEGER);

    /**
     * Maximum number of WAN event batches being sent to the target cluster
     * concurrently.
     * <p>
     * Setting this property to anything less than {@code 2} will only allow a
     * single batch of events to be sent to each target endpoint and will
     * maintain causality of events for a single partition.
     * <p>
     * Setting this property to {@code 2} or higher will allow multiple batches
     * of WAN events to be sent to each target endpoint. Since this allows
     * reordering or batches due to network conditions, causality and ordering
     * of events for a single partition is lost and batches for a single
     * partition are now sent randomly to any available target endpoint.
     * This, however, does present faster WAN replication for certain scenarios
     * such as replicating immutable, independent map entries which are only
     * added once and where ordering of when these entries are added is not
     * necessary.
     * Keep in mind that if you set this property to a value which is less than
     * the target endpoint count, you will lose performance as not all target
     * endpoints will be used at any point in time to process WAN event batches.
     * So, for instance, if you have a target cluster with 3 members (target
     * endpoints) and you want to use this property, it makes sense to set it
     * to a value higher than {@code 3}. Otherwise, you can simply disable it
     * by setting it to less than {@code 2} in which case WAN will use the
     * default replication strategy and adapt to the target endpoint count
     * while maintaining causality.
     */
    public static final PropertyDefinition MAX_CONCURRENT_INVOCATIONS
            = property("max.concurrent.invocations", PropertyTypeConverter.INTEGER);

    /**
     * Minimum duration in nanoseconds that the WAN replication thread will be
     * parked if there are no events to replicate.
     * The default value is {@link WanConfigurationContext#DEFAULT_IDLE_MIN_PARK_NS}.
     */
    public static final PropertyDefinition IDLE_MIN_PARK_NS
            = property("replication.idle.minParkNs", PropertyTypeConverter.LONG);

    /**
     * Maximum duration in nanoseconds that the WAN replication thread will be
     * parked if there are no events to replicate.
     * The default value is {@link WanConfigurationContext#DEFAULT_IDLE_MAX_PARK_NS}.
     */
    public static final PropertyDefinition IDLE_MAX_PARK_NS
            = property("replication.idle.maxParkNs", PropertyTypeConverter.LONG);

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
