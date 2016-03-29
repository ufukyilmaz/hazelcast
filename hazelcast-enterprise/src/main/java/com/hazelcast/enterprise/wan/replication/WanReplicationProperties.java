package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;

import java.util.Map;

/**
 * Property definitions for {@link WanNoDelayReplication} and {@link WanBatchReplication} implementations
 */
public final class WanReplicationProperties {

    /**
     * Property to define maximum batch size that can be sent to target cluster,
     * valid when used with {@link WanBatchReplication} implementation
     */
    public static final PropertyDefinition BATCH_SIZE
            = property("batch.size", PropertyTypeConverter.STRING);

    /**
     * Property to define maximum amount of time to be waited before sending a batch of events to target cluster
     * if {@link #BATCH_SIZE} of events are not arrived within this duration
     */
    public static final PropertyDefinition BATCH_MAX_DELAY_MILLIS
            = property("batch.max.delay.millis", PropertyTypeConverter.STRING);

    /**
     * This property is only valid when used with {@link WanBatchReplication} implementation
     * When enabled, only the latest {@link com.hazelcast.wan.WanReplicationEvent} of a key is sent to target
     */
    public static final PropertyDefinition SNAPSHOT_ENABLED
            = property("snapshot.enabled", PropertyTypeConverter.BOOLEAN);

    /**
     * Duration in milliseconds to define waiting time before retrying to send the events to target cluster again
     * in case of acknowledge is not arrived.
     */
    public static final PropertyDefinition RESPONSE_TIMEOUT_MILLIS
            = property("response.timeout.millis", PropertyTypeConverter.STRING);

    /**
     * Determines acknowledge waiting type of wan replication operation invocation.
     * @see com.hazelcast.config.WanAcknowledgeType for valid values.
     */
    public static final PropertyDefinition ACK_TYPE
            = property("ack.type", PropertyTypeConverter.STRING);

    /**
     * Group password of target cluster
     */
    public static final PropertyDefinition GROUP_PASSWORD
            = property("group.password", false, PropertyTypeConverter.STRING);

    /**
     * Comma seperated list of target cluster members.
     * ie. 127.0.0.1:5701, 127.0.0.1:5702
     */
    public static final PropertyDefinition ENDPOINTS
            = property("endpoints", false, PropertyTypeConverter.STRING);

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
        return (T) propertyDefinition.typeConverter().convert(value);
    }
}
