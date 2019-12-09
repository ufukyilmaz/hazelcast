package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.PropertyTypeConverter;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.config.properties.ValueValidator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;

/**
 * Property definitions for {@link WanBatchPublisher} implementation.
 */
public final class WanBatchPublisherProperties {

    private WanBatchPublisherProperties() {
    }

    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD",
            justification = "This class may still be used in the future")
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
                throw new InvalidConfigurationException(String.format("Config %s is needed in CustomWanPublisherConfig",
                        propertyDefinition.key()));
            }
            return defaultValue;
        }
        return (T) propertyDefinition.typeConverter().convert(value.toString());
    }
}
