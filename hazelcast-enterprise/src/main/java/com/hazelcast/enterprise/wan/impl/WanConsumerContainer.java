package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.WanReplicationConsumer;
import com.hazelcast.instance.impl.Node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.nio.ClassLoaderUtil.getOrCreate;

/**
 * Container responsible for handling the lifecycle of the
 * {@link WanReplicationConsumer}s defined by the configuration.
 */
public class WanConsumerContainer {
    private final Node node;
    /**
     * Consumer implementations grouped by WAN replication config name.
     */
    private final Map<String, WanReplicationConsumer> wanConsumers = new ConcurrentHashMap<String, WanReplicationConsumer>(2);

    public WanConsumerContainer(Node node) {
        this.node = node;
    }

    /**
     * Construct and initialize all WAN consumers by fetching the class names
     * or implementations from the config and store them under the WAN
     * replication config name in {@link #wanConsumers}.
     */
    public void initializeCustomConsumers() {
        final Map<String, WanReplicationConfig> configs = node.getConfig().getWanReplicationConfigs();
        if (configs != null) {
            for (Map.Entry<String, WanReplicationConfig> wanReplicationConfigEntry : configs.entrySet()) {
                final WanConsumerConfig consumerConfig = wanReplicationConfigEntry.getValue().getWanConsumerConfig();
                if (consumerConfig != null) {
                    final WanReplicationConsumer consumer = getOrCreate(
                            (WanReplicationConsumer) consumerConfig.getImplementation(),
                            node.getConfigClassLoader(),
                            consumerConfig.getClassName());
                    if (consumer != null) {
                        consumer.init(node, wanReplicationConfigEntry.getKey(), consumerConfig);
                        wanConsumers.put(wanReplicationConfigEntry.getKey(), consumer);
                    }
                }
            }
        }
    }

    public void shutdown() {
        for (WanReplicationConsumer consumer : wanConsumers.values()) {
            consumer.shutdown();
        }
        wanConsumers.clear();
    }
}
