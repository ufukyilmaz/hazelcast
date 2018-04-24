package com.hazelcast.enterprise.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.nio.ClassLoaderUtil.getOrCreate;

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

                    if (consumer == null) {
                        throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                                + "attribute need to be set in WanConsumerConfig");
                    }
                    consumer.init(node, wanReplicationConfigEntry.getKey(), consumerConfig);
                    wanConsumers.put(wanReplicationConfigEntry.getKey(), consumer);
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
