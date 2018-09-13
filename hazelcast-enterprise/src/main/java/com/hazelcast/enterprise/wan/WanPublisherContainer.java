package com.hazelcast.enterprise.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Container responsible for handling the lifecycle of the
 * {@link WanReplicationPublisher}s defined by the configuration.
 */
class WanPublisherContainer {
    /**
     * Publisher delegates grouped by WAN replication config name.
     */
    private final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications
            = new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    /** Mutex for creating new {@link WanReplicationPublisher} instances */
    private final Object publisherMutex = new Object();
    private final Node node;
    private final ConstructorFunction<String, WanReplicationPublisherDelegate> publisherDelegateConstructor =
            new ConstructorFunction<String, WanReplicationPublisherDelegate>() {
                @Override
                public WanReplicationPublisherDelegate createNew(String name) {
                    final WanReplicationConfig replicationConfig = node.getConfig().getWanReplicationConfig(name);
                    final List<WanPublisherConfig> publisherConfigs = replicationConfig.getWanPublisherConfigs();
                    return new WanReplicationPublisherDelegate(name, createPublishers(replicationConfig, publisherConfigs));
                }
            };

    WanPublisherContainer(Node node) {
        this.node = node;
    }

    /**
     * Instantiate and initialize the {@link WanReplicationEndpoint}s and
     * group by WAN publisher name.
     */
    private Map<String, WanReplicationEndpoint> createPublishers(
            WanReplicationConfig wanReplicationConfig,
            List<WanPublisherConfig> publisherConfigs) {
        final Map<String, WanReplicationEndpoint> targetEndpoints = new HashMap<String, WanReplicationEndpoint>();
        if (publisherConfigs.isEmpty()) {
            return targetEndpoints;
        }

        for (WanPublisherConfig publisherConfig : publisherConfigs) {
            final WanReplicationEndpoint endpoint = getOrCreate(
                    (WanReplicationEndpoint) publisherConfig.getImplementation(),
                    node.getConfigClassLoader(),
                    publisherConfig.getClassName());
            if (endpoint == null) {
                throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                        + "attribute need to be set in WanPublisherConfig");
            }
            final String publisherName = publisherConfig.getGroupName();
            if (targetEndpoints.containsKey(publisherName)) {
                throw new InvalidConfigurationException(
                        "Detected duplicate group-name '" + publisherName + "' for a single WAN replication config");
            }

            endpoint.init(node, wanReplicationConfig, publisherConfig);
            targetEndpoints.put(publisherName, endpoint);
        }
        return targetEndpoints;
    }

    /**
     * Returns the {@link WanReplicationPublisher} for the given {@code name}
     * or {@code null} if there is none and there is no configuration for it.
     * <p>
     * If there is no publisher but there is configuration, it will try to create
     * the publisher.
     *
     * @param name the name of the publisher
     * @return the WAN publisher or {@code null} if there is no configuration
     * for the publisher
     */
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        if (!wanReplications.containsKey(name) && node.getConfig().getWanReplicationConfig(name) == null) {
            return null;
        }
        return getOrPutSynchronized(wanReplications, name, publisherMutex, publisherDelegateConstructor);
    }

    /**
     * Returns a map of publisher delegates grouped by WAN replication config
     * name
     */
    ConcurrentHashMap<String, WanReplicationPublisherDelegate> getWanReplications() {
        return wanReplications;
    }

    /**
     * Shuts down all {@link WanReplicationEndpoint}s and clears the endpoint
     * map
     */
    public void shutdown() {
        for (WanReplicationPublisherDelegate publisher : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : publisher.getEndpoints()) {
                if (endpoint != null) {
                    endpoint.shutdown();
                }
            }
        }
        wanReplications.clear();
    }
}
