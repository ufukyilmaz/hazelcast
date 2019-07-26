package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.CustomWanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationPublisher;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService.getPublisherIdOrGroupName;
import static com.hazelcast.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.MapUtil.createConcurrentHashMap;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Container responsible for handling the lifecycle of the
 * {@link WanReplicationPublisher}s defined by the configuration.
 */
class WanPublisherContainer {
    /**
     * Publisher delegates grouped by WAN replication config name.
     */
    private final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = new ConcurrentHashMap<>(2);
    /** Mutex for creating new {@link WanReplicationPublisher} instances */
    private final Object publisherInitializationMutex = new Object();
    private final Node node;
    private final ConstructorFunction<String, WanReplicationPublisherDelegate> publisherDelegateConstructor =
            new ConstructorFunction<String, WanReplicationPublisherDelegate>() {
                @Override
                public WanReplicationPublisherDelegate createNew(String name) {
                    WanReplicationConfig replicationConfig = node.getConfig().getWanReplicationConfig(name);
                    return new WanReplicationPublisherDelegate(name, createPublishers(replicationConfig));
                }
            };

    WanPublisherContainer(Node node) {
        this.node = node;
    }

    /**
     * Instantiate and initialize the {@link WanReplicationEndpoint}s and group by WAN publisher name.
     *
     * @throws InvalidConfigurationException if the method was unable to create an publisher endpoint because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    private ConcurrentMap<String, WanReplicationEndpoint> createPublishers(WanReplicationConfig wanConfig) {
        List<WanBatchReplicationPublisherConfig> batchPublisherConfigs = wanConfig.getBatchPublisherConfigs();
        List<CustomWanPublisherConfig> customPublisherConfigs = wanConfig.getCustomPublisherConfigs();
        int publisherCount = batchPublisherConfigs.size() + customPublisherConfigs.size();

        if (publisherCount == 0) {
            return createConcurrentHashMap(1);
        }

        ConcurrentMap<String, WanReplicationEndpoint> endpoints = createConcurrentHashMap(publisherCount);
        Map<String, AbstractWanPublisherConfig> endpointConfigs = createHashMap(publisherCount);

        Stream.of(batchPublisherConfigs, customPublisherConfigs)
              .flatMap(Collection::stream)
              .forEach(publisherConfig -> {
                  String publisherId = getPublisherIdOrGroupName(publisherConfig);
                  if (endpoints.containsKey(publisherId)) {
                      throw new InvalidConfigurationException(
                              "Detected duplicate publisher ID '" + publisherId + "' for a single WAN replication config");
                  }

                  WanReplicationEndpoint endpoint = createPublisherEndpoint(publisherConfig);
                  endpoints.put(publisherId, endpoint);
                  endpointConfigs.put(publisherId, publisherConfig);
              });

        for (Entry<String, WanReplicationEndpoint> endpointEntry : endpoints.entrySet()) {
            String publisherId = endpointEntry.getKey();
            WanReplicationEndpoint endpoint = endpointEntry.getValue();
            endpoint.init(node, wanConfig, endpointConfigs.get(publisherId));
        }

        return endpoints;
    }

    /**
     * Instantiates a {@link WanReplicationEndpoint} from the provided publisher
     * configuration.
     *
     * @param publisherConfig the WAN publisher configuration
     * @return the WAN replication endpoint
     * @throws InvalidConfigurationException if the method was unable to create the endpoint because there was no
     *                                       implementation or class name defined on the config
     */
    private WanReplicationEndpoint createPublisherEndpoint(AbstractWanPublisherConfig publisherConfig) {
        WanReplicationEndpoint endpoint = getOrCreate(
                (WanReplicationEndpoint) publisherConfig.getImplementation(),
                node.getConfigClassLoader(),
                publisherConfig.getClassName());
        if (endpoint == null) {
            throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                    + "attribute need to be set in the WAN publisher configuration for publisher " + publisherConfig);
        }
        return endpoint;
    }

    /**
     * Returns the {@link WanReplicationPublisher} for the given {@code name}
     * or {@code null} if there is none and there is no configuration for it.
     * <p>
     * If there is no publisher but there is configuration, it will try to create
     * the publisher.
     *
     * @param name the name of the publisher
     * @return the WAN publisher or {@code null} if there is no configuration for the
     * publisher
     * @throws InvalidConfigurationException if the method was unable to create an publisher endpoint because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        if (!wanReplications.containsKey(name) && node.getConfig().getWanReplicationConfig(name) == null) {
            return null;
        }
        return getOrPutSynchronized(wanReplications, name, publisherInitializationMutex, publisherDelegateConstructor);
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

    /**
     * Constructs and initializes any publishers defined in the WAN replication
     * under the provided {@code wanReplicationName} but not yet initialized.
     *
     * @param wanReplicationName the WAN replication
     * @throws InvalidConfigurationException if the method was unable to create an publisher endpoint because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    public void ensurePublishersInitialized(String wanReplicationName) {
        WanReplicationPublisherDelegate existingPublishers =
                (WanReplicationPublisherDelegate) getWanReplicationPublisher(wanReplicationName);
        WanReplicationConfig wanConfig = node.getConfig()
                                             .getWanReplicationConfig(wanReplicationName);

        Map<String, AbstractWanPublisherConfig> newConfigMap = createHashMap(1);
        Stream.of(wanConfig.getBatchPublisherConfigs(), wanConfig.getCustomPublisherConfigs())
              .flatMap(Collection::stream)
              .forEach(publisherConfig -> {
                  String publisherId = getPublisherIdOrGroupName(publisherConfig);
                  if (existingPublishers.getEndpoint(publisherId) == null) {
                      if (newConfigMap.put(publisherId, publisherConfig) != null) {
                          throw new InvalidConfigurationException(
                                  "Detected duplicate publisher ID '" + publisherId + "' for a single WAN replication config");
                      }
                  }
              });
        if (newConfigMap.isEmpty()) {
            // fast path, no uninitialized publishers
            return;
        }

        synchronized (publisherInitializationMutex) {
            // first construct publisher endpoints
            Map<String, WanReplicationEndpoint> newEndpoints = createHashMap(newConfigMap.size());
            for (Entry<String, AbstractWanPublisherConfig> newPublisherEntry : newConfigMap.entrySet()) {
                String publisherId = newPublisherEntry.getKey();
                AbstractWanPublisherConfig publisherConfig = newPublisherEntry.getValue();

                if (existingPublishers.getEndpoint(publisherId) == null) {
                    newEndpoints.put(publisherId, createPublisherEndpoint(publisherConfig));
                }
            }

            // then initialize them
            for (Entry<String, WanReplicationEndpoint> newEndpointEntry : newEndpoints.entrySet()) {
                String publisherId = newEndpointEntry.getKey();
                WanReplicationEndpoint endpoint = newEndpointEntry.getValue();
                endpoint.init(node, wanConfig, newConfigMap.get(publisherId));
                existingPublishers.addEndpoint(publisherId, endpoint);
            }
        }
    }
}
