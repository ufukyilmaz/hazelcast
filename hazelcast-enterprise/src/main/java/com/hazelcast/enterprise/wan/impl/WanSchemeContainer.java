package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.DelegatingWanScheme;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.hazelcast.internal.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.wan.impl.WanReplicationServiceImpl.getWanPublisherId;

/**
 * Container responsible for handling the lifecycle of the
 * WAN replication scheme implementations defined by the configuration.
 *
 * @see DelegatingWanScheme
 */
class WanSchemeContainer {
    /**
     * Publisher delegates grouped by WAN replication config name.
     */
    private final ConcurrentHashMap<String, DelegatingWanScheme> wanReplications = new ConcurrentHashMap<>(2);
    /** Mutex for creating new {@link WanPublisher} instances */
    private final Object publisherInitializationMutex = new Object();
    private final Node node;
    private final ConstructorFunction<String, DelegatingWanScheme> publisherDelegateConstructor =
            new ConstructorFunction<String, DelegatingWanScheme>() {
                @Override
                public DelegatingWanScheme createNew(String name) {
                    WanReplicationConfig replicationConfig = node.getConfig().getWanReplicationConfig(name);
                    return new DelegatingWanScheme(name, createPublishers(replicationConfig));
                }
            };

    WanSchemeContainer(Node node) {
        this.node = node;
    }

    /**
     * Instantiate and initialize the {@link WanPublisher}s and group by WAN publisher name.
     *
     * @throws InvalidConfigurationException if the method was unable to create an publisher because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    private ConcurrentMap<String, WanPublisher> createPublishers(WanReplicationConfig wanReplicationConfig) {
        List<WanBatchPublisherConfig> batchPublisherConfigs = wanReplicationConfig.getBatchPublisherConfigs();
        List<WanCustomPublisherConfig> customPublisherConfigs = wanReplicationConfig.getCustomPublisherConfigs();
        int publisherCount = batchPublisherConfigs.size() + customPublisherConfigs.size();

        if (publisherCount == 0) {
            return createConcurrentHashMap(1);
        }

        ConcurrentMap<String, WanPublisher> publishers = createConcurrentHashMap(publisherCount);
        Map<String, AbstractWanPublisherConfig> publisherConfigs = createHashMap(publisherCount);

        Stream.of(batchPublisherConfigs, customPublisherConfigs)
              .flatMap(Collection::stream)
              .forEach(publisherConfig -> {
                  String publisherId = getWanPublisherId(publisherConfig);
                  if (publishers.containsKey(publisherId)) {
                      throw new InvalidConfigurationException(
                              "Detected duplicate publisher ID '" + publisherId + "' for a single WAN replication config");
                  }

                  WanPublisher publisher = createPublisher(publisherConfig);
                  publishers.put(publisherId, publisher);
                  publisherConfigs.put(publisherId, publisherConfig);
              });

        for (Entry<String, WanPublisher> publisherEntry : publishers.entrySet()) {
            String publisherId = publisherEntry.getKey();
            WanPublisher publisher = publisherEntry.getValue();
            node.getSerializationService().getManagedContext().initialize(publisher);
            publisher.init(wanReplicationConfig, publisherConfigs.get(publisherId));
        }

        return publishers;
    }

    /**
     * Instantiates a {@link WanPublisher} from the provided publisher
     * configuration.
     *
     * @param publisherConfig the WAN publisher configuration
     * @return the WAN replication publisher
     * @throws InvalidConfigurationException if the method was unable to create the publisher because there was no
     *                                       implementation or class name defined on the config
     */
    private WanPublisher createPublisher(AbstractWanPublisherConfig publisherConfig) {
        WanPublisher publisher = getOrCreate(
                publisherConfig.getImplementation(),
                node.getConfigClassLoader(),
                publisherConfig.getClassName());
        if (publisher == null) {
            throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                    + "attribute need to be set in the WAN publisher configuration for publisher " + publisherConfig);
        }
        return publisher;
    }

    /**
     * Returns the {@link WanPublisher} for the given {@code name}
     * or {@code null} if there is none and there is no configuration for it.
     * <p>
     * If there is no publisher but there is configuration, it will try to create
     * the publisher.
     *
     * @param wanReplicationScheme the name of the publisher
     * @return the WAN publisher or {@code null} if there is no configuration for the
     * publisher
     * @throws InvalidConfigurationException if the method was unable to create an publisher because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    public DelegatingWanScheme getWanReplicationPublishers(String wanReplicationScheme) {
        if (!wanReplications.containsKey(wanReplicationScheme)
                && node.getConfig().getWanReplicationConfig(wanReplicationScheme) == null) {
            return null;
        }
        return getOrPutSynchronized(wanReplications, wanReplicationScheme,
                publisherInitializationMutex, publisherDelegateConstructor);
    }

    /**
     * Returns a map of publisher delegates grouped by WAN replication config
     * name
     */
    ConcurrentHashMap<String, DelegatingWanScheme> getWanReplications() {
        return wanReplications;
    }

    /**
     * Shuts down all {@link WanPublisher}s and clears the publisher
     * map
     */
    public void shutdown() {
        for (DelegatingWanScheme publisherDelegate : wanReplications.values()) {
            for (WanPublisher publisher : publisherDelegate.getPublishers()) {
                if (publisher != null) {
                    publisher.shutdown();
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
     * @throws InvalidConfigurationException if the method was unable to create an publisher because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    public void ensurePublishersInitialized(String wanReplicationName) {
        DelegatingWanScheme existingPublishers = getWanReplicationPublishers(wanReplicationName);
        WanReplicationConfig wanReplicationConfig = node.getConfig()
                                                        .getWanReplicationConfig(wanReplicationName);

        Map<String, AbstractWanPublisherConfig> newConfigMap = createHashMap(1);
        Stream.of(wanReplicationConfig.getBatchPublisherConfigs(), wanReplicationConfig.getCustomPublisherConfigs())
              .flatMap(Collection::stream)
              .forEach(publisherConfig -> {
                  String publisherId = getWanPublisherId(publisherConfig);
                  if (existingPublishers.getPublisher(publisherId) == null) {
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
            // first construct publisher publishers
            Map<String, WanPublisher> newPublishers = createHashMap(newConfigMap.size());
            for (Entry<String, AbstractWanPublisherConfig> newPublisherEntry : newConfigMap.entrySet()) {
                String publisherId = newPublisherEntry.getKey();
                AbstractWanPublisherConfig publisherConfig = newPublisherEntry.getValue();

                if (existingPublishers.getPublisher(publisherId) == null) {
                    newPublishers.put(publisherId, createPublisher(publisherConfig));
                }
            }

            // then initialize them
            for (Entry<String, WanPublisher> newPublisherEntry : newPublishers.entrySet()) {
                String publisherId = newPublisherEntry.getKey();
                WanPublisher publisher = newPublisherEntry.getValue();
                node.getSerializationService().getManagedContext().initialize(publisher);
                publisher.init(wanReplicationConfig, newConfigMap.get(publisherId));
                existingPublishers.addPublisher(publisherId, publisher);
            }
        }
    }
}
