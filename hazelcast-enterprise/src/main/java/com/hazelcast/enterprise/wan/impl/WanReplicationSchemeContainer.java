package com.hazelcast.enterprise.wan.impl;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.CustomWanPublisherConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.impl.DelegatingWanReplicationScheme;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.hazelcast.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.MapUtil.createConcurrentHashMap;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.wan.impl.WanReplicationServiceImpl.getWanPublisherId;

/**
 * Container responsible for handling the lifecycle of the
 * WAN replication scheme implementations defined by the configuration.
 *
 * @see DelegatingWanReplicationScheme
 */
class WanReplicationSchemeContainer {
    /**
     * Publisher delegates grouped by WAN replication config name.
     */
    private final ConcurrentHashMap<String, DelegatingWanReplicationScheme> wanReplications = new ConcurrentHashMap<>(2);
    /** Mutex for creating new {@link WanReplicationPublisher} instances */
    private final Object publisherInitializationMutex = new Object();
    private final Node node;
    private final ConstructorFunction<String, DelegatingWanReplicationScheme> publisherDelegateConstructor =
            new ConstructorFunction<String, DelegatingWanReplicationScheme>() {
                @Override
                public DelegatingWanReplicationScheme createNew(String name) {
                    WanReplicationConfig replicationConfig = node.getConfig().getWanReplicationConfig(name);
                    return new DelegatingWanReplicationScheme(name, createPublishers(replicationConfig));
                }
            };

    WanReplicationSchemeContainer(Node node) {
        this.node = node;
    }

    /**
     * Instantiate and initialize the {@link WanReplicationPublisher}s and group by WAN publisher name.
     *
     * @throws InvalidConfigurationException if the method was unable to create an publisher because there
     *                                       was no implementation or class name defined on the config or if there
     *                                       were multiple publisher configurations with the same publisher ID.
     */
    private ConcurrentMap<String, WanReplicationPublisher> createPublishers(WanReplicationConfig wanConfig) {
        List<WanBatchReplicationPublisherConfig> batchPublisherConfigs = wanConfig.getBatchPublisherConfigs();
        List<CustomWanPublisherConfig> customPublisherConfigs = wanConfig.getCustomPublisherConfigs();
        int publisherCount = batchPublisherConfigs.size() + customPublisherConfigs.size();

        if (publisherCount == 0) {
            return createConcurrentHashMap(1);
        }

        ConcurrentMap<String, WanReplicationPublisher> publishers = createConcurrentHashMap(publisherCount);
        Map<String, AbstractWanPublisherConfig> publisherConfigs = createHashMap(publisherCount);

        Stream.of(batchPublisherConfigs, customPublisherConfigs)
              .flatMap(Collection::stream)
              .forEach(publisherConfig -> {
                  String publisherId = getWanPublisherId(publisherConfig);
                  if (publishers.containsKey(publisherId)) {
                      throw new InvalidConfigurationException(
                              "Detected duplicate publisher ID '" + publisherId + "' for a single WAN replication config");
                  }

                  WanReplicationPublisher publisher = createPublisher(publisherConfig);
                  publishers.put(publisherId, publisher);
                  publisherConfigs.put(publisherId, publisherConfig);
              });

        for (Entry<String, WanReplicationPublisher> publisherEntry : publishers.entrySet()) {
            String publisherId = publisherEntry.getKey();
            WanReplicationPublisher publisher = publisherEntry.getValue();
            node.getSerializationService().getManagedContext().initialize(publisher);
            publisher.init(wanConfig, publisherConfigs.get(publisherId));
        }

        return publishers;
    }

    /**
     * Instantiates a {@link WanReplicationPublisher} from the provided publisher
     * configuration.
     *
     * @param publisherConfig the WAN publisher configuration
     * @return the WAN replication publisher
     * @throws InvalidConfigurationException if the method was unable to create the publisher because there was no
     *                                       implementation or class name defined on the config
     */
    private WanReplicationPublisher createPublisher(AbstractWanPublisherConfig publisherConfig) {
        WanReplicationPublisher publisher = getOrCreate(
                (WanReplicationPublisher) publisherConfig.getImplementation(),
                node.getConfigClassLoader(),
                publisherConfig.getClassName());
        if (publisher == null) {
            throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                    + "attribute need to be set in the WAN publisher configuration for publisher " + publisherConfig);
        }
        return publisher;
    }

    /**
     * Returns the {@link WanReplicationPublisher} for the given {@code name}
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
    public DelegatingWanReplicationScheme getWanReplicationPublishers(String wanReplicationScheme) {
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
    ConcurrentHashMap<String, DelegatingWanReplicationScheme> getWanReplications() {
        return wanReplications;
    }

    /**
     * Shuts down all {@link WanReplicationPublisher}s and clears the publisher
     * map
     */
    public void shutdown() {
        for (DelegatingWanReplicationScheme publisherDelegate : wanReplications.values()) {
            for (WanReplicationPublisher publisher : publisherDelegate.getPublishers()) {
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
        DelegatingWanReplicationScheme existingPublishers = getWanReplicationPublishers(wanReplicationName);
        WanReplicationConfig wanConfig = node.getConfig()
                                             .getWanReplicationConfig(wanReplicationName);

        Map<String, AbstractWanPublisherConfig> newConfigMap = createHashMap(1);
        Stream.of(wanConfig.getBatchPublisherConfigs(), wanConfig.getCustomPublisherConfigs())
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
            Map<String, WanReplicationPublisher> newPublishers = createHashMap(newConfigMap.size());
            for (Entry<String, AbstractWanPublisherConfig> newPublisherEntry : newConfigMap.entrySet()) {
                String publisherId = newPublisherEntry.getKey();
                AbstractWanPublisherConfig publisherConfig = newPublisherEntry.getValue();

                if (existingPublishers.getPublisher(publisherId) == null) {
                    newPublishers.put(publisherId, createPublisher(publisherConfig));
                }
            }

            // then initialize them
            for (Entry<String, WanReplicationPublisher> newPublisherEntry : newPublishers.entrySet()) {
                String publisherId = newPublisherEntry.getKey();
                WanReplicationPublisher publisher = newPublisherEntry.getValue();
                node.getSerializationService().getManagedContext().initialize(publisher);
                publisher.init(wanConfig, newConfigMap.get(publisherId));
                existingPublishers.addPublisher(publisherId, publisher);
            }
        }
    }
}
