package com.hazelcast.enterprise.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MerkleTreeConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.operation.AddWanConfigOperationFactory;
import com.hazelcast.enterprise.wan.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.sync.WanAntiEntropyEvent;
import com.hazelcast.enterprise.wan.sync.WanConsistencyCheckEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.events.AddWanConfigIgnoredEvent;
import com.hazelcast.internal.management.events.Event;
import com.hazelcast.internal.management.events.WanConfigurationAddedEvent;
import com.hazelcast.internal.management.events.WanConfigurationExtendedEvent;
import com.hazelcast.internal.management.events.WanConsistencyCheckIgnoredEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.LocalWanPublisherStatsImpl;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.AddWanConfigResult;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;
import com.hazelcast.wan.impl.WanEventCounters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Enterprise implementation for WAN replication.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class EnterpriseWanReplicationService implements WanReplicationService, FragmentedMigrationAwareService,
                                                        PostJoinAwareService, LiveOperationsTracker, ManagedService {

    private static final int ADD_WAN_CONFIG_MAX_RETRIES = 10;
    private final Node node;
    private final ILogger logger;
    private final WanReplicationMigrationAwareService migrationAwareService;
    private final WanEventProcessor eventProcessor;
    private final WanPublisherContainer publisherContainer;
    private final WanConsumerContainer consumerContainer;
    private final WanSyncManager syncManager;
    /** Mutex for adding new WAN replication config */
    private final Object configUpdateMutex = new Object();

    /** WAN event counters for all services and only received events */
    private final WanEventCounters receivedWanEventCounters = new WanEventCounters();

    /** WAN event counters for all services and only sent events */
    private final WanEventCounters sentWanEventCounters = new WanEventCounters();

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
        this.migrationAwareService = new WanReplicationMigrationAwareService(this, node);
        this.eventProcessor = new WanEventProcessor(node);
        this.publisherContainer = new WanPublisherContainer(node);
        this.consumerContainer = new WanConsumerContainer(node);
        this.syncManager = new WanSyncManager(this, node);
    }

    /**
     * Returns a WAN replication configured under a WAN replication config with
     * the name {@code wanReplicationName} and with a WAN publisher ID of
     * {@code wanPublisherId} or {@code null} if there is no configuration for
     * the given parameters.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param wanPublisherId     the publisher ID
     * @return the WAN endpoint or {@code null} if there is no configuration for the
     * given parameters
     * @see WanReplicationConfig#getName
     * @see WanPublisherConfig#getGroupName()
     * @see WanPublisherConfig#getPublisherId()
     */
    public WanReplicationEndpoint getEndpointOrNull(String wanReplicationName,
                                                    String wanPublisherId) {
        final WanReplicationPublisherDelegate publisherDelegate
                = (WanReplicationPublisherDelegate) getWanReplicationPublisher(wanReplicationName);
        return publisherDelegate != null
                ? publisherDelegate.getEndpoint(wanPublisherId)
                : null;
    }

    /**
     * Returns a WAN replication configured under a WAN replication config with
     * the name {@code wanReplicationName} and with a WAN publisher ID of
     * {@code wanPublisherId} or throws a {@link InvalidConfigurationException}
     * if there is no configuration for the given parameters.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param wanPublisherId     the publisher ID
     * @return the WAN endpoint
     * @throws InvalidConfigurationException if there is no replication config
     *                                       with the name {@code wanReplicationName}
     *                                       and publisher ID {@code wanPublisherId}
     * @see WanReplicationConfig#getName
     * @see WanPublisherConfig#getGroupName()
     * @see WanPublisherConfig#getPublisherId()
     */
    public WanReplicationEndpoint getEndpointOrFail(String wanReplicationName,
                                                    String wanPublisherId) {
        WanReplicationEndpoint endpoint = getEndpointOrNull(wanReplicationName, wanPublisherId);
        if (endpoint == null) {
            throw new InvalidConfigurationException("WAN Replication Config doesn't exist with WAN configuration name "
                    + wanReplicationName + " and publisher ID " + wanPublisherId);
        }
        return endpoint;
    }


    /**
     * Processes the replication event sent from the source cluster.
     *
     * @param event        the event, can be of type
     *                     {@link WanReplicationEvent} or
     *                     {@link BatchWanReplicationEvent}
     * @param wanOperation the operation sent by the source cluster
     */
    public void handleEvent(IdentifiedDataSerializable event, WanOperation wanOperation) {
        if (event instanceof BatchWanReplicationEvent) {
            eventProcessor.handleRepEvent((BatchWanReplicationEvent) event, wanOperation);
        } else {
            eventProcessor.handleRepEvent((WanReplicationEvent) event, wanOperation);
        }
    }

    /**
     * Appends the provided {@link WanReplicationConfig} to the configuration of
     * this member. If there is no WAN replication config with the same name/scheme,
     * the provided config will be used. If there is an existing WAN replication
     * config with the same name, any publishers with publisher IDs that are
     * present in the provided {@code newConfig} but not present in the existing
     * config will be added (appended).
     * If the existing config contains all of the publishers from the provided
     * config, no change is done to the existing config.
     * This method is thread-safe and may be called concurrently.
     *
     * @param newConfig the WAN configuration to add
     * @see WanPublisherConfig#getPublisherId()
     */
    public void appendWanReplicationConfig(WanReplicationConfig newConfig) {
        String scheme = newConfig.getName();
        ConcurrentMap<String, WanReplicationConfig> wanConfigs =
                (ConcurrentMap<String, WanReplicationConfig>) node.getConfig().getWanReplicationConfigs();
        WanReplicationConfig existingConfig = wanConfigs.putIfAbsent(scheme, newConfig);

        if (existingConfig == null) {
            return;
        }

        Map<String, WanPublisherConfig> missingPublisherConfigs = getPublisherConfigMap(newConfig);
        for (WanPublisherConfig existingPublisherConfig : existingConfig.getWanPublisherConfigs()) {
            missingPublisherConfigs.remove(getPublisherIdOrGroupName(existingPublisherConfig));
        }
        if (missingPublisherConfigs.isEmpty()) {
            return;
        }

        synchronized (configUpdateMutex) {
            existingConfig = wanConfigs.get(scheme);
            for (WanPublisherConfig existingPublisherConfig : existingConfig.getWanPublisherConfigs()) {
                missingPublisherConfigs.remove(getPublisherIdOrGroupName(existingPublisherConfig));
            }
            if (missingPublisherConfigs.isEmpty()) {
                // no new publishers
                return;
            }

            // new publishers, create new config
            ArrayList<WanPublisherConfig> mergedPublisherConfigs = new ArrayList<WanPublisherConfig>(
                    existingConfig.getWanPublisherConfigs().size() + missingPublisherConfigs.size());
            mergedPublisherConfigs.addAll(existingConfig.getWanPublisherConfigs());
            mergedPublisherConfigs.addAll(missingPublisherConfigs.values());
            WanReplicationConfig mergedConfig = new WanReplicationConfig();
            mergedConfig.setWanConsumerConfig(existingConfig.getWanConsumerConfig());
            mergedConfig.setName(existingConfig.getName());
            mergedConfig.setWanPublisherConfigs(mergedPublisherConfigs);
            wanConfigs.put(scheme, mergedConfig);
        }
    }

    void emitManagementCenterEvent(Event event) {
        // management center service may be temporarily null
        // while node is joining the cluster
        if (node.getManagementCenterService() != null) {
            node.getManagementCenterService().log(event);
        }
    }

    private static Map<String, WanPublisherConfig> getPublisherConfigMap(WanReplicationConfig config) {
        List<WanPublisherConfig> publisherConfigs = config.getWanPublisherConfigs();
        Map<String, WanPublisherConfig> publisherConfigMap = MapUtil.createHashMap(publisherConfigs.size());
        for (WanPublisherConfig publisherConfig : publisherConfigs) {
            publisherConfigMap.put(getPublisherIdOrGroupName(publisherConfig), publisherConfig);
        }
        return publisherConfigMap;
    }

    /**
     * Publishes an anti-entropy event for the given {@code wanReplicationName}
     * and {@code wanPublisherId}.
     * This method does not wait for the event processing to complete.
     *
     * @param wanReplicationName the WAN replication config name
     * @param wanPublisherId     the publisher ID in the WAN replication config
     * @param event              the WAN anti-entropy event
     * @throws InvalidConfigurationException if there is no replication config
     *                                       with the {@code wanReplicationName}
     *                                       and {@code wanPublisherId}
     */
    public void publishAntiEntropyEvent(String wanReplicationName,
                                        String wanPublisherId,
                                        WanAntiEntropyEvent event) {
        WanReplicationEndpoint endpoint = getEndpointOrFail(wanReplicationName, wanPublisherId);
        if (event instanceof WanSyncEvent) {
            endpoint.publishSyncEvent((WanSyncEvent) event);
            return;
        }

        if (endpoint instanceof WanBatchReplication) {
            ((WanBatchReplication) endpoint).publishAntiEntropyEvent(event);
        } else {
            logger.info("WAN replication for the scheme " + wanReplicationName
                    + " with publisher ID " + wanPublisherId
                    + " does not support anti-entropy events.");
        }
    }

    /**
     * Returns the manager for WAN sync events.
     */
    public WanSyncManager getSyncManager() {
        return syncManager;
    }

    /**
     * Constructs and initializes all WAN consumers defined in the WAN
     * configuration
     *
     * @see com.hazelcast.enterprise.wan.WanReplicationConsumer
     */
    public void initializeCustomConsumers() {
        consumerContainer.initializeCustomConsumers();
    }

    /**
     * Returns a map of publisher delegates grouped by WAN replication config
     * name
     */
    ConcurrentHashMap<String, WanReplicationPublisherDelegate> getWanReplications() {
        return publisherContainer.getWanReplications();
    }

    // only for testing
    public void handleEvent(WanReplicationEvent event) {
        eventProcessor.handleEvent(event);
    }

    @Override
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        return publisherContainer.getWanReplicationPublisher(name);
    }

    /**
     * {@inheritDoc}
     * Returns map from WAN replication config name to {@link LocalWanStats}.
     */
    @Override
    public Map<String, LocalWanStats> getStats() {
        ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        Map<String, LocalWanStats> wanStats = MapUtil.createHashMap(wanReplications.size());

        // add stats from initialized WAN replications
        for (Map.Entry<String, WanReplicationPublisherDelegate> delegateEntry : wanReplications.entrySet()) {
            LocalWanStats localWanStats = new LocalWanStatsImpl();
            String wanReplicationConfigName = delegateEntry.getKey();
            WanReplicationPublisherDelegate delegate = delegateEntry.getValue();
            localWanStats.getLocalWanPublisherStats().putAll(delegate.getStats());
            wanStats.put(wanReplicationConfigName, localWanStats);
        }

        // add stats from uninitialized but configured WAN replications
        final Map<String, WanReplicationConfig> wanReplicationConfigs
                = node.getNodeEngine().getConfig().getWanReplicationConfigs();
        final LocalWanPublisherStatsImpl stoppedPublisherStats = new LocalWanPublisherStatsImpl();
        stoppedPublisherStats.setState(WanPublisherState.STOPPED);

        for (Entry<String, WanReplicationConfig> replicationEntry : wanReplicationConfigs.entrySet()) {
            String wanReplicationConfigName = replicationEntry.getKey();
            if (wanStats.containsKey(wanReplicationConfigName)) {
                continue;
            }
            LocalWanStatsImpl localWanStats = new LocalWanStatsImpl();

            for (WanPublisherConfig publisherConfig : replicationEntry.getValue().getWanPublisherConfigs()) {
                String publisherId = getPublisherIdOrGroupName(publisherConfig);
                localWanStats.getLocalWanPublisherStats().put(publisherId, stoppedPublisherStats);
            }
            wanStats.put(wanReplicationConfigName, localWanStats);
        }

        return wanStats;
    }

    /**
     * Returns the publisher ID for the given WAN publisher configuration which
     * is then used for identifying the WAN publisher in a WAN replication
     * scheme.
     * If the publisher ID is empty, returns the publisher group name.
     *
     * @param publisherConfig the WAN replication publisher configuration
     * @return the publisher ID or group name
     */
    public static String getPublisherIdOrGroupName(WanPublisherConfig publisherConfig) {
        return !isNullOrEmptyAfterTrim(publisherConfig.getPublisherId())
                ? publisherConfig.getPublisherId()
                : publisherConfig.getGroupName();
    }

    @Override
    public WanSyncState getWanSyncState() {
        return syncManager.getWanSyncState();
    }

    @Override
    public DistributedServiceWanEventCounters getReceivedEventCounters(String serviceName) {
        return receivedWanEventCounters.getWanEventCounter("", "", serviceName);
    }

    @Override
    public DistributedServiceWanEventCounters getSentEventCounters(String wanReplicationName,
                                                                   String wanPublisherId,
                                                                   String serviceName) {
        return sentWanEventCounters.getWanEventCounter(wanReplicationName, wanPublisherId, serviceName);
    }

    @Override
    public void removeWanEventCounters(String serviceName, String dataStructureName) {
        receivedWanEventCounters.removeCounter(serviceName, dataStructureName);
        sentWanEventCounters.removeCounter(serviceName, dataStructureName);
    }

    @Override
    public Operation getPostJoinOperation() {
        Map<String, WanReplicationConfig> wanConfigs = node.getConfig().getWanReplicationConfigs();
        return new PostJoinWanOperation(wanConfigs.values());
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        final Collection<WanReplicationPublisherDelegate> publishers = getWanReplications().values();
        // populate for all WanAntiEntropyEventPublishOperation
        for (WanReplicationPublisherDelegate publisher : publishers) {
            for (WanReplicationEndpoint endpoint : publisher.getEndpoints()) {
                if (endpoint instanceof LiveOperationsTracker) {
                    ((LiveOperationsTracker) endpoint).populate(liveOperations);
                }
            }
        }
        eventProcessor.populate(liveOperations);
    }

    @Override
    public void shutdown() {
        consumerContainer.shutdown();
        syncManager.shutdown();
        eventProcessor.shutdown();
        publisherContainer.shutdown();
    }

    @Override
    public void pause(String wanReplicationName, String wanPublisherId) {
        getEndpointOrFail(wanReplicationName, wanPublisherId).pause();
    }

    @Override
    public void stop(String wanReplicationName, String wanPublisherId) {
        getEndpointOrFail(wanReplicationName, wanPublisherId).stop();
    }

    @Override
    public void resume(String wanReplicationName, String wanPublisherId) {
        getEndpointOrFail(wanReplicationName, wanPublisherId).resume();
    }

    @Override
    public void checkWanReplicationQueues(String name) {
        getWanReplications().get(name).checkWanReplicationQueues();
    }

    @Override
    public void syncMap(String wanReplicationName, String wanPublisherId, String mapName) {
        WanSyncEvent event = new WanSyncEvent(WanSyncType.SINGLE_MAP, mapName);
        syncManager.initiateAntiEntropyRequest(wanReplicationName, wanPublisherId, event);
    }

    @Override
    public void syncAllMaps(String wanReplicationName, String wanPublisherId) {
        WanSyncEvent event = new WanSyncEvent(WanSyncType.ALL_MAPS);
        syncManager.initiateAntiEntropyRequest(wanReplicationName, wanPublisherId, event);
    }

    @Override
    public void consistencyCheck(String wanReplicationName, String wanPublisherId, String mapName) {
        MerkleTreeConfig merkleTreeConfig = node.getConfig().findMapMerkleTreeConfig(mapName);
        if (!merkleTreeConfig.isEnabled()) {
            emitManagementCenterEvent(new WanConsistencyCheckIgnoredEvent(wanReplicationName,
                    wanPublisherId, mapName, "Map has merkle trees disabled."));
            logger.info(String.format(
                    "Consistency check request for WAN replication '%s',"
                            + " target group name '%s' and map '%s'"
                            + " ignored because map has merkle trees disabled",
                    wanReplicationName, wanPublisherId, mapName));
            return;
        }

        syncManager.initiateAntiEntropyRequest(wanReplicationName, wanPublisherId,
                new WanConsistencyCheckEvent(mapName));
    }

    @Override
    public void clearQueues(String wanReplicationName, String wanPublisherId) {
        WanReplicationEndpoint endpoint = getEndpointOrFail(wanReplicationName, wanPublisherId);
        endpoint.clearQueues();
    }

    @Override
    public void addWanReplicationConfigLocally(WanReplicationConfig wanConfig) {
        appendWanReplicationConfig(wanConfig);
        publisherContainer.ensurePublishersInitialized(wanConfig.getName());
    }

    @Override
    public AddWanConfigResult addWanReplicationConfig(final WanReplicationConfig wanConfig) {
        Event mancenterEvent;
        WanReplicationConfig existingConfig = node.getConfig().getWanReplicationConfig(wanConfig.getName());
        AddWanConfigResult result;
        HashSet<String> newPublisherIds = new HashSet<String>(getPublisherConfigMap(wanConfig).keySet());
        if (existingConfig != null) {
            HashSet<String> ignoredPublisherIds = new HashSet<String>(getPublisherConfigMap(existingConfig).keySet());
            ignoredPublisherIds.retainAll(newPublisherIds);
            newPublisherIds.removeAll(ignoredPublisherIds);
            result = new AddWanConfigResult(newPublisherIds, ignoredPublisherIds);
            if (newPublisherIds.isEmpty()) {
                mancenterEvent = AddWanConfigIgnoredEvent.alreadyExists(wanConfig.getName());
            } else {
                mancenterEvent = new WanConfigurationExtendedEvent(wanConfig.getName(), newPublisherIds);
            }
        } else {
            mancenterEvent = new WanConfigurationAddedEvent(wanConfig.getName());
            result = new AddWanConfigResult(newPublisherIds, Collections.<String>emptySet());
        }

        invokeAddWanReplicationConfig(wanConfig);
        emitManagementCenterEvent(mancenterEvent);
        return result;
    }

    private void invokeAddWanReplicationConfig(final WanReplicationConfig wanConfig) {
        try {
            OperationService operationService = node.getNodeEngine().getOperationService();
            operationService.invokeOnAllPartitions(null, new AddWanConfigOperationFactory(wanConfig));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return migrationAwareService.getAllServiceNamespaces(event);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return migrationAwareService.isKnownServiceNamespace(namespace);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        return migrationAwareService.prepareReplicationOperation(event, namespaces);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrationAwareService.commitMigration(event);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        migrationAwareService.rollbackMigration(event);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // NOP
    }

    @Override
    public void reset() {
        final Collection<WanReplicationPublisherDelegate> publishers = getWanReplications().values();
        for (WanReplicationPublisherDelegate publisherDelegate : publishers) {
            for (WanReplicationEndpoint endpoint : publisherDelegate.getEndpoints()) {
                if (endpoint instanceof AbstractWanPublisher) {
                    ((AbstractWanPublisher) endpoint).reset();
                }
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }
}
