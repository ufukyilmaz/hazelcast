package com.hazelcast.enterprise.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanEventCounter;
import com.hazelcast.wan.impl.WanEventCounterContainer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enterprise implementation for WAN replication.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class EnterpriseWanReplicationService implements WanReplicationService, FragmentedMigrationAwareService,
        PostJoinAwareService, LiveOperationsTracker {

    private final Node node;
    private final ILogger logger;
    private final WanReplicationMigrationAwareService migrationAwareService;
    private final WanEventProcessor eventProcessor;
    private final WanPublisherContainer publisherContainer;
    private final WanConsumerContainer consumerContainer;
    private final WanSyncManager syncManager;

    /** WAN event counters for all services and only received events */
    private final WanEventCounterContainer receivedWanEventCounters = new WanEventCounterContainer();

    /** WAN event counters for all services and only sent events */
    private final WanEventCounterContainer sentWanEventCounters = new WanEventCounterContainer();

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
        this.migrationAwareService = new WanReplicationMigrationAwareService(this);
        this.eventProcessor = new WanEventProcessor(node);
        this.publisherContainer = new WanPublisherContainer(node);
        this.consumerContainer = new WanConsumerContainer(node);
        this.syncManager = new WanSyncManager(this, node);
    }

    /**
     * Returns a WAN replication configured under a WAN replication config with
     * the name {@code wanReplicationName} and with a group name of
     * {@code target}.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param groupName          the group name of the publisher
     * @return the WAN endpoint
     * @throws InvalidConfigurationException if there is no replication config
     *                                       with the name {@code wanReplicationName}
     *                                       and group name {@code groupName}
     * @see WanReplicationConfig#getName
     * @see WanPublisherConfig#getGroupName
     */
    public WanReplicationEndpoint getEndpoint(String wanReplicationName, String groupName) {
        final WanReplicationPublisherDelegate publisherDelegate
                = (WanReplicationPublisherDelegate) getWanReplicationPublisher(wanReplicationName);
        final WanReplicationEndpoint endpoint = getEndpointFromDelegate(publisherDelegate, groupName);

        if (publisherDelegate == null || endpoint == null) {
            throw new InvalidConfigurationException("WAN Replication Config doesn't exist with WAN configuration name "
                    + wanReplicationName + " and publisher target group name " + groupName);
        }

        return endpoint;
    }

    private WanReplicationEndpoint getEndpointFromDelegate(WanReplicationPublisherDelegate publisherDelegate, String groupName) {
        if (publisherDelegate != null) {
            return publisherDelegate.getEndpoint(groupName);
        }
        return null;
    }

    /**
     * Processes the replication event sent from the source cluster.
     *
     * @param data         the serialized event, can be of type
     *                     {@link WanReplicationEvent} or
     *                     {@link BatchWanReplicationEvent}
     * @param wanOperation the operation sent by the source cluster
     */
    public void handleEvent(Data data, WanOperation wanOperation) {
        final Object event = node.nodeEngine.toObject(data);
        if (event instanceof BatchWanReplicationEvent) {
            eventProcessor.handleRepEvent((BatchWanReplicationEvent) event, wanOperation);
        } else {
            eventProcessor.handleRepEvent((WanReplicationEvent) event, wanOperation);
        }
    }

    /**
     * Adds a {@link WanReplicationConfig} if there is none registered under
     * the provided {@link WanReplicationConfig#getName()}.
     *
     * @param wanConfig the WAN configuration to add
     * @return {@code true} if the config was added, {@code false} otherwise
     */
    public boolean addWanReplicationConfigIfAbsent(WanReplicationConfig wanConfig) {
        WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(wanConfig.getName());
        if (wanReplicationConfig == null) {
            node.getConfig().addWanReplicationConfig(wanConfig);
            return true;
        }
        return false;
    }

    /**
     * Publishes a sync event for the given {@code wanReplicationName} and
     * {@code targetGroupName}.
     *
     * @param wanReplicationName the WAN replication config name
     * @param targetGroupName    the group name in the WAN replication config
     * @param syncEvent          the WAN sync event
     * @throws InvalidConfigurationException if there is no replication config
     *                                       with the name {@code wanReplicationName}
     */
    public void publishSyncEvent(String wanReplicationName,
                                 String targetGroupName,
                                 WanSyncEvent syncEvent) {
        getEndpoint(wanReplicationName, targetGroupName).publishSyncEvent(syncEvent);
    }

    public void populateSyncEventOnMembers(String wanReplicationName, String targetGroupName, WanSyncEvent syncEvent) {
        syncManager.populateSyncRequestOnMembers(wanReplicationName, targetGroupName, syncEvent);
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
        final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications = getWanReplications();
        if (wanReplications.isEmpty()) {
            return null;
        }

        Map<String, LocalWanStats> wanStatsMap = MapUtil.createHashMap(wanReplications.size());
        for (Map.Entry<String, WanReplicationPublisherDelegate> delegateEntry : wanReplications.entrySet()) {
            LocalWanStats localWanStats = new LocalWanStatsImpl();
            String wanReplicationConfigName = delegateEntry.getKey();
            WanReplicationPublisherDelegate delegate = delegateEntry.getValue();
            localWanStats.getLocalWanPublisherStats().putAll(delegate.getStats());
            wanStatsMap.put(wanReplicationConfigName, localWanStats);
        }
        return wanStatsMap;
    }

    @Override
    public WanSyncState getWanSyncState() {
        return syncManager.getWanSyncState();
    }

    @Override
    public WanEventCounter getReceivedEventCounter(String serviceName) {
        return receivedWanEventCounters.getWanEventCounter(serviceName);
    }

    @Override
    public WanEventCounter getSentEventCounter(String serviceName) {
        return sentWanEventCounters.getWanEventCounter(serviceName);
    }

    @Override
    public void removeWanEventCounters(String serviceName, String dataStructureName) {
        receivedWanEventCounters.removeCounter(serviceName, dataStructureName);
        sentWanEventCounters.removeCounter(serviceName, dataStructureName);
    }

    @Override
    public Operation getPostJoinOperation() {
        PostJoinWanOperation postJoinWanOperation = new PostJoinWanOperation();
        Map<String, WanReplicationConfig> wanConfigs = node.getConfig().getWanReplicationConfigs();
        for (Map.Entry<String, WanReplicationConfig> wanReplicationConfigEntry : wanConfigs.entrySet()) {
            postJoinWanOperation.addWanConfig(wanReplicationConfigEntry.getValue());
        }
        return postJoinWanOperation;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        final Collection<WanReplicationPublisherDelegate> publishers = getWanReplications().values();
        // populate for all WanSyncOperation
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
    public void pause(String name, String targetGroupName) {
        getEndpoint(name, targetGroupName).pause();
    }

    @Override
    public void resume(String name, String targetGroupName) {
        getEndpoint(name, targetGroupName).resume();
    }

    @Override
    public void checkWanReplicationQueues(String name) {
        getWanReplications().get(name).checkWanReplicationQueues();
    }

    @Override
    public void syncMap(String wanReplicationName, String targetGroupName, String mapName) {
        syncManager.initiateSyncRequest(
                wanReplicationName, targetGroupName, new WanSyncEvent(WanSyncType.SINGLE_MAP, mapName));
    }

    @Override
    public void syncAllMaps(String wanReplicationName, String targetGroupName) {
        syncManager.initiateSyncRequest(
                wanReplicationName, targetGroupName, new WanSyncEvent(WanSyncType.ALL_MAPS));
    }

    @Override
    public void clearQueues(String wanReplicationName, String targetGroupName) {
        WanReplicationEndpoint endpoint = getEndpoint(wanReplicationName, targetGroupName);
        endpoint.clearQueues();
    }

    @Override
    public void addWanReplicationConfig(WanReplicationConfig wanConfig) {
        if (addWanReplicationConfigIfAbsent(wanConfig)) {
            getWanReplicationPublisher(wanConfig.getName());
        } else {
            logger.warning(
                    "Ignoring new WAN config request. A WanReplicationConfig already exists with the given name: "
                    + wanConfig.getName());
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
}
