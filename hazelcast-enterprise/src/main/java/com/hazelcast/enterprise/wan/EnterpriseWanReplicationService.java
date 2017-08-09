package com.hazelcast.enterprise.wan;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.enterprise.wan.operation.PostJoinWanOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.enterprise.wan.sync.WanSyncEvent;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.enterprise.wan.sync.WanSyncType;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.WanSyncState;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ExecutorConfig.DEFAULT_POOL_SIZE;
import static com.hazelcast.nio.ClassLoaderUtil.getOrCreate;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.ThreadUtil.createThreadName;

/**
 * Enterprise implementation for WAN replication.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
public class EnterpriseWanReplicationService implements WanReplicationService, FragmentedMigrationAwareService,
        PostJoinAwareService, LiveOperationsTracker {

    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 1000;
    private static final int DEFAULT_KEY_FOR_STRIPED_EXECUTORS = -1;

    private final Node node;
    private volatile WanSyncManager syncManager;
    private final ILogger logger;

    /**
     * Publisher delegates grouped by WAN replication config name.
     */
    private final ConcurrentHashMap<String, WanReplicationPublisherDelegate> wanReplications
            = new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    /**
     * Consumer implementations grouped by WAN replication config name.
     */
    private final Map<String, WanReplicationConsumer> wanConsumers
            = new ConcurrentHashMap<String, WanReplicationConsumer>(2);

    /**
     * Operations which are processed on threads other than the operation thread. We must report these operations
     * to the operation system for it to send operation heartbeats to the operation sender.
     */
    private final Set<Operation> liveOperations = Collections.newSetFromMap(new ConcurrentHashMap<Operation, Boolean>());
    private final Object publisherMutex = new Object();
    private final Object executorMutex = new Object();
    private final Object syncManagerMutex = new Object();
    private volatile StripedExecutor executor;
    private final ConstructorFunction<String, WanReplicationPublisherDelegate> publisherDelegateConstructor =
            new ConstructorFunction<String, WanReplicationPublisherDelegate>() {
                @Override
                public WanReplicationPublisherDelegate createNew(String name) {
                    final WanReplicationConfig replicationConfig = node.getConfig().getWanReplicationConfig(name);
                    final List<WanPublisherConfig> publisherConfigs = replicationConfig.getWanPublisherConfigs();
                    return new WanReplicationPublisherDelegate(name, createPublishers(replicationConfig, publisherConfigs));
                }
            };

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
    }

    /**
     * Construct and initialize all WAN consumers by fetching the class names or implementations from the config and store them
     * under the WAN replication config name in {@link #wanConsumers}.
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

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    private void clearMigrationData(int partitionId) {
        for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint != null) {
                    final PublisherQueueContainer publisherQueueContainer
                            = endpoint.getPublisherQueueContainer();
                    final PartitionWanEventContainer eventQueueContainer
                            = publisherQueueContainer.getPublisherEventQueueMap().get(partitionId);
                    eventQueueContainer.clear();
                }
            }
        }
    }

    @Override
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        if (!wanReplications.containsKey(name) && node.getConfig().getWanReplicationConfig(name) == null) {
            return null;
        }
        return getOrPutSynchronized(wanReplications, name, publisherMutex, publisherDelegateConstructor);
    }

    /**
     * Instantiate and initialize the {@link WanReplicationEndpoint}s and group by endpoint group name.
     */
    private Map<String, WanReplicationEndpoint> createPublishers(
            WanReplicationConfig wanReplicationConfig,
            List<WanPublisherConfig> publisherConfigs) {
        final Map<String, WanReplicationEndpoint> targetEndpoints = new HashMap<String, WanReplicationEndpoint>();
        if (!publisherConfigs.isEmpty()) {
            for (WanPublisherConfig publisherConfig : publisherConfigs) {
                final WanReplicationEndpoint endpoint = getOrCreate(
                        (WanReplicationEndpoint) publisherConfig.getImplementation(),
                        node.getConfigClassLoader(),
                        publisherConfig.getClassName());
                if (endpoint == null) {
                    throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                            + "attribute need to be set in WanPublisherConfig");
                }
                final String groupName = publisherConfig.getGroupName();
                endpoint.init(node, wanReplicationConfig, publisherConfig);
                targetEndpoints.put(groupName, endpoint);
            }
        }
        return targetEndpoints;
    }

    /**
     * Return a WAN replication configured under a WAN replication config with the name {@code wanReplicationName}
     * and with a group name of {@code target}.
     *
     * @param wanReplicationName the name of the {@link WanReplicationConfig}
     * @param groupName          the group name of the publisher
     * @return the wan endpoint
     * @see WanReplicationConfig#getName
     * @see WanPublisherConfig#getGroupName
     */
    public WanReplicationEndpoint getEndpoint(String wanReplicationName, String groupName) {
        final WanReplicationPublisherDelegate publisherDelegate
                = (WanReplicationPublisherDelegate) getWanReplicationPublisher(wanReplicationName);
        if (publisherDelegate == null) {
            throw new InvalidConfigurationException("WAN Replication Config doesn't exist with WAN configuration name "
                    + wanReplicationName + " and publisher target group name " + groupName);
        }
        return publisherDelegate.getEndpoint(groupName);
    }

    /**
     * Processes the replication event sent from the source cluster.
     *
     * @param data         the serialized event, can be of type {@link WanReplicationEvent} or {@link BatchWanReplicationEvent}
     * @param wanOperation the operation sent by the source cluster
     */
    public void handleEvent(final Data data, WanOperation wanOperation) {
        Object event = node.nodeEngine.toObject(data);
        if (event instanceof BatchWanReplicationEvent) {
            handleRepEvent((BatchWanReplicationEvent) event, wanOperation);
        } else {
            handleRepEvent((WanReplicationEvent) event, wanOperation);
        }
    }

    public void handleEvent(WanReplicationEvent event) {
        String serviceName = event.getServiceName();
        ReplicationSupportingService service = node.nodeEngine.getService(serviceName);
        service.onReplicationEvent(event);
    }

    private void handleRepEvent(final BatchWanReplicationEvent batchWanReplicationEvent, WanOperation op) {
        StripedExecutor ex = getExecutor();
        int partitionId = getPartitionId(batchWanReplicationEvent);
        BatchWanEventRunnable wanEventStripedRunnable = new BatchWanEventRunnable(batchWanReplicationEvent, op, partitionId);
        try {
            ex.execute(wanEventStripedRunnable);
            liveOperations.add(op);
        } catch (RejectedExecutionException ree) {
            logger.warning("Can not handle incoming wan replication event. Retrying.", ree);
            op.sendResponse(false);
        }
    }

    private void handleRepEvent(final WanReplicationEvent replicationEvent, WanOperation op) {
        StripedExecutor ex = getExecutor();
        EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) replicationEvent.getEventObject();
        int partitionId = getPartitionId(eventObject.getKey());
        WanEventRunnable wanEventStripedRunnable = new WanEventRunnable(replicationEvent, op, partitionId);
        try {
            ex.execute(wanEventStripedRunnable);
            liveOperations.add(op);
        } catch (RejectedExecutionException ree) {
            logger.warning("Can not handle incoming wan replication event.", ree);
            op.sendResponse(false);
        }
    }

    @Override
    public Map<String, LocalWanStats> getStats() {
        if (wanReplications.isEmpty()) {
            return null;
        }

        Map<String, LocalWanStats> wanStatsMap = MapUtil.createHashMap(wanReplications.size());
        for (Map.Entry<String, WanReplicationPublisherDelegate> delegateEntry : wanReplications.entrySet()) {
            LocalWanStats localWanStats = new LocalWanStatsImpl();
            String schemeName = delegateEntry.getKey();
            WanReplicationPublisherDelegate delegate = delegateEntry.getValue();
            localWanStats.getLocalWanPublisherStats().putAll(delegate.getStats());
            wanStatsMap.put(schemeName, localWanStats);
        }
        return wanStatsMap;
    }

    @Override
    public WanSyncState getWanSyncState() {
        return getSyncManager().getWanSyncState();
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
        // populate for all WanSyncOperation
        for (WanReplicationPublisherDelegate delegate : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : delegate.getEndpoints()) {
                if (endpoint instanceof LiveOperationsTracker) {
                    ((LiveOperationsTracker) endpoint).populate(liveOperations);
                }
            }
        }
        // populate for all WanOperation
        for (Operation op : this.liveOperations) {
            liveOperations.add(op.getCallerAddress(), op.getCallId());
        }
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        if (wanReplications.isEmpty()) {
            return Collections.emptyList();
        }

        final Set<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>();
        for (WanReplicationPublisherDelegate publisher : wanReplications.values()) {
            publisher.collectAllServiceNamespaces(event, namespaces);
        }
        return namespaces;
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        final String serviceName = namespace.getServiceName();
        return namespace instanceof ObjectNamespace
                && (MapService.SERVICE_NAME.equals(serviceName) || CacheService.SERVICE_NAME.equals(serviceName));
    }


    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        if (wanReplications.isEmpty() || namespaces.isEmpty()) {
            return null;
        }
        final EWRMigrationContainer migrationData = new EWRMigrationContainer();
        for (WanReplicationPublisherDelegate delegate : wanReplications.values()) {
            delegate.collectReplicationData(event, namespaces, migrationData);
        }

        return migrationData.isEmpty() ? null
                : new EWRQueueReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    /**
     * {@link StripedRunnable} implementation that is responsible dispatching incoming {@link WanReplicationEvent}s to
     * related {@link ReplicationSupportingService}.
     */
    private class WanEventRunnable extends AbstractWanEventRunnable {

        WanReplicationEvent event;

        WanEventRunnable(WanReplicationEvent event, WanOperation operation, int partitionId) {
            super(operation, partitionId);
            this.event = event;
        }

        @Override
        public void run() {
            try {
                String serviceName = event.getServiceName();
                ReplicationSupportingService service = node.nodeEngine.getService(serviceName);
                event.setAcknowledgeType(operation.getAcknowledgeType());
                service.onReplicationEvent(event);
                operation.sendResponse(true);
            } catch (Exception e) {
                operation.sendResponse(false);
                logger.severe(e);
            } finally {
                liveOperations.remove(operation);
            }
        }
    }

    /**
     * {@link StripedRunnable} implementation that is responsible dispatching incoming {@link WanReplicationEvent}s to
     * related {@link ReplicationSupportingService}.
     */
    private class BatchWanEventRunnable extends AbstractWanEventRunnable {

        BatchWanReplicationEvent batchEvent;

        BatchWanEventRunnable(BatchWanReplicationEvent batchEvent, WanOperation operation, int partitionId) {
            super(operation, partitionId);
            this.batchEvent = batchEvent;
        }

        @Override
        public void run() {
            try {
                for (WanReplicationEvent wanReplicationEvent : batchEvent.getEventList()) {
                    String serviceName = wanReplicationEvent.getServiceName();
                    ReplicationSupportingService service = node.nodeEngine.getService(serviceName);
                    wanReplicationEvent.setAcknowledgeType(operation.getAcknowledgeType());
                    service.onReplicationEvent(wanReplicationEvent);
                }
                operation.sendResponse(true);
            } catch (Exception e) {
                operation.sendResponse(false);
                logger.severe(e);
            } finally {
                liveOperations.remove(operation);
            }
        }
    }

    private abstract class AbstractWanEventRunnable implements StripedRunnable, TimeoutRunnable {

        WanOperation operation;
        int partitionId;

        AbstractWanEventRunnable(WanOperation operation, int partitionId) {
            this.operation = operation;
            this.partitionId = partitionId;
        }

        @Override
        public int getKey() {
            return partitionId;
        }

        @Override
        public long getTimeout() {
            return STRIPED_RUNNABLE_TIMEOUT_SECONDS;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.SECONDS;
        }
    }

    /**
     * Returns the partition ID for the first event or -1 if the event batch is empty.
     */
    private int getPartitionId(BatchWanReplicationEvent batchWanReplicationEvent) {
        List<WanReplicationEvent> eventList = batchWanReplicationEvent.getEventList();
        if (eventList.isEmpty()) {
            return DEFAULT_KEY_FOR_STRIPED_EXECUTORS;
        }
        EnterpriseReplicationEventObject eventObject = (EnterpriseReplicationEventObject) eventList.get(0).getEventObject();
        return getPartitionId(eventObject.getKey());
    }

    private int getPartitionId(Object key) {
        return node.nodeEngine.getPartitionService().getPartitionId(key);
    }

    private StripedExecutor getExecutor() {
        StripedExecutor ex = executor;
        if (ex == null) {
            synchronized (executorMutex) {
                if (executor == null) {
                    String prefix = createThreadName(node.hazelcastInstance.getName(), "wan");
                    executor = new StripedExecutor(logger, prefix,
                            DEFAULT_POOL_SIZE, STRIPED_RUNNABLE_JOB_QUEUE_SIZE);
                }
                ex = executor;
            }
        }
        return ex;
    }

    @Override
    public void shutdown() {
        for (WanReplicationPublisherDelegate wanReplication : wanReplications.values()) {
            for (WanReplicationEndpoint endpoint : wanReplication.getEndpoints()) {
                if (endpoint != null) {
                    endpoint.shutdown();
                }
            }
        }

        for (WanReplicationConsumer consumer : wanConsumers.values()) {
            consumer.shutdown();
        }

        if (syncManager != null) {
            syncManager.shutdown();
        }

        StripedExecutor ex = executor;
        if (ex != null) {
            ex.shutdown();
        }
        wanReplications.clear();
        wanConsumers.clear();
    }

    @Override
    public void pause(String name, String targetGroupName) {
        WanReplicationEndpoint endpoint = getEndpoint(name, targetGroupName);
        endpoint.pause();
    }

    @Override
    public void resume(String name, String targetGroupName) {
        WanReplicationEndpoint endpoint = getEndpoint(name, targetGroupName);
        endpoint.resume();
    }

    @Override
    public void checkWanReplicationQueues(String name) {
        WanReplicationPublisherDelegate delegate = wanReplications.get(name);
        delegate.checkWanReplicationQueues();
    }

    @Override
    public void syncMap(String wanReplicationName, String targetGroupName, String mapName) {
        getSyncManager().initiateSyncRequest(wanReplicationName, targetGroupName,
                new WanSyncEvent(WanSyncType.SINGLE_MAP, mapName));
    }

    @Override
    public void syncAllMaps(String wanReplicationName, String targetGroupName) {
        getSyncManager().initiateSyncRequest(wanReplicationName, targetGroupName,
                new WanSyncEvent(WanSyncType.ALL_MAPS));
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
            logger.warning("Ignoring new WAN config request. A WanReplicationConfig already exists with the given name: "
                    + wanConfig.getName());
        }
    }

    public boolean addWanReplicationConfigIfAbsent(WanReplicationConfig wanConfig) {
        WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(wanConfig.getName());
        if (wanReplicationConfig == null) {
            node.getConfig().addWanReplicationConfig(wanConfig);
            return true;
        }
        return false;
    }

    public void publishSyncEvent(String wanReplicationName, String targetGroupName,
                                 WanSyncEvent syncEvent) {
        WanReplicationEndpoint endpoint = getEndpoint(wanReplicationName, targetGroupName);
        endpoint.publishSyncEvent(syncEvent);
    }

    public void populateSyncEventOnMembers(String wanReplicationName, String targetGroupName, WanSyncEvent syncEvent) {
        getSyncManager().populateSyncRequestOnMembers(wanReplicationName, targetGroupName, syncEvent);
    }

    public WanSyncManager getSyncManager() {
        initializeSyncManagerIfNeeded();
        return syncManager;
    }

    private void initializeSyncManagerIfNeeded() {
        if (syncManager == null) {
            synchronized (syncManagerMutex) {
                if (syncManager == null) {
                    syncManager = new WanSyncManager(node.getNodeEngine());
                }
            }
        }
    }
}
