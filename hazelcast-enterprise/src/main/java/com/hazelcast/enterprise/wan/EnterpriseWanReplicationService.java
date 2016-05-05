package com.hazelcast.enterprise.wan;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
import com.hazelcast.enterprise.wan.sync.PartitionSyncReplicationEventObject;
import com.hazelcast.enterprise.wan.sync.WanSyncManager;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MapUtil;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ExecutorConfig.DEFAULT_POOL_SIZE;

/**
 * Enterprise implementation for WAN replication
 */
public class EnterpriseWanReplicationService
        implements WanReplicationService, MigrationAwareService {


    private static final int STRIPED_RUNNABLE_TIMEOUT_SECONDS = 10;
    private static final int STRIPED_RUNNABLE_JOB_QUEUE_SIZE = 1000;
    private static final int DEFAULT_KEY_FOR_STRIPED_EXECUTORS = -1;

    private final Node node;
    private WanSyncManager syncManager;
    private final ILogger logger;

    private final Map<String, WanReplicationPublisherDelegate> wanReplications = initializeWanReplicationPublisherMapping();
    private final Map<String, WanReplicationConsumer> wanConsumers = initializeCustomWanReplicationConsumerMapping();
    private final Object publisherMutex = new Object();
    private final Object executorMutex = new Object();
    private final Object syncManagerMutex = new Object();
    private volatile StripedExecutor executor;

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
    }

    public void initializeCustomConsumers() {
        Map<String, WanReplicationConfig> configs = node.getConfig().getWanReplicationConfigs();
        if (configs != null) {
            for (Map.Entry<String, WanReplicationConfig> wanReplicationConfigEntry : configs.entrySet()) {
                WanConsumerConfig consumerConfig = wanReplicationConfigEntry.getValue().getWanConsumerConfig();

                if (consumerConfig != null) {
                    WanReplicationConsumer consumer;
                    if (consumerConfig.getImplementation() != null) {
                        consumer = (WanReplicationConsumer) consumerConfig.getImplementation();
                    } else if (consumerConfig.getClassName() != null) {
                        try {
                            consumer = ClassLoaderUtil
                                    .newInstance(node.getConfigClassLoader(), consumerConfig.getClassName());
                        } catch (Exception e) {
                            throw ExceptionUtil.rethrow(e);
                        }
                    } else {
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
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (wanReplications.isEmpty()) {
            return null;
        }

        int partitionId = event.getPartitionId();
        EWRMigrationContainer migrationData = new EWRMigrationContainer();
        Set<Map.Entry<String, WanReplicationPublisherDelegate>> entrySet = wanReplications.entrySet();
        for (Map.Entry<String, WanReplicationPublisherDelegate> entry : entrySet) {
            String wanReplicationName = entry.getKey();
            WanReplicationPublisherDelegate publisherDelegate = entry.getValue();
            for (WanReplicationEndpoint endpoint : publisherDelegate.getEndpoints().values()) {
                AbstractWanPublisher wanReplication = (AbstractWanPublisher) endpoint;
                PublisherQueueContainer publisherQueueContainer = endpoint.getPublisherQueueContainer();
                Map<Integer, PartitionWanEventContainer> eventQueueMap
                        = publisherQueueContainer.getPublisherEventQueueMap();
                if (eventQueueMap.get(partitionId) != null) {
                    PartitionWanEventContainer partitionWanEventContainer
                            = eventQueueMap.get(partitionId);
                    PartitionWanEventQueueMap eligibleMapEventQueues
                            = partitionWanEventContainer.getMapEventQueueMapByBackupCount(event.getReplicaIndex());
                    PartitionWanEventQueueMap eligibleCacheEventQueues
                            = partitionWanEventContainer.getCacheEventQueueMapByBackupCount(event.getReplicaIndex());
                    if (!eligibleMapEventQueues.isEmpty()) {
                        migrationData.addMapEventQueueMap(wanReplicationName,
                                wanReplication.getTargetGroupName(), eligibleMapEventQueues);
                    }
                    if (!eligibleCacheEventQueues.isEmpty()) {
                        migrationData.addCacheEventQueueMap(wanReplicationName,
                                wanReplication.getTargetGroupName(), eligibleCacheEventQueues);
                    }
                }
            }
        }

        if (migrationData.isEmpty()) {
            return null;
        } else {
            return new EWRQueueReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
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
            Map<String, WanReplicationEndpoint> wanReplicationEndpoints = wanReplication.getEndpoints();
            if (wanReplicationEndpoints != null) {
                for (WanReplicationEndpoint wanReplicationEndpoint : wanReplicationEndpoints.values()) {
                    if (wanReplicationEndpoint != null) {
                        PublisherQueueContainer publisherQueueContainer
                                = wanReplicationEndpoint.getPublisherQueueContainer();
                        PartitionWanEventContainer eventQueueContainer
                                = publisherQueueContainer.getPublisherEventQueueMap().get(partitionId);
                        eventQueueContainer.clear();
                    }
                }
            }
        }
    }

    @Override
    public WanReplicationPublisher getWanReplicationPublisher(String name) {
        WanReplicationPublisherDelegate wr = wanReplications.get(name);
        if (wr != null) {
            return wr;
        }
        synchronized (publisherMutex) {
            wr = wanReplications.get(name);
            if (wr != null) {
                return wr;
            }
            WanReplicationConfig wanReplicationConfig = node.getConfig().getWanReplicationConfig(name);
            if (wanReplicationConfig == null) {
                return null;
            }
            List<WanPublisherConfig> publisherConfigs = wanReplicationConfig.getWanPublisherConfigs();
            Map<String, WanReplicationEndpoint> targetEndpoints = new HashMap<String, WanReplicationEndpoint>();

            if (!publisherConfigs.isEmpty()) {
                for (WanPublisherConfig wanPublisherConfig : publisherConfigs) {
                    WanReplicationEndpoint target;
                    if (wanPublisherConfig.getImplementation() != null) {
                        target = (WanReplicationEndpoint) wanPublisherConfig.getImplementation();
                    } else if (wanPublisherConfig.getClassName() != null) {
                        target = createWanReplicationEndpoint(wanPublisherConfig.getClassName());
                    } else {
                        throw new InvalidConfigurationException("Either \'implementation\' or \'className\' "
                                + "attribute need to be set in WanPublisherConfig");
                    }
                    String groupName = wanPublisherConfig.getGroupName();
                    target.init(node, wanReplicationConfig, wanPublisherConfig);
                    targetEndpoints.put(groupName, target);
                }
            }
            wr = new WanReplicationPublisherDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
    }

    private WanReplicationEndpoint createWanReplicationEndpoint(String className) {
        try {
            return ClassLoaderUtil
                    .newInstance(node.getConfigClassLoader(), className);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public WanReplicationEndpoint getEndpoint(String wanReplicationName, String target) {
        WanReplicationPublisherDelegate publisherDelegate
                = (WanReplicationPublisherDelegate) getWanReplicationPublisher(wanReplicationName);
        Map<String, WanReplicationEndpoint> endpoints = publisherDelegate.getEndpoints();
        return endpoints.get(target);
    }

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
        BatchWanEventRunnable wanEventStripedRunnable
                = new BatchWanEventRunnable(batchWanReplicationEvent, op, partitionId);
        try {
            ex.execute(wanEventStripedRunnable);
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

    /**
     * {@link StripedRunnable} implementation that is responsible dispatching incoming {@link WanReplicationEvent}s to
     * related {@link ReplicationSupportingService}
     */
    private class WanEventRunnable extends AbstractWanEventRunnable {

        WanReplicationEvent event;

        WanEventRunnable(WanReplicationEvent event,
                         WanOperation operation,
                         int partitionId) {
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
            }
        }
    }

    /**
     * {@link StripedRunnable} implementation that is responsible dispatching incoming {@link WanReplicationEvent}s to
     * related {@link ReplicationSupportingService}
     */
    private class BatchWanEventRunnable extends AbstractWanEventRunnable {

        BatchWanReplicationEvent batchEvent;

        BatchWanEventRunnable(BatchWanReplicationEvent batchEvent,
                              WanOperation operation, int partitionId) {
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
                    HazelcastThreadGroup hazelcastThreadGroup = node.getHazelcastThreadGroup();
                    String prefix = hazelcastThreadGroup.getThreadNamePrefix("wan");
                    ThreadGroup threadGroup = hazelcastThreadGroup.getInternalThreadGroup();
                    executor = new StripedExecutor(logger, prefix, threadGroup,
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
            Map<String, WanReplicationEndpoint> wanReplicationEndpoints = wanReplication.getEndpoints();
            if (wanReplicationEndpoints != null) {
                for (WanReplicationEndpoint wanReplicationEndpoint : wanReplicationEndpoints.values()) {
                    if (wanReplicationEndpoint != null) {
                        wanReplicationEndpoint.shutdown();
                    }
                }
            }
        }

        for (WanReplicationConsumer consumer : wanConsumers.values()) {
            consumer.shutdown();
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

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWanReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }

    private ConcurrentHashMap<String, WanReplicationConsumer> initializeCustomWanReplicationConsumerMapping() {
        return new ConcurrentHashMap<String, WanReplicationConsumer>(2);
    }

    @Override
    public void syncMap(String wanReplicationName, String targetGroupName, String mapName) {
        initializeSyncManagerIfNeeded();
        syncManager.initiateSyncOnAllPartitions(wanReplicationName, targetGroupName, mapName);
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

    public void publishMapWanSyncEvent(PartitionSyncReplicationEventObject eventObject) {
        WanReplicationEndpoint endpoint = getEndpoint(eventObject.getWanReplicationName(), eventObject.getTargetGroupName());
        endpoint.publishMapSyncEvent(eventObject);
    }
}
