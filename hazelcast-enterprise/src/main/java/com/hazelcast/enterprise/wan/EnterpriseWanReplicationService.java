package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
import com.hazelcast.enterprise.wan.operation.WanOperation;
import com.hazelcast.enterprise.wan.replication.AbstractWanReplication;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ReplicationSupportingService;
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
    private final ILogger logger;

    private final Map<String, WanReplicationPublisherDelegate> wanReplications = initializeWebReplicationPublisherMapping();
    private final Object publisherMutex = new Object();
    private final Object executorMutex = new Object();
    private volatile StripedExecutor executor;

    public EnterpriseWanReplicationService(Node node) {
        this.node = node;
        this.logger = node.getLogger(EnterpriseWanReplicationService.class.getName());
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        EWRMigrationContainer migrationData = new EWRMigrationContainer();
        Set<Map.Entry<String, WanReplicationPublisherDelegate>> entrySet = wanReplications.entrySet();
        for (Map.Entry<String, WanReplicationPublisherDelegate> entry : entrySet) {
            String wanReplicationName = entry.getKey();
            WanReplicationPublisherDelegate publisherDelegate = entry.getValue();
            for (WanReplicationEndpoint endpoint : publisherDelegate.getEndpoints().values()) {
                AbstractWanReplication wanReplication = (AbstractWanReplication) endpoint;
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
                        migrationData.addMapEventQueueMap(wanReplicationName,
                                wanReplication.getTargetGroupName(), eligibleCacheEventQueues);
                    }
                }
            }
        }

        if (migrationData.isEmpty()) {
            return null;
        } else {
            return new EWRQueueReplicationOperation(migrationData, event.getPartitionId());
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

    @Override
    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    private void clearMigrationData(int partitionId) {
        synchronized (publisherMutex) {
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
            List<WanTargetClusterConfig> targets = wanReplicationConfig.getTargetClusterConfigs();

            Map<String, WanReplicationEndpoint> targetEndpoints = new HashMap<String, WanReplicationEndpoint>();
            for (WanTargetClusterConfig targetClusterConfig : targets) {
                WanReplicationEndpoint target;
                if (targetClusterConfig.getReplicationImpl() != null) {
                    try {
                        target = ClassLoaderUtil
                                .newInstance(node.getConfigClassLoader(), targetClusterConfig.getReplicationImpl());
                    } catch (Exception e) {
                        throw ExceptionUtil.rethrow(e);
                    }
                } else {
                    target = new WanNoDelayReplication();
                }
                String groupName = targetClusterConfig.getGroupName();
                target.init(node, name, targetClusterConfig, wanReplicationConfig.isSnapshotEnabled());
                targetEndpoints.put(groupName, target);
            }
            wr = new WanReplicationPublisherDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
    }

    public WanReplicationEndpoint getEndpoint(String wanReplicationName, String target) {
        WanReplicationPublisherDelegate publisherDelegate
                = (WanReplicationPublisherDelegate) getWanReplicationPublisher(wanReplicationName);
        Map<String, WanReplicationEndpoint> endpoints = publisherDelegate.getEndpoints();
        return endpoints.get(target);
    }

    @Override
    public void handle(final Packet packet) {
        handleEvent(packet, null);
    }

    public void handleEvent(final Data data, WanOperation wanOperation) {
        Object event = node.nodeEngine.toObject(data);
        if (event instanceof BatchWanReplicationEvent) {
            handleRepEvent((BatchWanReplicationEvent) event, wanOperation);
        } else {
            handleRepEvent((WanReplicationEvent) event, wanOperation);
        }
    }

    private void handleRepEvent(final BatchWanReplicationEvent batchWanReplicationEvent, WanOperation op) {
        StripedExecutor ex = getExecutor();
        int partitionId = getPartitionId(batchWanReplicationEvent);
        BatchWanEventRunnable wanEventStripedRunnable
                = new BatchWanEventRunnable(batchWanReplicationEvent, op, partitionId);
        try {
            ex.execute(wanEventStripedRunnable);
        } catch (RejectedExecutionException ree) {
            logger.info("Can not handle incoming wan replication event. Retrying.");
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
            logger.info("Can not handle incoming wan replication event.");
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

        public WanEventRunnable(WanReplicationEvent event,
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

        public BatchWanEventRunnable(BatchWanReplicationEvent batchEvent,
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

        public AbstractWanEventRunnable(WanOperation operation, int partitionId) {
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
        return  node.nodeEngine.getPartitionService().getPartitionId(key);
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
        synchronized (publisherMutex) {
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
            StripedExecutor ex = executor;
            if (ex != null) {
                ex.shutdown();
            }
            wanReplications.clear();
        }
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

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWebReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }
}
