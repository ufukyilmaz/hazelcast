package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.enterprise.wan.operation.EWRQueueReplicationOperation;
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
        if (wanReplications != null) {
            WanReplicationPublisherDelegate publisherDelegate = wanReplications.get(wanReplicationName);
            if (publisherDelegate != null) {
                return publisherDelegate.getEndpoints().get(target);
            }
        }
        return null;
    }

    @Override
    public void handle(final Packet packet) {
        handleEvent(packet);
    }

    public void handleEvent(final Data data) {
        Object event = node.nodeEngine.toObject(data);
        if (event instanceof BatchWanReplicationEvent) {
            BatchWanReplicationEvent batchWanEvent = (BatchWanReplicationEvent) event;
            for (WanReplicationEvent wanReplicationEvent : batchWanEvent.getEventList()) {
                handleRepEvent(wanReplicationEvent);
            }
        } else {
            handleRepEvent((WanReplicationEvent) event);
        }
    }

    private void handleRepEvent(final WanReplicationEvent replicationEvent) {
        StripedExecutor ex = getExecutor();
        boolean taskSubmitted = false;
        WanEventStripedRunnable wanEventStripedRunnable = new WanEventStripedRunnable(replicationEvent);
        do {
            try {
                ex.execute(wanEventStripedRunnable);
                taskSubmitted = true;
            } catch (RejectedExecutionException ree) {
                logger.info("Can not handle incoming wan replication event. Retrying.");
            }
        } while (!taskSubmitted);
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
    private class WanEventStripedRunnable implements StripedRunnable, TimeoutRunnable {

        WanReplicationEvent wanReplicationEvent;

        public WanEventStripedRunnable(WanReplicationEvent wanReplicationEvent) {
            this.wanReplicationEvent = wanReplicationEvent;
        }

        @Override
        public int getKey() {
            return node.nodeEngine.getPartitionService().getPartitionId(
                    ((EnterpriseReplicationEventObject) wanReplicationEvent.getEventObject()).getKey());
        }

        @Override
        public long getTimeout() {
            return STRIPED_RUNNABLE_TIMEOUT_SECONDS;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.SECONDS;
        }

        @Override
        public void run() {
            try {
                String serviceName = wanReplicationEvent.getServiceName();
                ReplicationSupportingService service = node.nodeEngine.getService(serviceName);
                service.onReplicationEvent(wanReplicationEvent);
            } catch (Exception e) {
                logger.severe(e);
            }
        }
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
