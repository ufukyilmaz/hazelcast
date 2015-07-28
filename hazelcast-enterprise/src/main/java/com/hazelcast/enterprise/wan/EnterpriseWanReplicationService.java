package com.hazelcast.enterprise.wan;

import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ExecutorConfig.DEFAULT_POOL_SIZE;

/**
 * Enterprise implementation for WAN replication
 */
public class EnterpriseWanReplicationService
        implements WanReplicationService {


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
            WanReplicationEndpoint[] targetEndpoints = new WanReplicationEndpoint[targets.size()];
            int count = 0;
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
                String password = targetClusterConfig.getGroupPassword();
                String[] addresses = new String[targetClusterConfig.getEndpoints().size()];
                targetClusterConfig.getEndpoints().toArray(addresses);
                target.init(node, groupName, password, wanReplicationConfig.isSnapshotEnabled(), addresses);
                targetEndpoints[count++] = target;
            }
            wr = new WanReplicationPublisherDelegate(name, targetEndpoints);
            wanReplications.put(name, wr);
            return wr;
        }
    }

    @Override
    public void handle(final Packet packet) {
        final Data data = packet.getData();
        handleEvent(data);
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
                WanReplicationEndpoint[] wanReplicationEndpoints = wanReplication.getEndpoints();
                if (wanReplicationEndpoints != null) {
                    for (WanReplicationEndpoint wanReplicationEndpoint : wanReplicationEndpoints) {
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

    private ConcurrentHashMap<String, WanReplicationPublisherDelegate> initializeWebReplicationPublisherMapping() {
        return new ConcurrentHashMap<String, WanReplicationPublisherDelegate>(2);
    }
}
