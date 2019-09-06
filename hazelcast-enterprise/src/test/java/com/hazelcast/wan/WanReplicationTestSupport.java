package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.impl.replication.AbstractWanPublisher;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WanStatisticsRule;
import com.hazelcast.wan.DistributedServiceWanEventCounters.DistributedObjectWanEventCounters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class WanReplicationTestSupport extends HazelcastTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(WanReplicationTestSupport.class);

    protected HazelcastInstance[] clusterA = new HazelcastInstance[2];
    protected HazelcastInstance[] clusterB = new HazelcastInstance[2];
    protected HazelcastInstance[] clusterC = new HazelcastInstance[2];
    protected HazelcastInstance[] singleNodeA = new HazelcastInstance[1];
    protected HazelcastInstance[] singleNodeB = new HazelcastInstance[1];
    protected HazelcastInstance[] singleNodeC = new HazelcastInstance[1];
    protected TestHazelcastInstanceFactory factory;

    protected Config configA;
    protected Config configB;
    protected Config configC;

    protected static Random random = new Random();

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    @Rule
    public WanStatisticsRule wanStatisticsRule = new WanStatisticsRule();

    @After
    public void cleanup() {
        wanStatisticsRule.snapshotStats(factory);
        factory.shutdownAll();
    }

    protected void givenSizeOfClusterB(int sizeOfCluster) {
        clusterB = new HazelcastInstance[sizeOfCluster];
    }

    public abstract InMemoryFormat getMemoryFormat();

    protected boolean isSnapshotEnabled() {
        return false;
    }

    protected int getMaxConcurrentInvocations() {
        return -1;
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                  .setEnabled(false);
        joinConfig.getTcpIpConfig()
                  .setEnabled(true)
                  .addMember("127.0.0.1");
        if (isNativeMemoryEnabled()) {
            config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
                  .setNativeMemoryConfig(getMemoryConfig());
        }
        return config;
    }

    protected void startClusterA() {
        initCluster(clusterA, configA);
    }

    protected void startClusterB() {
        initCluster(clusterB, configB);
    }

    protected void startClusterC() {
        initCluster(clusterC, configC);
    }

    protected void startAllClusters() {
        startClusterA();
        startClusterB();
        startClusterC();
    }

    protected void initCluster(HazelcastInstance[] cluster, Config config) {
        initCluster(cluster, config, factory);
    }

    protected void initCluster(HazelcastInstance[] cluster,
                               Config config,
                               TestHazelcastInstanceFactory factory) {
        factory.cleanup();
        for (int i = 0; i < cluster.length; i++) {
            config.setInstanceName(config.getInstanceName() + i);
            cluster[i] = factory.newHazelcastInstance(config);
        }
    }

    public static HazelcastInstance getNode(HazelcastInstance[] cluster) {
        return cluster[random.nextInt(cluster.length)];
    }

    protected String getClusterEndPoints(Config config, int count) {
        StringBuilder ends = new StringBuilder();

        int port = config.getNetworkConfig().getPort();

        for (int i = 0; i < count; i++) {
            ends.append("127.0.0.1:").append(port++).append(",");
        }
        return ends.toString();
    }

    protected WanBatchReplicationPublisherConfig targetCluster(Config config, int count) {
        return targetCluster(config, count, ConsistencyCheckStrategy.NONE);
    }

    protected WanBatchReplicationPublisherConfig targetCluster(Config config,
                                                               int count,
                                                               ConsistencyCheckStrategy consistencyCheckStrategy) {
        WanBatchReplicationPublisherConfig target = new WanBatchReplicationPublisherConfig()
                .setGroupName(config.getGroupConfig().getName())
                .setTargetEndpoints(getClusterEndPoints(config, count))
                .setAcknowledgeType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE)
                .setSnapshotEnabled(isSnapshotEnabled())
                .setBatchMaxDelayMillis(10)
                .setMaxConcurrentInvocations(getMaxConcurrentInvocations());
        target.getWanSyncConfig().setConsistencyCheckStrategy(consistencyCheckStrategy);
        return target;
    }

    protected void printReplicaConfig(Config config) {
        final Map<String, WanReplicationConfig> configs = config.getWanReplicationConfigs();
        for (Map.Entry<String, WanReplicationConfig> entry : configs.entrySet()) {
            LOGGER.info(entry.getKey() + " ==> " + entry.getValue());
        }
    }

    protected void printAllReplicaConfig() {
        LOGGER.info("\n==configA==\n");
        printReplicaConfig(configA);
        LOGGER.info("\n==configB==\n");
        printReplicaConfig(configB);
        LOGGER.info("\n==configC==\n");
        printReplicaConfig(configC);
        LOGGER.info("==\n");
    }

    protected static void assertWanQueueSizesEventually(final HazelcastInstance[] cluster,
                                                        final String wanReplicationConfigName,
                                                        final String endpointGroupName,
                                                        final int totalEvents) {
        assertWanQueueSizesEventually(cluster, wanReplicationConfigName, endpointGroupName, totalEvents, totalEvents);
    }

    protected static void assertWanQueueSizesEventually(final HazelcastInstance[] cluster,
                                                        final String wanReplicationConfigName,
                                                        final String endpointGroupName,
                                                        final int primaryEvents,
                                                        final int backupEvents) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int totalBackupEvents = 0;
                int totalEvents = 0;
                for (HazelcastInstance instance : cluster) {
                    if (instance != null && instance.getLifecycleService().isRunning()) {
                        EnterpriseWanReplicationService s = getWanReplicationService(instance);
                        WanReplicationPublisherDelegate delegate
                                = (WanReplicationPublisherDelegate) s.getWanReplicationPublisher(wanReplicationConfigName);
                        AbstractWanPublisher endpoint = (AbstractWanPublisher) delegate.getEndpoint(endpointGroupName);
                        totalEvents += endpoint.getCurrentElementCount();
                        totalBackupEvents += endpoint.getCurrentBackupElementCount();
                    }
                }
                assertEquals(primaryEvents, totalEvents);
                assertEquals(backupEvents, totalBackupEvents);
            }
        });
    }

    protected static void assertReceivedEventCountEventually(HazelcastInstance[] targetCluster,
                                                             String serviceName,
                                                             String distributedObjectName,
                                                             int expectedSyncCount,
                                                             int expectedUpdateCount,
                                                             int expectedRemoveCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int syncCount = 0;
                int updateCount = 0;
                int removeCount = 0;
                for (HazelcastInstance instance : targetCluster) {
                    DistributedServiceWanEventCounters serviceCounters = getWanReplicationService(instance).getReceivedEventCounters(serviceName);
                    DistributedObjectWanEventCounters objectCounters = serviceCounters.getEventCounterMap().get(distributedObjectName);
                    if (objectCounters != null) {
                        syncCount += objectCounters.getSyncCount();
                        updateCount += objectCounters.getUpdateCount();
                        removeCount += objectCounters.getRemoveCount();
                    }
                }
                assertEquals(expectedSyncCount, syncCount);
                assertEquals(expectedUpdateCount, updateCount);
                assertEquals(expectedRemoveCount, removeCount);
            }
        });

    }

    protected static void assertWanPublisherStateEventually(final HazelcastInstance[] cluster,
                                                            final String wanReplicationConfigName,
                                                            final String endpointGroupName,
                                                            final WanPublisherState state) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : cluster) {
                    final LocalWanStats localWanStats = getWanReplicationService(instance)
                            .getStats().get(wanReplicationConfigName);
                    final LocalWanPublisherStats publisherStats = localWanStats.getLocalWanPublisherStats()
                                                                               .get(endpointGroupName);
                    assertEquals(state, publisherStats.getPublisherState());
                }
            }
        });
    }

    public static EnterpriseWanReplicationService getWanReplicationService(HazelcastInstance instance) {
        return (EnterpriseWanReplicationService) getNodeEngineImpl(instance).getWanReplicationService();
    }

    protected void stopWanReplication(final HazelcastInstance[] cluster,
                                      final String wanRepName,
                                      final String targetGroupName) {
        for (HazelcastInstance instance : cluster) {
            getWanReplicationService(instance).stop(wanRepName, targetGroupName);
        }

        // wait until WAN replication threads have observed the stopped state
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance member : cluster) {
                    WanBatchReplication endpoint = (WanBatchReplication) wanReplicationService(member)
                            .getEndpointOrFail(wanRepName, targetGroupName);
                    assertTrue(!endpoint.getReplicationStrategy().hasOngoingReplication());
                }
            }
        });
    }

    protected void pauseWanReplication(final HazelcastInstance[] cluster,
                                       final String wanRepName,
                                       final String targetGroupName) {
        for (HazelcastInstance instance : cluster) {
            getWanReplicationService(instance).pause(wanRepName, targetGroupName);
        }

        // wait until WAN replication threads have observed the paused state
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance member : cluster) {
                    WanBatchReplication endpoint = (WanBatchReplication) wanReplicationService(member)
                            .getEndpointOrFail(wanRepName, targetGroupName);
                    assertTrue(!endpoint.getReplicationStrategy().hasOngoingReplication());
                }
            }
        });
    }

    protected void resumeWanReplication(HazelcastInstance[] cluster, String wanRepName, String targetGroupName) {
        for (HazelcastInstance instance : cluster) {
            getWanReplicationService(instance).resume(wanRepName, targetGroupName);
        }
    }

    protected boolean isNativeMemoryEnabled() {
        return getMemoryFormat() == InMemoryFormat.NATIVE;
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(128, MemoryUnit.MEGABYTES);
        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(memorySize).setEnabled(true);
    }

    public void startGatedThread(GatedThread thread) {
        thread.start();
    }

    public abstract class GatedThread extends Thread {

        private final CyclicBarrier gate;

        protected GatedThread(CyclicBarrier gate) {
            this.gate = gate;
        }

        @Override
        public void run() {
            try {
                gate.await();
                go();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        public abstract void go();
    }
}