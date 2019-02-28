package com.hazelcast.wan;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.AbstractWanPublisher;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
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

    public abstract String getReplicationImpl();

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

    protected WanPublisherConfig targetCluster(Config config, int count) {
        return targetCluster(config, count, ConsistencyCheckStrategy.NONE);
    }

    protected WanPublisherConfig targetCluster(Config config, int count, ConsistencyCheckStrategy consistencyCheckStrategy) {
        WanPublisherConfig target = new WanPublisherConfig();
        target.setGroupName(config.getGroupConfig().getName());
        target.setClassName(getReplicationImpl());
        Map<String, Comparable> props = target.getProperties();
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), config.getGroupConfig().getPassword());
        props.put(WanReplicationProperties.ENDPOINTS.key(), (getClusterEndPoints(config, count)));
        props.put(WanReplicationProperties.ACK_TYPE.key(), WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        props.put(WanReplicationProperties.SNAPSHOT_ENABLED.key(), isSnapshotEnabled());
        props.put(WanReplicationProperties.BATCH_MAX_DELAY_MILLIS.key(), "10");
        props.put(WanReplicationProperties.MAX_CONCURRENT_INVOCATIONS.key(), getMaxConcurrentInvocations());

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
                                                        final int eventCount) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int totalBackupEvents = 0;
                int totalEvents = 0;
                for (HazelcastInstance instance : cluster) {
                    final EnterpriseWanReplicationService s = getWanReplicationService(instance);
                    final WanReplicationPublisherDelegate delegate
                            = (WanReplicationPublisherDelegate) s.getWanReplicationPublisher(wanReplicationConfigName);
                    final AbstractWanPublisher endpoint = (AbstractWanPublisher) delegate.getEndpoint(endpointGroupName);
                    totalEvents += endpoint.getCurrentElementCount();
                    totalBackupEvents += endpoint.getCurrentBackupElementCount();
                }
                assertEquals(eventCount, totalEvents);
                assertEquals(eventCount, totalBackupEvents);
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
