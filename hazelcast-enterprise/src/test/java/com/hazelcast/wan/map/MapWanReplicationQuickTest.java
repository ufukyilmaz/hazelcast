package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.WanReplicationQueueFullException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.wan.WanPublisherState.PAUSED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWanReplicationQuickTest extends MapWanReplicationTestSupport {

    @Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {NATIVE},
                {BINARY},
        });
    }

    @Parameter(0)
    public InMemoryFormat memoryFormat;

    private HazelcastInstance[] basicCluster = new HazelcastInstance[2];

    @Test(expected = WanReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrun() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanBatchReplicationPublisherConfig pc = wanConfig.getBatchPublisherConfigs().get(0);
        pc.setQueueCapacity(10)
          .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION);
        initCluster(basicCluster, configA);
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test(expected = WanReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrunIfReplicationActive() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanBatchReplicationPublisherConfig pc = wanConfig.getBatchPublisherConfigs().get(0);
        pc.setQueueCapacity(10)
          .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE);
        initCluster(basicCluster, configA);
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test
    public void testExceptionOnQueueOverrunIfReplicationPassive() throws ExecutionException, InterruptedException {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanBatchReplicationPublisherConfig pc = wanConfig.getBatchPublisherConfigs().get(0);
        pc.setQueueCapacity(10)
          .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE);
        initCluster(basicCluster, configA);
        for (HazelcastInstance instance : basicCluster) {
            ChangeWanStateOperation changeWanStateOperation = new ChangeWanStateOperation("atob",
                    configB.getClusterName(), PAUSED);
            getOperationService(instance).createInvocationBuilder(EnterpriseWanReplicationService.SERVICE_NAME,
                    changeWanStateOperation, getNode(instance).address).invoke().get();
        }
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test
    public void testPublisherProperties() {
        Config propTestConfig = getConfig();
        propTestConfig.setNetworkConfig(configA.getNetworkConfig());
        propTestConfig.setNativeMemoryConfig(configA.getNativeMemoryConfig());
        propTestConfig.setInstanceName("propTestConfA");
        setupReplicateFrom(propTestConfig, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = propTestConfig.getWanReplicationConfig("atob");
        WanBatchReplicationPublisherConfig pc = wanConfig.getBatchPublisherConfigs().get(0);
        pc.setBatchSize(500)
          .setBatchMaxDelayMillis(1000)
          .setResponseTimeoutMillis(500);
        initCluster(basicCluster, propTestConfig);
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test
    public void testClearQueues() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanBatchReplicationPublisherConfig pc = wanConfig.getBatchPublisherConfigs().get(0);
        pc.setQueueCapacity(1000)
          .setQueueFullBehavior(WanQueueFullBehavior.DISCARD_AFTER_MUTATION);
        initCluster(singleNodeA, configA);
        pauseWanReplication(singleNodeA, "atob", configB.getClusterName());
        createDataIn(singleNodeA, "map", 0, 1000);
        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(singleNodeA[0]);
        WanBatchReplication publisher =
                (WanBatchReplication) wanReplicationService.getPublisherOrFail("atob", configB.getClusterName());
        assertTrueEventually(() -> {
            assert publisher.getStats().getOutboundQueueSize() == 1000;
        });
        wanReplicationService.removeWanEvents("atob", configB.getClusterName());
        assertTrueEventually(() -> {
            assert publisher.getStats().getOutboundQueueSize() == 0;
        });
    }

    @Test
    public void testStatisticsForUninitializedReplications() {
        final String setupName = "unused";
        setupReplicateFrom(configA, configB, clusterB.length, setupName, PassThroughMergePolicy.class.getName());
        initCluster(singleNodeA, configA, factory);

        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(singleNodeA[0]);
        final Map<String, LocalWanStats> stats = wanReplicationService.getStats();
        final LocalWanStats localWanStats = stats.get(setupName);
        assertEquals(1, stats.size());
        assertNotNull(localWanStats);
        assertEquals(1, localWanStats.getLocalWanPublisherStats().size());

        for (LocalWanPublisherStats localWanPublisherStats : localWanStats.getLocalWanPublisherStats().values()) {
            assertEquals(0, localWanPublisherStats.getOutboundQueueSize());
            assertEquals(0, localWanPublisherStats.getTotalPublishedEventCount());
            assertEquals(0, localWanPublisherStats.getTotalPublishLatency());
            assertEquals(WanPublisherState.STOPPED, localWanPublisherStats.getPublisherState());
        }
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return memoryFormat;
    }
}
