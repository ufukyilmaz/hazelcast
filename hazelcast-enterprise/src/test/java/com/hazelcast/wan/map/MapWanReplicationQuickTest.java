package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WANReplicationQueueFullException;
import org.junit.Before;
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

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MapWanReplicationQuickTest extends MapWanReplicationTestSupport {

    private static final String BATCH_IMPL = WanBatchReplication.class.getName();

    @Parameters(name = "replicationImpl:{0},memoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {BATCH_IMPL, NATIVE},
                {BATCH_IMPL, BINARY},
        });
    }

    @Parameter(0)
    public String replicationImpl;

    @Parameter(1)
    public InMemoryFormat memoryFormat;

    private HazelcastInstance[] basicCluster = new HazelcastInstance[2];
    private TestHazelcastInstanceFactory factory;

    @Before
    @Override
    public void setup() {
        factory = createHazelcastInstanceFactory(2);
        super.setup();
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrun() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanPublisherConfig targetClusterConfig = wanConfig.getWanPublisherConfigs().get(0);
        targetClusterConfig.setQueueCapacity(10);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION);
        initCluster(basicCluster, configA, factory);
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrunIfReplicationActive() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanPublisherConfig targetClusterConfig = wanConfig.getWanPublisherConfigs().get(0);
        targetClusterConfig.setQueueCapacity(10);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE);
        initCluster(basicCluster, configA, factory);
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test
    public void testExceptionOnQueueOverrunIfReplicationPassive() throws ExecutionException, InterruptedException {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanPublisherConfig targetClusterConfig = wanConfig.getWanPublisherConfigs().get(0);
        targetClusterConfig.setQueueCapacity(10);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE);
        initCluster(basicCluster, configA, factory);
        for (HazelcastInstance instance : basicCluster) {
            ChangeWanStateOperation changeWanStateOperation = new ChangeWanStateOperation("atob",
                    configB.getGroupConfig().getName(), false);
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
        WanPublisherConfig targetClusterConfig = wanConfig.getWanPublisherConfigs().get(0);
        Map<String, Comparable> properties = targetClusterConfig.getProperties();
        properties.put(WanReplicationProperties.BATCH_SIZE.key(), "500");
        properties.put(WanReplicationProperties.BATCH_MAX_DELAY_MILLIS.key(), 1000);
        properties.put(WanReplicationProperties.RESPONSE_TIMEOUT_MILLIS.key(), "500");
        initCluster(basicCluster, propTestConfig, factory);
        createDataIn(basicCluster, "map", 0, 1000);
    }

    @Test
    public void testClearQueues() {
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName());
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanPublisherConfig targetClusterConfig = wanConfig.getWanPublisherConfigs().get(0);
        targetClusterConfig.setQueueCapacity(1000);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.DISCARD_AFTER_MUTATION);
        initCluster(singleNodeA, configA, factory);
        pauseWanReplication(singleNodeA, "atob", configB.getGroupConfig().getName());
        createDataIn(singleNodeA, "map", 0, 1000);
        EnterpriseWanReplicationService wanReplicationService = getWanReplicationService(singleNodeA[0]);
        final WanReplicationEndpoint endpoint = wanReplicationService.getEndpoint("atob", configB.getGroupConfig().getName());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assert endpoint.getStats().getOutboundQueueSize() == 1000;
            }
        });
        wanReplicationService.clearQueues("atob", configB.getGroupConfig().getName());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assert endpoint.getStats().getOutboundQueueSize() == 0;
            }
        });
    }

    @Override
    public String getReplicationImpl() {
        return replicationImpl;
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return memoryFormat;
    }
}
