package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanReplicationProperties;
import com.hazelcast.internal.management.operation.ChangeWanStateOperation;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.wan.WANReplicationQueueFullException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class})
public class MapWanReplicationQuickTest extends MapWanReplicationTestSupport {

    private static final String BATCH_IMPL = WanBatchReplication.class.getName();

    private HazelcastInstance[] basicCluster = new HazelcastInstance[2];
    private TestHazelcastInstanceFactory factory;

    @Parameterized.Parameters(name = "replicationImpl:{0},memoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {BATCH_IMPL, NATIVE},
                {BATCH_IMPL, BINARY}
        });
    }

    @Parameterized.Parameter(0)
    public String replicationImpl;

    @Parameterized.Parameter(1)
    public InMemoryFormat memoryFormat;

    @Before
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

    @Override
    public String getReplicationImpl() {
        return replicationImpl;
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return memoryFormat;
    }
}
