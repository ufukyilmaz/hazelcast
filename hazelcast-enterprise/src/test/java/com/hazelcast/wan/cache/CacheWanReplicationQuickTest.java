package com.hazelcast.wan.cache;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WANReplicationQueueFullException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class CacheWanReplicationQuickTest extends CacheWanReplicationTestSupport {

    private static final String BATCH_IMPL = WanBatchReplication.class.getName();

    private HazelcastInstance[] basicCluster = new HazelcastInstance[2];

    @Parameters(name = "replicationImpl:{0}, memoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BATCH_IMPL, NATIVE},
                {BATCH_IMPL, BINARY},
        });
    }

    @Parameter(0)
    public String replicationImpl;

    @Parameter(1)
    public InMemoryFormat memoryFormat;

    @BeforeClass
    public static void initJCache() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrTestUtil.cleanup();
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrun() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanPublisherConfig targetClusterConfig = wanConfig.getWanPublisherConfigs().get(0);
        targetClusterConfig.setQueueCapacity(10);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION);
        initCluster(basicCluster, configA);
        createCacheDataIn(basicCluster, DEFAULT_CACHE_NAME, 0, 1000, false);
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
