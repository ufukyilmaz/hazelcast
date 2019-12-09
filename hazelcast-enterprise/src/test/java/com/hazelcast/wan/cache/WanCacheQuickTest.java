package com.hazelcast.wan.cache;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanQueueFullException;
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
public class WanCacheQuickTest extends WanCacheTestSupport {

    private HazelcastInstance[] basicCluster = new HazelcastInstance[2];

    @Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE},
                {BINARY},
        });
    }

    @Parameter(0)
    public InMemoryFormat memoryFormat;

    @BeforeClass
    public static void initJCache() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrTestUtil.cleanup();
    }

    @Test(expected = WanQueueFullException.class)
    public void testExceptionOnQueueOverrun() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        WanReplicationConfig wanReplicationConfig = configA.getWanReplicationConfig("atob");
        WanBatchPublisherConfig pc = wanReplicationConfig.getBatchPublisherConfigs().get(0);
        pc.setQueueCapacity(10)
                .setQueueFullBehavior(WanQueueFullBehavior.THROW_EXCEPTION);
        initCluster(basicCluster, configA);
        createCacheDataIn(basicCluster, DEFAULT_CACHE_NAME, 0, 1000, false);
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return memoryFormat;
    }
}
