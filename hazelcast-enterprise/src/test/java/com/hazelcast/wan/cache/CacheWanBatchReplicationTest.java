package com.hazelcast.wan.cache;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import com.hazelcast.wan.cache.filter.DummyCacheWanFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public class CacheWanBatchReplicationTest extends AbstractCacheWanReplicationTest {

    @Rule
    public RuntimeAvailableProcessorsRule processorsRule = new RuntimeAvailableProcessorsRule(2);

    @Parameter
    public int maxConcurrentInvocations;

    @Parameters(name = "maxConcurrentInvocations:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {-1},
                {100},
        });
    }

    @BeforeClass
    public static void initJCache() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrTestUtil.cleanup();
    }

    @Override
    protected int getMaxConcurrentInvocations() {
        return maxConcurrentInvocations;
    }

    @Test
    public void recoverFromConnectionFailure() {
        initConfigA();
        //configA.setProperty(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY, "100");
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");
        initCluster(singleNodeA, configA);
        // exceed the size of event queue
        createCacheDataIn(singleNodeA, DEFAULT_CACHE_NAME, 0, 10000, false);
        sleepSeconds(20);
        // at least the last 100 should be on target
        startClusterB();
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 9900, 10000, singleNodeA);
    }

    @Test
    public void testCacheWanFilter() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                "default", DummyCacheWanFilter.class.getName());
        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 1, 10, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 1, 2, clusterA);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 2, 10);
    }

    @Test
    public void testMigration() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");

        initCluster(singleNodeA, configA);
        createCacheDataIn(singleNodeA, DEFAULT_CACHE_NAME, 0, 200, false);
        initCluster(singleNodeC, configA);

        initCluster(clusterB, configB);

        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 200, singleNodeA);
    }

    @Override
    public void cache_wan_events_should_be_processed_in_order() {
        assumeTrue("maxConcurrentInvocations higher than 1 does not guarantee ordering", maxConcurrentInvocations < 2);
        super.cache_wan_events_should_be_processed_in_order();
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }
}
