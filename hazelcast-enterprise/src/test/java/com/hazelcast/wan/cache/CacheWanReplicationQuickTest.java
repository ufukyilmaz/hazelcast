package com.hazelcast.wan.cache;

import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WANQueueFullBehavior;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParameterizedTestRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import com.hazelcast.wan.WANReplicationQueueFullException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunParallel
@RunWith(EnterpriseParameterizedTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheWanReplicationQuickTest extends CacheWanReplicationTestSupport {

    private static final String NO_DELAY_IMPL = WanNoDelayReplication.class.getName();
    private static final String BATCH_IMPL = WanBatchReplication.class.getName();


    HazelcastInstance[] basicCluster = new HazelcastInstance[2];
    TestHazelcastInstanceFactory factory;

    @Parameterized.Parameters(name = "replicationImpl:{0},memoryFormat:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {NO_DELAY_IMPL, NATIVE},
                {NO_DELAY_IMPL, BINARY},
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
    }

    @Test(expected = WANReplicationQueueFullException.class)
    public void testExceptionOnQueueOverrun() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        WanReplicationConfig wanConfig = configA.getWanReplicationConfig("atob");
        WanTargetClusterConfig targetClusterConfig = wanConfig.getTargetClusterConfigs().get(0);
        targetClusterConfig.setQueueCapacity(10);
        targetClusterConfig.setQueueFullBehavior(WANQueueFullBehavior.THROW_EXCEPTION);
        initCluster(basicCluster, configA, factory);
        createCacheDataIn(basicCluster, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 1000, false);
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
