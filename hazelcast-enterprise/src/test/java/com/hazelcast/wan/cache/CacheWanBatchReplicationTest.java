package com.hazelcast.wan.cache;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.cache.filter.DummyCacheWanFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class CacheWanBatchReplicationTest extends AbstractCacheWanReplicationTest {

    @BeforeClass
    public static void initJCache() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrTestUtil.cleanup();
    }

    @Test
    public void recoverFromConnectionFailure() {
        initConfigA();
        //configA.setProperty(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY, "100");
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");
        initCluster(singleNodeA, configA);
        // exceed the size of event queue
        createCacheDataIn(singleNodeA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 10000, false);
        sleepSeconds(20);
        // at least the last 100 should be on target
        startClusterB();
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 9900, 10000, singleNodeA);
    }

    @Test
    public void testCacheWanFilter() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                "default", DummyCacheWanFilter.class.getName());
        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 1, 10, false);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 1, 2, clusterA);
        checkKeysNotIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 2, 10);
    }

    @Test
    public void testMigration() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");

        initCluster(singleNodeA, configA);
        createCacheDataIn(singleNodeA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 200, false);
        initCluster(singleNodeC, configA);

        initCluster(clusterB, configB);

        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 200, singleNodeA);
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
