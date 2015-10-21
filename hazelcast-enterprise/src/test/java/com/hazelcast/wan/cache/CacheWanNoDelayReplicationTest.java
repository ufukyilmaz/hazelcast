package com.hazelcast.wan.cache;

import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SlowTest.class)
public class CacheWanNoDelayReplicationTest extends AbstractCacheWanReplicationTest {

    @Test
    public void recoverFromConnectionFailure() {
        initConfigA();
//        configA.setProperty(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY, "100");
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");
        initCluster(singleNodeA, configA);
        createCacheDataIn(singleNodeA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 200, false);
        sleepSeconds(10);
        startClusterB();
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 200, singleNodeA);
        //One event comes from failure queue, others (between 1-100) should be dropped
        checkCacheDataSize(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 200);
    }

    @Override
    public String getReplicationImpl() {
        return WanNoDelayReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }
}
