package com.hazelcast.wan.cache;

import com.hazelcast.cache.impl.merge.policy.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class CacheWanBatchReplicationTest extends AbstractCacheWanReplicationTest {

    @Test
    public void recoverFromConnectionFailure() {
        initConfigA();
//        configA.setProperty(GroupProperty.ENTERPRISE_WAN_REP_QUEUE_CAPACITY, "100");
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");
        initCluster(singleNodeA, configA);
        //Exceed the size of event queue
        createCacheDataIn(singleNodeA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 10000, false);
        sleepSeconds(20);
        startClusterB();//At least the last 100 should be on target
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 9900, 10000, singleNodeA);
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
