package com.hazelcast.wan.cache;

import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.wan.replication.WanNoDelayReplication;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NightlyTest.class)
public class CacheWanNoDelayReplicationTest extends AbstractCacheWanReplicationTest {

    @Test
    public void recoverFromConnectionFailure() {
        initConfigA();
        configA.setProperty(GroupProperties.PROP_ENTERPRISE_WAN_REP_QUEUE_CAPACITY, "100");
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(), "default");
        initCluster(singleNodeA, configA);
        createCacheDataIn(singleNodeA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 200, false);
        sleepSeconds(10);
        startClusterB();
        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", 100, 200, singleNodeA);
        //One event comes from failure queue, others (between 1-100) should be dropped
        checkCacheDataSize(clusterB, classLoaderB, "my-cache-manager", "default", 101);
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
