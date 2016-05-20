package com.hazelcast.wan.cache;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.merge.HigherHitsCacheMergePolicy;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import org.junit.Ignore;
import org.junit.Test;

import javax.cache.expiry.ExpiryPolicy;

public abstract class AbstractCacheWanReplicationTest extends CacheWanReplicationTestSupport {

    @Test
    public void cache_linkTopo_ActivePassiveReplication_2clusters() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB,
                clusterB.length, "atob", HigherHitsCacheMergePolicy.class.getName(), DEFAULT_CACHE_NAME);

        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        removeCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50);
        checkCacheDataSize(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0);
    }

    @Test
    public void linkTopo_ActivePassiveReplication_2clusters_withHigherHitsCacheMergePolicy() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();

        // create data in B then increase hit count
        createCacheDataIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);

        increaseHitCount(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, 1000);
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 100, false);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 50, 100, clusterA);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterB);
    }

    @Test
    public void cache_wan_events_should_be_processed_in_order() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, true);

        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }

    @Test
    public void cache_VTopo_2passiveReplica_1producer() {
        String replicaName = "multiReplica";
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configA, configC, clusterC.length, replicaName, PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startAllClusters();

        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);

        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);
        checkCacheDataInFrom(clusterC, classLoaderC, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        removeCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50);

        checkCacheDataSize(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0);
        checkCacheDataSize(clusterC, classLoaderC, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withHigherHitsCacheMergePolicy() {
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitsCacheMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitsCacheMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        startAllClusters();

        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);
        checkCacheDataInFrom(clusterC, classLoaderC, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        createCacheDataIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);
        checkCacheDataInFrom(clusterC, classLoaderC, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        increaseHitCount(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, 1000);
        createCacheDataIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);

        checkCacheDataInFrom(clusterC, classLoaderC, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterB);
    }

    @Test
    public void cache_replicationRing() {
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configC, configA, clusterA.length, "ctoa", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startAllClusters();

        createCacheDataIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);

        checkCacheDataInFrom(clusterC, classLoaderC, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterB);
        checkCacheDataInFrom(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterB);
    }

    @Test
    public void createEntryWithExpiry() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(10000, 10000, 10000);
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false,
                expiryPolicy);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);
        sleepSeconds(20);
        checkKeysNotIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50);
    }

    @Test
    public void testPauseResume() {
        initConfigA();
        initConfigB();

        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);

        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        pauseWanReplication(clusterA, "atob", configB.getGroupConfig().getName());

        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 50, 100, false);
        checkKeysNotIn(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 50, 100);

        resumeWanReplication(clusterA, "atob", configB.getGroupConfig().getName());
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }

    @Test
    @Ignore // useless, cache wan events are now separate from internal cache events
    public void updateExpiredEntry() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughCacheMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(10000, 10000, 10000);
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false,
                expiryPolicy);
        sleepSeconds(20);
        createCacheDataIn(clusterA, classLoaderA, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, getMemoryFormat(), 0, 50, false,
                expiryPolicy);
        checkCacheDataInFrom(clusterB, classLoaderB, DEFAULT_CACHE_MANAGER, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }
}
