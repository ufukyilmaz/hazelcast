package com.hazelcast.wan.cache;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.WanBatchReplicationPublisherConfig;
import com.hazelcast.config.WanConsumerConfig;
import com.hazelcast.config.WanPublisherState;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.merge.HigherHitsMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.wan.custom.CustomWanConsumer;
import org.junit.Ignore;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public abstract class AbstractCacheWanReplicationTest extends CacheWanReplicationTestSupport {

    @Test
    public void cache_linkTopo_ActivePassiveReplication_2clusters_putAll() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB,
                clusterB.length, "atob", HigherHitsMergePolicy.class.getName(), DEFAULT_CACHE_NAME);

        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false, null, true);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        removeCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50);
        checkCacheDataSize(clusterB, DEFAULT_CACHE_NAME, 0);
    }

    @Test
    public void cache_linkTopo_ActivePassiveReplication_2clusters() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB,
                clusterB.length, "atob", HigherHitsMergePolicy.class.getName(), DEFAULT_CACHE_NAME);

        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        removeCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50);
        checkCacheDataSize(clusterB, DEFAULT_CACHE_NAME, 0);
    }

    @Test
    public void linkTopo_ActivePassiveReplication_2clusters_withHigherHitsCacheMergePolicy() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitsMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();

        // create data in B then increase hit count
        createCacheDataIn(clusterB, DEFAULT_CACHE_NAME, 0, 50, false);
        increaseHitCount(clusterB, DEFAULT_CACHE_NAME, 0, 50, 1000);

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 100, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 50, 100, clusterA);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterB);
    }

    @Test
    public void cache_wan_events_should_be_processed_in_order() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, true);

        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }

    @Test
    public void cache_VTopo_2passiveReplica_1producer() {
        String replicaName = "multiReplica";
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configA, configC, clusterC.length, replicaName, PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startAllClusters();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);

        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
        checkCacheDataInFrom(clusterC, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        removeCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50);

        checkCacheDataSize(clusterB, DEFAULT_CACHE_NAME, 0);
        checkCacheDataSize(clusterC, DEFAULT_CACHE_NAME, 0);
    }

    @Test
    public void VTopo_1passiveReplica_2producers_withHigherHitsCacheMergePolicy() {
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitsMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitsMergePolicy.class.getName(), DEFAULT_CACHE_NAME);
        startAllClusters();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        checkCacheDataInFrom(clusterC, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        createCacheDataIn(clusterB, DEFAULT_CACHE_NAME, 0, 50, false);
        checkCacheDataInFrom(clusterC, DEFAULT_CACHE_NAME, 0, 50, clusterA);

        increaseHitCount(clusterB, DEFAULT_CACHE_NAME, 0, 50, 1000);
        createCacheDataIn(clusterB, DEFAULT_CACHE_NAME, 0, 50, false);

        checkCacheDataInFrom(clusterC, DEFAULT_CACHE_NAME, 0, 50, clusterB);
    }

    @Test
    public void cache_replicationRing() {
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configC, configA, clusterA.length, "ctoa", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startAllClusters();

        createCacheDataIn(clusterB, DEFAULT_CACHE_NAME, 0, 50, false);

        checkCacheDataInFrom(clusterC, DEFAULT_CACHE_NAME, 0, 50, clusterB);
        checkCacheDataInFrom(clusterA, DEFAULT_CACHE_NAME, 0, 50, clusterB);
    }

    @Test
    public void testEntryWithReplicationExpiresOnTargetCluster() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(10000, 10000, 10000);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false, expiryPolicy);

        ICache<Object, Object> cache = getNode(clusterA).getCacheManager().getCache(DEFAULT_CACHE_NAME);
        assertReceivedEventCountEventually(clusterB, CacheService.SERVICE_NAME, cache.getPrefixedName(),
                0, 50, 0);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 0, 50);
    }

    @Test
    public void testEntryWithExpirationIsReplicated() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        long oneHourMillis = TimeUnit.HOURS.toMillis(1);
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(oneHourMillis, oneHourMillis, oneHourMillis);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false, expiryPolicy);

        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }

    @Test
    public void setExpiryPolicyPropagatesExpiryPolicyChangeToPassiveCluster() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1000, 1000, 1000);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
        setExpiryPolicyDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, expiryPolicy);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 0, 50);
    }

    @Test
    public void setExpiryPolicyPropagatesExpiryPolicyBothWays() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        setupReplicateFrom(configB, configA, clusterA.length, "btoa", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1000, 1000, 1000);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 100, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 100, clusterA);
        setExpiryPolicyDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, expiryPolicy);
        setExpiryPolicyDataIn(clusterB, DEFAULT_CACHE_NAME, 50, 100, expiryPolicy);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 0, 50);
        checkKeysNotIn(clusterA, DEFAULT_CACHE_NAME, 50, 100);
    }

    @Test
    public void testPauseResume() {
        initConfigA();
        initConfigB();
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);

        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        pauseWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.PAUSED);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 50, 100, false);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 50);


        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.REPLICATING);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 100, clusterA);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testStopResume() {
        initConfigA();
        initConfigB();
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);

        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        stopWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.STOPPED);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 50, 100, false);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.REPLICATING);

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 100, 200, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 100, 200, clusterA);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testPublisherInitialStateStopped() {
        initConfigA();
        initConfigB();
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);

        final WanBatchReplicationPublisherConfig pc = configA.getWanReplicationConfig(wanReplicationConfigName)
                .getBatchPublisherConfigs()
                .get(0);
        pc.setInitialPublisherState(WanPublisherState.STOPPED);

        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 50, 100, false);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.STOPPED);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);

        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.REPLICATING);

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 100, 200, false);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 100, 200, clusterA);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testPublisherInitialStatePaused() {
        initConfigA();
        initConfigB();
        final String wanReplicationConfigName = "atob";
        final String targetGroupName = configB.getGroupConfig().getName();
        setupReplicateFrom(configA, configB, clusterB.length, wanReplicationConfigName, PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);

        WanBatchReplicationPublisherConfig pc = configA.getWanReplicationConfig(wanReplicationConfigName)
                .getBatchPublisherConfigs()
                .get(0);
        pc.setInitialPublisherState(WanPublisherState.PAUSED);

        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 50, 100, false);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.PAUSED);
        checkKeysNotIn(clusterB, DEFAULT_CACHE_NAME, 50, 100);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 50);


        resumeWanReplication(clusterA, wanReplicationConfigName, targetGroupName);
        assertWanPublisherStateEventually(clusterA, wanReplicationConfigName, targetGroupName, WanPublisherState.REPLICATING);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 50, 100, clusterA);
        assertWanQueueSizesEventually(clusterA, wanReplicationConfigName, targetGroupName, 0);
    }

    @Test
    public void testProxyCreation() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        assertTrueEventually(() -> {
            Collection<DistributedObject> distributedObjects = clusterB[0].getDistributedObjects();
            assertEquals(1, distributedObjects.size());
        }, 10);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }

    @Test
    @Ignore(value = "useless, cache wan events are now separate from internal cache events")
    public void updateExpiredEntry() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);
        startClusterA();
        startClusterB();
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(10000, 10000, 10000);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false,
                expiryPolicy);
        sleepSeconds(20);
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false,
                expiryPolicy);
        checkCacheDataInFrom(clusterB, DEFAULT_CACHE_NAME, 0, 50, clusterA);
    }

    @Test
    public void replicated_data_is_not_persisted_by_default() {
        TestCacheWriter1.reset();

        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", PassThroughMergePolicy.class.getName(),
                DEFAULT_CACHE_NAME);

        CacheSimpleConfig cacheConfig = configB.getCacheConfig(DEFAULT_CACHE_NAME);
        cacheConfig.setWriteThrough(true);
        cacheConfig.setCacheWriterFactory(TestCacheWriter1.class.getName());

        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        removeCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50);

        // Ensure no put or delete operation is passed to cache writer.
        assertTrueAllTheTime(() -> {
            assertEquals(0, TestCacheWriter1.writeCount.get());
            assertEquals(0, TestCacheWriter1.deleteCount.get());
        }, 10);
    }

    @Test
    public void replicated_data_is_persisted_when_persistWanReplicatedData_is_true() {
        TestCacheWriter2.reset();

        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length,
                "atob", PassThroughMergePolicy.class.getName(), DEFAULT_CACHE_NAME);

        CacheSimpleConfig cacheConfig = configB.getCacheConfig(DEFAULT_CACHE_NAME);
        cacheConfig.setWriteThrough(true);
        cacheConfig.setCacheWriterFactory(TestCacheWriter2.class.getName());
        cacheConfig.setWanReplicationRef(getWanReplicationRefFrom(configB, true));

        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50, false);
        removeCacheDataIn(clusterA, DEFAULT_CACHE_NAME, 0, 50);

        // Ensure put or delete operation are passed to cache writer.
        assertTrueEventually(() -> {
            assertEquals(50, TestCacheWriter2.writeCount.get());
            assertEquals(50, TestCacheWriter2.deleteCount.get());
        });
    }

    private static WanReplicationRef getWanReplicationRefFrom(Config config,
                                                              boolean persistWanReplicatedData) {

        WanConsumerConfig consumerConfig = new WanConsumerConfig();
        consumerConfig.setPersistWanReplicatedData(persistWanReplicatedData);
        consumerConfig.setClassName(CustomWanConsumer.class.getName());

        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName("b");
        wanReplicationConfig.setWanConsumerConfig(consumerConfig);

        config.addWanReplicationConfig(wanReplicationConfig);

        WanReplicationRef wanReplicationRef = new WanReplicationRef();
        wanReplicationRef.setName("b");
        wanReplicationRef.setMergePolicy(PassThroughMergePolicy.class.getName());

        return wanReplicationRef;
    }

    public static class TestCacheWriter1 implements CacheWriter<Integer, Integer>,
            Serializable, Factory<CacheWriter<Integer, Integer>> {

        protected static AtomicInteger writeCount = new AtomicInteger();
        protected static AtomicInteger deleteCount = new AtomicInteger();

        public static void reset() {
            writeCount.set(0);
            deleteCount.set(0);
        }

        @Override
        public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            Thread.dumpStack();
            writeCount.incrementAndGet();
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) throws CacheWriterException {
            for (Cache.Entry<? extends Integer, ? extends Integer> entry : entries) {
                write(entry);
            }
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            Thread.dumpStack();
            deleteCount.incrementAndGet();
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            for (Object key : keys) {
                delete(key);
            }
        }

        @Override
        public CacheWriter<Integer, Integer> create() {
            return this;
        }
    }

    public static class TestCacheWriter2 implements CacheWriter<Integer, Integer>,
            Serializable, Factory<CacheWriter<Integer, Integer>> {

        protected static AtomicInteger writeCount = new AtomicInteger();
        protected static AtomicInteger deleteCount = new AtomicInteger();

        public static void reset() {
            writeCount.set(0);
            deleteCount.set(0);
        }

        @Override
        public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            writeCount.incrementAndGet();
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends Integer, ? extends Integer>> entries) throws CacheWriterException {
            for (Cache.Entry<? extends Integer, ? extends Integer> entry : entries) {
                write(entry);
            }
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            deleteCount.incrementAndGet();
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            for (Object key : keys) {
                delete(key);
            }
        }

        @Override
        public CacheWriter<Integer, Integer> create() {
            return this;
        }
    }

}
