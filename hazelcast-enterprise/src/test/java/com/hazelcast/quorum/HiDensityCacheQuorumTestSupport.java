package com.hazelcast.quorum;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;

public class HiDensityCacheQuorumTestSupport extends HazelcastTestSupport {

    private static String hazelcastElasticMemoryEnabledProperty;

    static NativeMemPartitionedCluster cluster;

    protected static final String CACHE_NAME_PREFIX = "cacheQuorum";
    protected static final String QUORUM_ID = "threeNodeQuorumRule";

    protected static HazelcastServerCachingProvider cachingProvider1;
    protected static HazelcastServerCachingProvider cachingProvider2;
    protected static HazelcastServerCachingProvider cachingProvider3;
    protected static HazelcastServerCachingProvider cachingProvider4;
    protected static HazelcastServerCachingProvider cachingProvider5;
    protected ICache<Integer, String> cache1;
    protected ICache<Integer, String> cache2;
    protected ICache<Integer, String> cache3;
    protected ICache<Integer, String> cache4;
    protected ICache<Integer, String> cache5;

    protected static void initialize(QuorumType quorumType) throws InterruptedException {
        //As we create 5 instances, elastic memory causes out of memory error. We do not need elastic memory in this test.
        hazelcastElasticMemoryEnabledProperty = System.getProperty("hazelcast.elastic.memory.enabled");
        System.setProperty("hazelcast.elastic.memory.enabled", "false");

        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setName(QUORUM_ID);
        quorumConfig.setType(quorumType);
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);

        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(CACHE_NAME_PREFIX + "*");
        cacheConfig.setQuorumName(QUORUM_ID);
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99));
        cluster = new NativeMemPartitionedCluster(new TestHazelcastInstanceFactory()).partitionFiveMembersThreeAndTwo(cacheConfig, quorumConfig);
        cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(cluster.h1);
        cachingProvider2 = HazelcastServerCachingProvider.createCachingProvider(cluster.h2);
        cachingProvider3 = HazelcastServerCachingProvider.createCachingProvider(cluster.h3);
        cachingProvider4 = HazelcastServerCachingProvider.createCachingProvider(cluster.h4);
        cachingProvider5 = HazelcastServerCachingProvider.createCachingProvider(cluster.h5);
    }

    @Before
    public void setUp() throws Exception {
        final String cacheName = CACHE_NAME_PREFIX + randomString();
        cache1 = (ICache)cachingProvider1.getCacheManager().getCache(cacheName);
        cache2 = (ICache)cachingProvider2.getCacheManager().getCache(cacheName);
        cache3 = (ICache)cachingProvider3.getCacheManager().getCache(cacheName);
        cache4 = (ICache)cachingProvider4.getCacheManager().getCache(cacheName);
        cache5 = (ICache)cachingProvider5.getCacheManager().getCache(cacheName);
    }

    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
        System.setProperty("hazelcast.elastic.memory.enabled", hazelcastElasticMemoryEnabledProperty);
    }
}
