package com.hazelcast.client.quorum;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.quorum.NativeMemPartitionedCluster;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;

import java.io.IOException;

public class HiDensityClientCacheQuorumTestSupport
        extends HazelcastTestSupport {

    private static String hazelcastElasticMemoryEnabledProperty;
    private static final String elasticMemoryPropertyString = "hazelcast.elastic.memory.enabled";

    static NativeMemPartitionedCluster cluster;

    protected static final String CACHE_NAME_PREFIX = "cacheQuorum";
    protected static final String QUORUM_ID = "threeNodeQuorumRule";

    protected static HazelcastClientCachingProvider cachingProvider1;
    protected static HazelcastClientCachingProvider cachingProvider2;
    protected static HazelcastClientCachingProvider cachingProvider3;
    protected static HazelcastClientCachingProvider cachingProvider4;
    protected static HazelcastClientCachingProvider cachingProvider5;
    protected static HazelcastInstance c1;
    protected static HazelcastInstance c2;
    protected static HazelcastInstance c3;
    protected static HazelcastInstance c4;
    protected static HazelcastInstance c5;
    protected static ICache<Integer, String> cache1;
    protected static ICache<Integer, String> cache2;
    protected static ICache<Integer, String> cache3;
    protected static ICache<Integer, String> cache4;
    protected static ICache<Integer, String> cache5;

    protected static TestHazelcastFactory factory;

    public static void initialize(QuorumType quorumType) throws InterruptedException {
        //As we create 5 instances, elastic memory causes out of memory error. We do not need elastic memory in this test.
        hazelcastElasticMemoryEnabledProperty = System.getProperty(elasticMemoryPropertyString);
        System.setProperty(elasticMemoryPropertyString, "false");

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
        factory = new TestHazelcastFactory();
        cluster = new NativeMemPartitionedCluster(factory).createFiveMemberCluster(cacheConfig, quorumConfig);
        initializeClients();
        initializeCaches();
        cluster.splitFiveMembersThreeAndTwo();
    }

    private static void initializeClients() {
        c1 = factory.newHazelcastClient(getClientConfig(cluster.h1));
        c2 = factory.newHazelcastClient(getClientConfig(cluster.h2));
        c3 = factory.newHazelcastClient(getClientConfig(cluster.h3));
        c4 = factory.newHazelcastClient(getClientConfig(cluster.h4));
        c5 = factory.newHazelcastClient(getClientConfig(cluster.h5));
    }

    private static void initializeCaches() {
        cachingProvider1 = HazelcastClientCachingProvider.createCachingProvider(c1);
        cachingProvider2 = HazelcastClientCachingProvider.createCachingProvider(c2);
        cachingProvider3 = HazelcastClientCachingProvider.createCachingProvider(c3);
        cachingProvider4 = HazelcastClientCachingProvider.createCachingProvider(c4);
        cachingProvider5 = HazelcastClientCachingProvider.createCachingProvider(c5);

        final String cacheName = CACHE_NAME_PREFIX + randomString();
        cache1 = (ICache)cachingProvider1.getCacheManager().getCache(cacheName);
        cache2 = (ICache)cachingProvider2.getCacheManager().getCache(cacheName);
        cache3 = (ICache)cachingProvider3.getCacheManager().getCache(cacheName);
        cache4 = (ICache)cachingProvider4.getCacheManager().getCache(cacheName);
        cache5 = (ICache)cachingProvider5.getCacheManager().getCache(cacheName);
    }

    private static ClientConfig getClientConfig(HazelcastInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = getNode(instance).address;
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        clientConfig.getGroupConfig().setName(instance.getConfig().getGroupConfig().getName());
        return clientConfig;
    }

    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        factory.terminateAll();
        if (hazelcastElasticMemoryEnabledProperty != null ) {
            System.setProperty(elasticMemoryPropertyString, hazelcastElasticMemoryEnabledProperty);
        } else {
            System.clearProperty(elasticMemoryPropertyString);
        }
    }

}
