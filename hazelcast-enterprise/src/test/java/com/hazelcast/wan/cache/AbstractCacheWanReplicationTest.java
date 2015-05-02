package com.hazelcast.wan.cache;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.merge.CachePassThroughMergePolicy;
import com.hazelcast.cache.merge.HigherHitCacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.AssertTask;
import com.hazelcast.wan.AbstractWanReplicationTest;
import org.junit.Test;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractCacheWanReplicationTest extends AbstractWanReplicationTest {

    private ClassLoader classLoaderA;
    private ClassLoader classLoaderB;
    private ClassLoader classLoaderC;

    public abstract InMemoryFormat getMemoryFormat();

    public boolean isNativeMemoryEnabled() {
        return getMemoryFormat() == InMemoryFormat.NATIVE;
    }

    private void initConfigA() {
        classLoaderA = createCacheManagerClassLoader();
        configA = createConfig("A", "confA", 5701, classLoaderA, isNativeMemoryEnabled());
    }

    private void initConfigB() {
        classLoaderB = createCacheManagerClassLoader();
        configB = createConfig("B", "confB", 5801, classLoaderB, isNativeMemoryEnabled());
    }

    private void initConfigC() {
        classLoaderC = createCacheManagerClassLoader();
        configC = createConfig("C", "confC", 5901, classLoaderC, isNativeMemoryEnabled());
    }

    private Config createConfig(String groupName, String instanceName, int port,
                                ClassLoader classLoader, boolean nativeMemoryEnabled) {
        Config config = getConfig();
        config.getGroupConfig().setName(groupName);
        config.setInstanceName(instanceName);
        config.getNetworkConfig().setPort(port);
        config.setClassLoader(classLoader);
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "false");
        CacheSimpleConfig cacheConfig = config.getCacheConfig("default");
        EvictionConfig evictionConfig = new EvictionConfig();
        if (nativeMemoryEnabled) {
            config.setNativeMemoryConfig(getMemoryConfig());
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
            evictionConfig.setSize(90);
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);
        return config;
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(256, MemoryUnit.MEGABYTES);
        return
                new NativeMemoryConfig()
                        .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                        .setSize(memorySize).setEnabled(true);
    }

    private void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz, String setupName, String policy, String cacheName) {
        WanReplicationConfig wanConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
        }
        wanConfig.addTargetClusterConfig(targetCluster(toConfig, clusterSz));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicy(policy);

        fromConfig.addWanReplicationConfig(wanConfig);
        fromConfig.getCacheConfig(cacheName).setWanReplicationRef(wanRef);
    }

    private CacheManagerClassLoader createCacheManagerClassLoader() {
        URLClassLoader currentClassLoader = (URLClassLoader) getClass().getClassLoader();
        return new CacheManagerClassLoader(currentClassLoader.getURLs(), currentClassLoader);
    }

    private class CacheManagerClassLoader extends URLClassLoader {

        public CacheManagerClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

    }

    private int createCacheDataIn(HazelcastInstance[] cluster, ClassLoader classLoader,
                                  String cacheManager, String cacheName, InMemoryFormat format, int start, int end,boolean removeBeforePut) {

        HazelcastInstance node = getNode(cluster);
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider
                .propertiesByInstanceName(node.getConfig().getInstanceName());
        URI cacheManagerName;
        try {
            cacheManagerName = new URI(cacheManager);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        AbstractHazelcastCacheManager manager = (AbstractHazelcastCacheManager) provider.getCacheManager(cacheManagerName, classLoader, properties);
        CacheConfig cacheConfig;

        try {
            cacheConfig = createCacheConfig(cacheName, node, format);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ICache myCache = manager.getOrCreateCache(cacheName, cacheConfig);

        for (; start < end; start++) {
            if(removeBeforePut) myCache.remove(start);
            myCache.put(start, node.getConfig().getGroupConfig().getName() + start);
        }
        return myCache.size();
    }

    private int removeCacheDataIn(HazelcastInstance[] cluster, ClassLoader classLoader,
                                  String cacheManager, String cacheName, InMemoryFormat format, int start, int end) {

        HazelcastInstance node = getNode(cluster);
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider
                .propertiesByInstanceName(node.getConfig().getInstanceName());
        URI cacheManagerName;
        try {
            cacheManagerName = new URI(cacheManager);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        AbstractHazelcastCacheManager manager = (AbstractHazelcastCacheManager) provider.getCacheManager(cacheManagerName, classLoader, properties);
        CacheConfig cacheConfig;

        try {
            cacheConfig = createCacheConfig(cacheName, node, format);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ICache myCache = manager.getOrCreateCache(cacheName, cacheConfig);

        for (; start < end; start++) {
            myCache.remove(start);
        }
        return myCache.size();
    }

    private boolean checkCacheDataInFrom(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                         String cacheManager, final String cacheName, final int start, final int end, HazelcastInstance[] sourceCluster) {

        HazelcastInstance node = getNode(targetCluster);
        final String sourceGroupName = getNode(sourceCluster).getConfig().getGroupConfig().getName();
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider
                .propertiesByInstanceName(node.getConfig().getInstanceName());
        URI cacheManagerName;
        try {
            cacheManagerName = new URI(cacheManager);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        final CacheManager manager = provider.getCacheManager(cacheManagerName, classLoader, properties);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(manager.getCache(cacheName));
            }
        });
        final ICache cache = (ICache) manager.getCache(cacheName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = start; i < end; i++)
                    assertEquals(cache.get(i), sourceGroupName + i);
            }
        });

        return true;
    }

    private boolean checkCacheDataSize(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                       String cacheManager, final String cacheName, final int size) {

        HazelcastInstance node = getNode(targetCluster);
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider
                .propertiesByInstanceName(node.getConfig().getInstanceName());
        URI cacheManagerName;
        try {
            cacheManagerName = new URI(cacheManager);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        final CacheManager manager = provider.getCacheManager(cacheManagerName, classLoader, properties);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(manager.getCache(cacheName));
            }
        });
        final ICache cache = (ICache) manager.getCache(cacheName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(size, cache.size());
            }
        });

        return true;
    }

    protected CacheConfig createCacheConfig(String cacheName, HazelcastInstance node, InMemoryFormat inMemoryFormat) throws Exception {
        CacheConfig cacheConfig = new CacheConfig(node.getConfig().getCacheConfig(cacheName));
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setStatisticsEnabled(true);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setSize(90);
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    private void increaseHitCount(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                  String cacheManager, final String cacheName, final int start, final int end, final int repeat) {
        HazelcastInstance node = getNode(targetCluster);
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider
                .propertiesByInstanceName(node.getConfig().getInstanceName());
        URI cacheManagerName;
        try {
            cacheManagerName = new URI(cacheManager);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }

        final CacheManager manager = provider.getCacheManager(cacheManagerName, classLoader, properties);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(manager.getCache(cacheName));
            }
        });
        final ICache cache = (ICache) manager.getCache(cacheName);
        for (int i = start; i < end; i++)
            for (int j = 0; j < repeat; j++) cache.get(i);
    }

    @Test
    public void cache_linkTopo_ActivePassiveReplication_2clusters() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB,
                clusterB.length, "atob", HigherHitCacheMergePolicy.class.getName(), "default");

        startClusterA();
        startClusterB();
        createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);
        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", 0, 50, clusterA);

        removeCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50);
        checkCacheDataSize(clusterB, classLoaderB, "my-cache-manager", "default", 0);

    }

    @Test
    public void linkTopo_ActivePassiveReplication_2clusters_Test_HigherHitsCacheMergePolicy() {

        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", HigherHitCacheMergePolicy.class.getName(), "default");
        startClusterA();
        startClusterB();

        // create data in B then increase hit count
        createCacheDataIn(clusterB, classLoaderB, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);


        increaseHitCount(clusterB, classLoaderB, "my-cache-manager", "default", 0, 50, 1000);
        createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);
        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", 0, 50, clusterB);
    }


    @Test
    public void cache_wan_events_should_be_processed_in_order() {
        initConfigA();
        initConfigB();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", CachePassThroughMergePolicy.class.getName(), "default");
        startClusterA();
        startClusterB();

        createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);
        createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50,true);

        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", 0, 50, clusterA);
    }


    @Test
    public void cache_VTopo_2passiveReplicar_1producer_Test() {

        String replicaName = "multiReplica";
        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, CachePassThroughMergePolicy.class.getName(), "default");
        setupReplicateFrom(configA, configC, clusterC.length, replicaName, CachePassThroughMergePolicy.class.getName(), "default");
        startAllClusters();


        int cacheSize = createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);

        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", 0, 50, clusterA);
        checkCacheDataInFrom(clusterC, classLoaderC, "my-cache-manager", "default", 0, 50, clusterA);


        removeCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50);

        checkCacheDataSize(clusterB, classLoaderB, "my-cache-manager", "default", 0);
        checkCacheDataSize(clusterC, classLoaderC, "my-cache-manager", "default", 0);

    }

    @Test
    public void VTopo_1passiveReplicar_2producers_Test_HigherHitsCacheMergePolicy() {

        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configC, clusterC.length, "atoc", HigherHitCacheMergePolicy.class.getName(), "default");
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", HigherHitCacheMergePolicy.class.getName(), "default");
        startAllClusters();

        createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);
        checkCacheDataInFrom(clusterC, classLoaderC, "my-cache-manager", "default", 0, 50, clusterA);

        createCacheDataIn(clusterB, classLoaderB, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);
        checkCacheDataInFrom(clusterC, classLoaderC, "my-cache-manager", "default", 0, 50, clusterA);

        increaseHitCount(clusterB, classLoaderB, "my-cache-manager", "default", 0, 50, 1000);
        createCacheDataIn(clusterB, classLoaderB, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);


        checkCacheDataInFrom(clusterC, classLoaderC, "my-cache-manager", "default", 0, 50, clusterB);
    }


    @Test
    public void cache_replicationRing() throws InterruptedException {

        initConfigA();
        initConfigB();
        initConfigC();
        setupReplicateFrom(configA, configB, clusterB.length, "atob", CachePassThroughMergePolicy.class.getName(), "default");
        setupReplicateFrom(configB, configC, clusterC.length, "btoc", CachePassThroughMergePolicy.class.getName(), "default");
        setupReplicateFrom(configC, configA, clusterA.length, "ctoa", CachePassThroughMergePolicy.class.getName(), "default");
        startAllClusters();

        int cacheSize = createCacheDataIn(clusterB, classLoaderB, "my-cache-manager", "default", getMemoryFormat(), 0, 50,false);

        checkCacheDataInFrom(clusterC, classLoaderC, "my-cache-manager", "default", 0, 50, clusterB);
        checkCacheDataInFrom(clusterA, classLoaderA, "my-cache-manager", "default", 0, 50, clusterB);

    }


}
