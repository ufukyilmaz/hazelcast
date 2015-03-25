package com.hazelcast.wan;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.merge.HigherHitCacheMergePolicy;
import com.hazelcast.cluster.SystemClockChangeTest;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheEvictionConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.XmlConfigImportVariableReplacementTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.wan.impl.WanNoDelayReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(NightlyTest.class)
public class EnterpriseCacheWanReplicationTest extends HazelcastTestSupport {

    private int ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE = 3 * 60;


    private HazelcastInstance[] clusterA = new HazelcastInstance[2];
    private HazelcastInstance[] clusterB = new HazelcastInstance[2];
    private HazelcastInstance[] clusterC = new HazelcastInstance[2];

    private Random random = new Random();

    private ClassLoader classLoaderA;
    private ClassLoader classLoaderB;
    private ClassLoader classLoaderC;

    private Config configA;
    private Config configB;
    private Config configC;

    private void initConfigA(boolean nativeMemoryEnabled) {
        classLoaderA = createCacheManagerClassLoader();
        configA = createConfig("A", "confA", 5701, classLoaderA, nativeMemoryEnabled);
    }

    private void initConfigB(boolean nativeMemoryEnabled) {
        classLoaderB = createCacheManagerClassLoader();
        configB = createConfig("B", "confB", 5801, classLoaderB, nativeMemoryEnabled);
    }

    private void initConfigC(boolean nativeMemoryEnabled) {
        classLoaderC = createCacheManagerClassLoader();
        configC = createConfig("C", "confC", 5901, classLoaderC, nativeMemoryEnabled);
    }

    private Config createConfig(String groupName, String instanceName, int port,
                                ClassLoader classLoader, boolean nativeMemoryEnabled) {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        config.getGroupConfig().setName(groupName);
        config.setInstanceName(instanceName);
        config.getNetworkConfig().setPort(port);
        config.setClassLoader(classLoader);
        config.setProperty(GroupProperties.PROP_ELASTIC_MEMORY_ENABLED, "false");
        CacheSimpleConfig cacheConfig = config.getCacheConfig("default");
        CacheEvictionConfig evictionConfig = new CacheEvictionConfig();
        if (nativeMemoryEnabled) {
            config.setNativeMemoryConfig(getMemoryConfig());
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
            evictionConfig.setSize(90);
            evictionConfig.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT);
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

    private void startClusterA() {
        initCluster(clusterA, configA);
    }

    private void startClusterB() {
        initCluster(clusterB, configB);
    }

    private void startClusterC() {
        initCluster(clusterC, configC);
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.shutdownAll();
    }


    private void initCluster(HazelcastInstance[] cluster, Config config) {
        for (int i = 0; i < cluster.length; i++) {
            config.setInstanceName(config.getInstanceName() + i);
            cluster[i] = HazelcastInstanceFactory.newHazelcastInstance(config);
        }
    }

    private HazelcastInstance getNode(HazelcastInstance[] cluster) {
        return cluster[random.nextInt(cluster.length)];
    }


    private List getClusterEndPoints(Config config, int count) {
        List ends = new ArrayList<String>();

        int port = config.getNetworkConfig().getPort();

        for (int i = 0; i < count; i++) {
            ends.add(new String("127.0.0.1:" + port++));
        }
        return ends;
    }

    private WanTargetClusterConfig targetCluster(Config config, int count) {
        WanTargetClusterConfig target = new WanTargetClusterConfig();
        target.setGroupName(config.getGroupConfig().getName());
        target.setReplicationImpl(WanNoDelayReplication.class.getName());
        target.setEndpoints(getClusterEndPoints(config, count));
        return target;
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
        CacheManager manager = provider.getCacheManager(cacheManagerName, classLoader, properties);
        CacheConfig cacheConfig;

        try {
            cacheConfig = createCacheConfig(cacheName, node, format);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ICache myCache = (ICache) manager.createCache(cacheName, cacheConfig);

        for (; start < end; start++) {
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

    protected CacheConfig createCacheConfig(String cacheName, HazelcastInstance node, InMemoryFormat inMemoryFormat) throws Exception{
        CacheConfig cacheConfig = new CacheConfig(node.getConfig().getCacheConfig(cacheName));
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setStatisticsEnabled(true);
        CacheEvictionConfig evictionConfig = new CacheEvictionConfig();
        evictionConfig.setSize(90);
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            evictionConfig.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    @Test
    public void cache_linkTopo_ActivePassiveReplication_2clusters() {
        initConfigA(false);
        initConfigB(false);
        setupReplicateFrom(configA, configB,
                clusterB.length, "atob", HigherHitCacheMergePolicy.class.getName(), "default");

        startClusterA();
        startClusterB();

        int cacheSize = createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", InMemoryFormat.BINARY, 0, 10);
        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", cacheSize);
    }

    @Test
    public void hd_cache_linkTopo_ActivePassiveReplication_2clusters() {
        initConfigA(true);
        initConfigB(true);
        setupReplicateFrom(configA, configB,
                clusterB.length, "atob", HigherHitCacheMergePolicy.class.getName(), "default");

        startClusterA();
        startClusterB();

        int cacheSize = createCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", InMemoryFormat.NATIVE, 0, 10);
        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", cacheSize);
        cacheSize = removeCacheDataIn(clusterA, classLoaderA, "my-cache-manager", "default", InMemoryFormat.NATIVE, 0, 10);
        checkCacheDataInFrom(clusterB, classLoaderB, "my-cache-manager", "default", cacheSize);

    }

}
