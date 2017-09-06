package com.hazelcast.wan.cache;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.wan.WanReplicationTestSupport;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("WeakerAccess")
public abstract class CacheWanReplicationTestSupport extends WanReplicationTestSupport {

    protected static final String DEFAULT_CACHE_MANAGER = "my-cache-manager";
    protected static final String DEFAULT_CACHE_NAME = "default";

    protected ClassLoader classLoaderA;
    protected ClassLoader classLoaderB;
    protected ClassLoader classLoaderC;

    protected void initConfigA() {
        classLoaderA = createCacheManagerClassLoader();
        configA = createConfig("A", "confA", 5701, classLoaderA, isNativeMemoryEnabled());
    }

    protected void initConfigB() {
        classLoaderB = createCacheManagerClassLoader();
        configB = createConfig("B", "confB", 5801, classLoaderB, isNativeMemoryEnabled());
    }

    protected void initConfigC() {
        classLoaderC = createCacheManagerClassLoader();
        configC = createConfig("C", "confC", 5901, classLoaderC, isNativeMemoryEnabled());
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz,
                                      String setupName, String policy, String cacheName) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, cacheName, null);
    }

    protected void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz,
                                      String setupName, String policy, String cacheName, String filter) {
        WanReplicationConfig wanConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
        }
        wanConfig.addWanPublisherConfig(targetCluster(toConfig, clusterSz));

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicy(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }

        fromConfig.addWanReplicationConfig(wanConfig);
        fromConfig.getCacheConfig(cacheName).setWanReplicationRef(wanRef);
    }

    protected int createCacheDataIn(HazelcastInstance[] cluster, ClassLoader classLoader,
                                    String cacheManager, String cacheName, InMemoryFormat format,
                                    int start, int end, boolean removeBeforePut) {
        return createCacheDataIn(cluster, classLoader, cacheManager, cacheName, format, start, end, removeBeforePut, null, false);
    }

    protected int createCacheDataIn(HazelcastInstance[] cluster, ClassLoader classLoader,
                                    String cacheManager, String cacheName, InMemoryFormat format,
                                    int start, int end, boolean removeBeforePut, ExpiryPolicy expiryPolicy) {
        return createCacheDataIn(cluster, classLoader, cacheManager, cacheName, format, start, end, removeBeforePut, expiryPolicy, false);
    }

    protected int createCacheDataIn(HazelcastInstance[] cluster, ClassLoader classLoader,
                                    String cacheManager, String cacheName, InMemoryFormat format,
                                    int start, int end, boolean removeBeforePut, ExpiryPolicy expiryPolicy,
                                    boolean usePutAll) {
        ICache<Integer, String> myCache = getOrCreateCache(cluster, cacheManager, cacheName, format, classLoader);
        HashMap<Integer, String> putAllMap = usePutAll ? new HashMap<Integer, String>() : null;

        for (; start < end; start++) {
            if (removeBeforePut) {
                myCache.remove(start);
            }
            final String value = getNode(cluster).getConfig().getGroupConfig().getName() + start;
            if (usePutAll) {
                putAllMap.put(start, value);
            } else if (expiryPolicy == null) {
                myCache.put(start, value);
            } else {
                myCache.put(start, value, expiryPolicy);
            }
        }
        if (usePutAll) {
            if (expiryPolicy == null) {
                myCache.putAll(putAllMap);
            } else {
                myCache.putAll(putAllMap, expiryPolicy);
            }
        }
        return myCache.size();
    }

    protected int removeCacheDataIn(HazelcastInstance[] cluster, ClassLoader classLoader,
                                    String cacheManager, String cacheName, InMemoryFormat format, int start, int end) {
        ICache<Integer, String> myCache = getOrCreateCache(cluster, cacheManager, cacheName, format, classLoader);
        for (; start < end; start++) {
            myCache.remove(start);
        }
        return myCache.size();
    }

    protected boolean checkCacheDataInFrom(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                           String cacheManager, final String cacheName, final int start, final int end,
                                           HazelcastInstance[] sourceCluster) {
        final CacheManager manager = getCacheManager(targetCluster, cacheManager, classLoader);
        final String sourceGroupName = getNode(sourceCluster).getConfig().getGroupConfig().getName();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(manager.getCache(cacheName));
            }
        });
        final ICache<Integer, String> cache = ((AbstractHazelcastCacheManager) manager).getCache(cacheName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = start; i < end; i++) {
                    assertEquals(sourceGroupName + i, cache.get(i));
                }
            }
        });

        return true;
    }

    protected boolean checkKeysNotIn(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                     String cacheManager, String cacheName, final int start, final int end) {
        CacheManager manager = getCacheManager(targetCluster, cacheManager, classLoader);
        final ICache<Integer, String> cache = ((AbstractHazelcastCacheManager) manager).getCache(cacheName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (int i = start; i < end; i++) {
                    assertFalse(cache.containsKey(i));
                }
            }
        });

        return true;
    }

    protected boolean checkCacheDataSize(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                         String cacheManager, final String cacheName, final int size) {
        final CacheManager manager = getCacheManager(targetCluster, cacheManager, classLoader);
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

    protected CacheConfig<Integer, String> createCacheConfig(String cacheName, HazelcastInstance node,
                                                             InMemoryFormat inMemoryFormat) throws Exception {
        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>(node.getConfig().getCacheConfig(cacheName));
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

    protected void increaseHitCount(HazelcastInstance[] targetCluster, ClassLoader classLoader,
                                    String cacheManager, final String cacheName, final int start, final int end, final int repeat) {
        final CacheManager manager = getCacheManager(targetCluster, cacheManager, classLoader);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNotNull(manager.getCache(cacheName));
            }
        });
        final ICache<Integer, String> cache = ((AbstractHazelcastCacheManager) manager).getCache(cacheName);
        for (int i = start; i < end; i++) {
            for (int j = 0; j < repeat; j++) {
                cache.get(i);
            }
        }
    }

    private Config createConfig(String groupName, String instanceName, int port, ClassLoader classLoader,
                                boolean nativeMemoryEnabled) {
        Config config = getConfig();
        config.getGroupConfig().setName(groupName);
        config.setInstanceName(instanceName);
        config.getNetworkConfig().setPort(port);
        config.setClassLoader(classLoader);
        CacheSimpleConfig cacheConfig = config.getCacheConfig(DEFAULT_CACHE_NAME);
        EvictionConfig evictionConfig = new EvictionConfig();
        if (nativeMemoryEnabled) {
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
            evictionConfig.setSize(90);
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);
        return config;
    }

    private CacheManager getCacheManager(HazelcastInstance[] targetCluster, String cacheManager, ClassLoader classLoader) {
        HazelcastInstance node = getNode(targetCluster);
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider.propertiesByInstanceName(node.getConfig().getInstanceName());
        try {
            URI cacheManagerName = new URI(cacheManager);
            return provider.getCacheManager(cacheManagerName, classLoader, properties);
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    private ICache<Integer, String> getOrCreateCache(HazelcastInstance[] cluster, String cacheManager, String cacheName,
                                                     InMemoryFormat format, ClassLoader classLoader) {
        HazelcastInstance node = getNode(cluster);
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider.propertiesByInstanceName(node.getConfig().getInstanceName());
        try {
            URI cacheManagerName = new URI(cacheManager);
            AbstractHazelcastCacheManager manager = (AbstractHazelcastCacheManager) provider.getCacheManager(cacheManagerName,
                    classLoader, properties);

            CacheConfig<Integer, String> cacheConfig = createCacheConfig(cacheName, node, format);
            return manager.getOrCreateCache(cacheName, cacheConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CacheManagerClassLoader createCacheManagerClassLoader() {
        ClassLoader currentClassLoader = getClass().getClassLoader();
        return new CacheManagerClassLoader(new URL[0], currentClassLoader);
    }

    private static class CacheManagerClassLoader extends URLClassLoader {

        CacheManagerClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

        @Override
        public String toString() {
            return "test";
        }
    }
}
