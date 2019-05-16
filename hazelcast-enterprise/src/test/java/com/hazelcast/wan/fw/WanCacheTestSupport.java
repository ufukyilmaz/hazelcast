package com.hazelcast.wan.fw;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.DefaultTaskProgress;
import com.hazelcast.test.ProgressCheckerTask;
import com.hazelcast.test.TaskProgress;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertTrue;

public class WanCacheTestSupport {
    private static final ILogger LOGGER = Logger.getLogger(WanCacheTestSupport.class);
    private static final String CACHE_MANAGER = "cache-manager";

    public static int fillCache(HazelcastInstance instance, String cacheName, int start, int end) {
        ICache<Integer, String> cache = getOrCreateCache(instance, cacheName);

        String valuePrefix = instance.getConfig().getGroupConfig().getName();
        return fillCache(start, end, cache, valuePrefix);
    }

    public static int fillCache(Cluster cluster, String cacheName, int start, int end) {
        ICache<Integer, String> cache = getOrCreateCache(cluster, cacheName);

        String valuePrefix = cluster.config.getGroupConfig().getName();
        return fillCache(start, end, cache, valuePrefix);
    }

    public static int fillCache(int start, int end, ICache<Integer, String> cache, String valuePrefix) {
        LOGGER.info("Filling cache " + cache.getName() + " [" + start + "," + end + ")");
        for (; start < end; start++) {
            String value = valuePrefix + start;
            cache.put(start, value);
        }
        return cache.size();
    }

    public static ICache<Integer, String> getOrCreateCache(Cluster cluster, String cacheName) {
        HazelcastInstance instance = cluster.getAMember();
        return getOrCreateCache(instance, cacheName);
    }

    public static ICache<Integer, String> getOrCreateCache(HazelcastInstance instance, String cacheName) {
        ClassLoader classLoader = instance.getConfig().getClassLoader();
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider.propertiesByInstanceName(instance.getConfig().getInstanceName());
        try {
            URI cacheManagerName = new URI(CACHE_MANAGER);
            AbstractHazelcastCacheManager manager = (AbstractHazelcastCacheManager) provider.getCacheManager(cacheManagerName,
                    classLoader, properties);

            CacheConfig<Integer, String> cacheConfig = createCacheConfig(cacheName, instance);
            return manager.getOrCreateCache(cacheName, cacheConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CacheConfig<Integer, String> createCacheConfig(String cacheName, HazelcastInstance node) throws Exception {
        return new CacheConfig<Integer, String>(node.getConfig().getCacheConfig(cacheName));
    }

    public static void verifyCacheReplicated(Cluster sourceCluster, Cluster targetCluster, String cacheName) {
        final ICache<Integer, String> sourceCache = getOrCreateCache(sourceCluster, cacheName);
        final ICache<Integer, String> targetCache = getOrCreateCache(targetCluster, cacheName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(verifyCacheReplicatedInternal(sourceCache, targetCache));
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    private static boolean verifyCacheReplicatedInternal(ICache<Integer, String> source, ICache<Integer, String> target) {
        for (Cache.Entry<Integer, String> entry : source) {
            Integer key = entry.getKey();
            if (!target.containsKey(key)) {
                return false;
            }
        }

        return true;
    }

    private static class ReplicationProgressCheckerTask implements ProgressCheckerTask {
        private final ICache<Object, Object> sourceCache;
        private final ICache<Object, Object> targetCache;

        private ReplicationProgressCheckerTask(ICache<Object, Object> sourceCache, ICache<Object, Object> targetCache) {
            this.sourceCache = sourceCache;
            this.targetCache = targetCache;
        }

        @Override
        public TaskProgress checkProgress() {
            int totalKeys = 0;
            int replicatedKeys = 0;

            for (Cache.Entry<Object, Object> entry : sourceCache) {
                totalKeys++;
                Object key = entry.getKey();
                if (!targetCache.containsKey(key)) {
                    replicatedKeys++;
                }
            }

            return new DefaultTaskProgress(totalKeys, replicatedKeys);
        }
    }
}
