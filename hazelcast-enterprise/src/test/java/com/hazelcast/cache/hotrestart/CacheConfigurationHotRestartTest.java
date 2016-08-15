package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.util.IterableUtil.getFirst;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class CacheConfigurationHotRestartTest extends AbstractCacheHotRestartTest {

    @Test
    public void givenDynamicallyCreatedCacheExist_whenClusterRestarted_thenCacheStillExists() {
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, Object> cache = createCache(hz);
        CacheConfig originalConfig = cache.getConfiguration(CacheConfig.class);

        hz = restartHazelcastInstance(hz);

        CacheConfig config = getSingleCacheConfig(hz);
        assertEquals(cacheName, config.getName());
        assertEquals(originalConfig, config);
    }

    @Test
    public void givenStaticallyCreatedCacheExist_whenClusterRestarted_thenCacheStillExists() {
        Config hzConfig = makeConfig().addCacheConfig(createSimpleCacheConfig());
        HazelcastInstance hz = newHazelcastInstance(hzConfig);
        ICache<Integer, Object> cache = hz.getCacheManager().getCache(cacheName);
        CacheConfig originalConfig = cache.getConfiguration(CacheConfig.class);

        hz = restartHazelcastInstance(hz);

        CacheConfig config = getSingleCacheConfig(hz);
        assertEquals(cacheName, config.getName());
        assertEquals(originalConfig, config);
    }

    @Test
    public void givenCacheWithWANExist_whenClusterRestarted_thenCacheStillHasWANEndpoint() {
        String wanConfigName = "name";

        Config hzConfig = createConfigWithWAN(wanConfigName);
        HazelcastInstance hz = newHazelcastInstance(hzConfig);
        ICache<Integer, Object> cache = hz.getCacheManager().getCache(cacheName);
        CacheConfig originalConfig = cache.getConfiguration(CacheConfig.class);
        String cacheNameWithPrefix = originalConfig.getNameWithPrefix();

        hz = restartHazelcastInstance(hz);

        assertCacheHasWANReplicationEnabled(hz, cacheNameWithPrefix);
    }

    private void assertCacheHasWANReplicationEnabled(HazelcastInstance hz, String cacheNameWithPrefix) {
        EnterpriseCacheService cacheService = getCacheService(hz);
        assertTrue(cacheService.isWanReplicationEnabled(cacheNameWithPrefix));
    }

    private Config createConfigWithWAN(String wanConfigName) {
        Config hzConfig = makeConfig();
        WanReplicationConfig wanConfig = new WanReplicationConfig();
        wanConfig.setName(wanConfigName);
        hzConfig.addWanReplicationConfig(wanConfig);
        hzConfig.addCacheConfig(createCacheConfigWithWAN(wanConfigName));
        return hzConfig;
    }

    private CacheSimpleConfig createSimpleCacheConfig() {
        CacheSimpleConfig simpleCacheConfig = new CacheSimpleConfig();
        simpleCacheConfig.setName(cacheName);
        simpleCacheConfig.getHotRestartConfig().setEnabled(true);
        return simpleCacheConfig;
    }


    private CacheSimpleConfig createCacheConfigWithWAN(String wanConfigName) {
        CacheSimpleConfig simpleCacheConfig = createSimpleCacheConfig();
        simpleCacheConfig.setWanReplicationRef(new WanReplicationRef(wanConfigName, "merge-policy", EMPTY_LIST, true));
        return simpleCacheConfig;
    }

    private CacheConfig getSingleCacheConfig(HazelcastInstance hz) {
        Collection<CacheConfig> cacheConfigs = getCacheConfigs(hz);
        assertEquals(1, cacheConfigs.size());
        return getFirst(cacheConfigs, null);
    }

    private Collection<CacheConfig> getCacheConfigs(HazelcastInstance hz) {
        CacheService cacheService = getCacheService(hz);
        return cacheService.getCacheConfigs();
    }

}
