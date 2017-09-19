package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.util.IterableUtil.getFirst;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheConfigurationHotRestartTest extends AbstractCacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false}
        });
    }

    @Test
    public void givenDynamicallyCreatedCacheExist_whenClusterRestarted_thenCacheStillExists() {
        Config hzConfig = makeConfig(factory.nextAddress());
        HazelcastInstance hz = factory.newHazelcastInstance(hzConfig);
        ICache<Integer, Object> cache = createCache(hz);
        CacheConfig<Integer, Object> originalConfig = cache.getConfiguration(CacheConfig.class);

        hz = restartHazelcastInstance(hz, hzConfig);

        CacheConfig config = getSingleCacheConfig(hz);
        assertEquals(cacheName, config.getName());
        assertEquals(originalConfig, config);
    }

    @Test
    public void givenStaticallyCreatedCacheExist_whenClusterRestarted_thenCacheStillExists() {
        Config hzConfig = makeConfig().addCacheConfig(createSimpleCacheConfig());
        HazelcastInstance hz = newHazelcastInstance(hzConfig);
        ICache<Integer, Object> cache = hz.getCacheManager().getCache(cacheName);
        CacheConfig<Integer, Object> originalConfig = cache.getConfiguration(CacheConfig.class);

        hz = restartHazelcastInstance(hz, hzConfig);

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
        CacheConfig<Integer, Object> originalConfig = cache.getConfiguration(CacheConfig.class);
        String cacheNameWithPrefix = originalConfig.getNameWithPrefix();

        hz = restartHazelcastInstance(hz, hzConfig);

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
        simpleCacheConfig.setWanReplicationRef(new WanReplicationRef(wanConfigName, "merge-policy",
                Collections.<String>emptyList(), true));
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
