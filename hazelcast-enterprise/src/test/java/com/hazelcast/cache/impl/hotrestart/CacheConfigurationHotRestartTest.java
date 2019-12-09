package com.hazelcast.cache.impl.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.internal.util.IterableUtil.getFirst;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheConfigurationHotRestartTest extends AbstractCacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, true},
        });
    }

    @Test
    public void givenDynamicallyCreatedCacheExist_whenClusterRestarted_thenCacheStillExists() {
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, Object> cache = createCache(hz);
        CacheConfig<Integer, Object> originalConfig = getConfiguration(cache);

        hz = restartInstances(1)[0];

        CacheConfig config = getSingleCacheConfig(hz);
        assertEquals(cacheName, config.getName());
        assertEquals(originalConfig, config);
    }

    @Test
    public void givenStaticallyCreatedCacheExist_whenClusterRestarted_thenCacheStillExists() {
        Config hzConfig = makeConfig().addCacheConfig(createSimpleCacheConfig());
        HazelcastInstance hz = newHazelcastInstance(hzConfig);
        ICache<Integer, Object> cache = hz.getCacheManager().getCache(cacheName);
        CacheConfig<Integer, Object> originalConfig = getConfiguration(cache);

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
        CacheConfig<Integer, Object> originalConfig = getConfiguration(cache);
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
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig();
        wanReplicationConfig.setName(wanConfigName);
        hzConfig.addWanReplicationConfig(wanReplicationConfig);
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
