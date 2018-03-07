package com.hazelcast.internal.config;

import com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDConfigValidatorTest extends HazelcastTestSupport {

    private static final String DATA_STRUCTURE_NAME = "test";

    @Test
    public void check_default_map_merge_policy_supports_NATIVE_map() {
        MapConfig mapConfig = getNativeMapConfig();

        Config config = getConfigWithHDSupport().addMapConfig(mapConfig);

        createHazelcastInstance(config).getMap(DATA_STRUCTURE_NAME);
    }

    @Test
    public void check_splitBrainMergePolicy_supports_NATIVE_map() {
        MapConfig mapConfig = getNativeMapConfig();
        mapConfig.getMergePolicyConfig().setPolicy(PutIfAbsentMergePolicy.class.getName());

        Config config = getConfigWithHDSupport().addMapConfig(mapConfig);

        createHazelcastInstance(config).getMap(DATA_STRUCTURE_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void check_legacyMapMergePolicy_throws_exception_when_NATIVE() {
        MapConfig mapConfig = getNativeMapConfig();
        mapConfig.getMergePolicyConfig().setPolicy(PutIfAbsentMapMergePolicy.class.getName());

        Config config = getConfigWithHDSupport().addMapConfig(mapConfig);

        createHazelcastInstance(config).getMap(DATA_STRUCTURE_NAME);
    }

    @Test
    public void check_default_cache_merge_policy_supports_NATIVE_map() {
        CacheSimpleConfig cacheSimpleConfig = getNativeCacheConfig();

        Config config = getConfigWithHDSupport().addCacheConfig(cacheSimpleConfig);

        createHazelcastInstance(config).getCacheManager().getCache(DATA_STRUCTURE_NAME);
    }

    private Config getConfigWithHDSupport() {
        return getHDConfig(getConfig());
    }

    @Test
    public void check_splitBrainMergePolicy_supports_NATIVE_cache() {
        CacheSimpleConfig cacheSimpleConfig = getNativeCacheConfig();
        cacheSimpleConfig.setMergePolicy(PutIfAbsentMergePolicy.class.getName());

        Config config = getConfigWithHDSupport().addCacheConfig(cacheSimpleConfig);

        createHazelcastInstance(config).getCacheManager().getCache(DATA_STRUCTURE_NAME);
    }

    @Test(expected = IllegalArgumentException.class)
    public void check_legacyCacheMergePolicy_throws_exception_when_NATIVE() {
        CacheSimpleConfig cacheSimpleConfig = getNativeCacheConfig();
        cacheSimpleConfig.setMergePolicy(PutIfAbsentCacheMergePolicy.class.getName());

        Config config = getConfigWithHDSupport().addCacheConfig(cacheSimpleConfig);

        createHazelcastInstance(config).getCacheManager().getCache(DATA_STRUCTURE_NAME);
    }

    private static MapConfig getNativeMapConfig() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);

        MapConfig mapConfig = new MapConfig().setInMemoryFormat(NATIVE);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setName(DATA_STRUCTURE_NAME);

        return mapConfig;
    }

    private static CacheSimpleConfig getNativeCacheConfig() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName(DATA_STRUCTURE_NAME);
        cacheSimpleConfig.setInMemoryFormat(NATIVE);
        cacheSimpleConfig.getEvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
        return cacheSimpleConfig;
    }
}
