package com.hazelcast.internal.config;

import com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.config.ConfigValidator.checkCacheConfig;
import static com.hazelcast.internal.config.ConfigValidator.checkMapConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDConfigValidatorTest {

    @Test
    public void check_default_map_merge_policy_supports_NATIVE_map() {
        MapConfig mapConfig = getNativeMapConfig();

        checkMapConfig(mapConfig);
    }

    @Test
    public void check_splitBrainMergePolicy_supports_NATIVE_map() {
        MapConfig mapConfig = getNativeMapConfig();
        mapConfig.getMergePolicyConfig().setPolicy(PutIfAbsentMergePolicy.class.getName());

        checkMapConfig(mapConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void check_legacyMapMergePolicy_throws_exception_when_NATIVE() {
        MapConfig mapConfig = getNativeMapConfig();
        mapConfig.getMergePolicyConfig().setPolicy(PutIfAbsentMapMergePolicy.class.getName());

        checkMapConfig(mapConfig);
    }

    @Test
    public void check_default_cache_merge_policy_supports_NATIVE_map() {
        CacheSimpleConfig cacheSimpleConfig = getNativeCacheConfig();

        checkCacheConfig(cacheSimpleConfig);
    }

    @Test
    public void check_splitBrainMergePolicy_supports_NATIVE_cache() {
        CacheSimpleConfig cacheSimpleConfig = getNativeCacheConfig();
        cacheSimpleConfig.setMergePolicy(PutIfAbsentMergePolicy.class.getName());

        checkCacheConfig(cacheSimpleConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void check_legacyCacheMergePolicy_throws_exception_when_NATIVE() {
        CacheSimpleConfig cacheSimpleConfig = getNativeCacheConfig();
        cacheSimpleConfig.setMergePolicy(PutIfAbsentCacheMergePolicy.class.getName());

        checkCacheConfig(cacheSimpleConfig);
    }

    private static MapConfig getNativeMapConfig() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);

        MapConfig mapConfig = new MapConfig().setInMemoryFormat(NATIVE);
        mapConfig.setMaxSizeConfig(maxSizeConfig);

        return mapConfig;
    }

    private static CacheSimpleConfig getNativeCacheConfig() {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setInMemoryFormat(NATIVE);
        cacheSimpleConfig.getEvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
        return cacheSimpleConfig;
    }
}
