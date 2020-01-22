package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDNearCacheNativeOOMETest extends HazelcastTestSupport {

    private static String EVICTION_DISABLED_NEAR_CACHED_MAP = "map-with-eviction-disabled-near-cache";
    private static String EVICTION_ENABLED_NEAR_CACHED_MAP = "map-with-near-cache-eviction-enabled";

    /**
     * This is needed to hold HD near cache segment size small.
     */
    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(1);

    @Test
    public void putWithoutNativeOOME() {
        HazelcastInstance node = createNodeWithMemorySize(new MemorySize(512, KILOBYTES));

        IMap mapWithNearCache = node.getMap(EVICTION_ENABLED_NEAR_CACHED_MAP);
        HDNearCache nearCache = ((HDNearCache) ((NearCachedMapProxyImpl) mapWithNearCache).getNearCache());

        mapWithNearCache.set(1, new byte[1024]);
        mapWithNearCache.get(1);

        int size = nearCache.size();
        assertEquals("size: " + size, 1, size);

        long evictions = nearCache.getNearCacheStats().getEvictions();
        assertEquals("eviction count: " + evictions, 0, evictions);

        long forcedEvictionCount = nearCache.getForcedEvictionCount();
        assertEquals("forced eviction count: " + forcedEvictionCount, 0, forcedEvictionCount);
    }

    @Test
    public void putWithNativeOOME_withSuccessfulEvictionOnSameNearCache() {
        HazelcastInstance node = createNodeWithMemorySize(new MemorySize(1, MEGABYTES));

        IMap mapWithNearCache = node.getMap(EVICTION_ENABLED_NEAR_CACHED_MAP);
        HDNearCache nearCache = ((HDNearCache) ((NearCachedMapProxyImpl) mapWithNearCache).getNearCache());

        for (int i = 0; i < 10000; i++) {
            mapWithNearCache.set(i, new byte[1024]);
            mapWithNearCache.get(i);
        }

        int size = nearCache.size();
        assertTrue("size: " + size, size > 0);

        long evictions = nearCache.getNearCacheStats().getEvictions();
        assertTrue("eviction count: " + evictions, evictions > 0);

        long forcedEvictionCount = nearCache.getForcedEvictionCount();
        assertTrue("forced eviction count: " + forcedEvictionCount, forcedEvictionCount > 0);
    }

    @Test
    public void putWithNativeOOME_withSuccessfulEvictionOnOtherNearCache() {
        MemorySize memorySize = new MemorySize(1, MEGABYTES);
        HazelcastInstance node = createNodeWithMemorySize(memorySize);

        IMap evictionDisabledNearCachedMap = node.getMap(EVICTION_DISABLED_NEAR_CACHED_MAP);

        IMap evictableNearCacheMap = node.getMap(EVICTION_ENABLED_NEAR_CACHED_MAP);
        HDNearCache evictableNearCache = ((HDNearCache) ((NearCachedMapProxyImpl) evictableNearCacheMap).getNearCache());

        // 1. First populate evictable map's near cache.
        evictableNearCacheMap.set(1, "value");
        evictableNearCacheMap.get(1);

        // 2. Then populate not-evictable map's near cache.
        long loop = memorySize.bytes() / 1024;
        int i = 0;
        do {
            evictionDisabledNearCachedMap.set(i, new byte[1024]);
            evictionDisabledNearCachedMap.get(i);
        } while (++i < loop);

        // 3. Expect forced eviction on evictable map's near cache
        long forcedEvictionCount = evictableNearCache.getForcedEvictionCount();
        assertTrue("forced eviction count: " + forcedEvictionCount, forcedEvictionCount > 0);
    }

    @Test
    public void putWithNativeOOME_removeKey_afterUnsuccessfulMemoryCompaction() {
        MemorySize memorySize = new MemorySize(64, KILOBYTES);
        HazelcastInstance node = createNodeWithMemorySize(memorySize);

        IMap evictionDisabledNearCacheMap = node.getMap(EVICTION_DISABLED_NEAR_CACHED_MAP);
        HDNearCache evictableNearCache = ((HDNearCache) ((NearCachedMapProxyImpl) evictionDisabledNearCacheMap).getNearCache());

        evictionDisabledNearCacheMap.set(1, new byte[1]);
        evictionDisabledNearCacheMap.get(1);

        evictionDisabledNearCacheMap.set(2, new byte[(int) memorySize.bytes()]);
        evictionDisabledNearCacheMap.get(2);

        assertEquals(0, evictableNearCache.size());
    }

    private HazelcastInstance createNodeWithMemorySize(MemorySize memorySize) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(1);

        // first map with near cache eviction
        MapConfig mapConfig = new MapConfig("map-with-near-cache*")
                .setEvictionConfig(evictionConfig);
        mapConfig.setNearCacheConfig(createNearCacheConfig());

        // second map without near cache eviction
        MapConfig evictionDisableNearCachedMap = new MapConfig("map-with-eviction-disabled-near-cache*")
                .setEvictionConfig(evictionConfig);
        evictionDisableNearCachedMap.setNearCacheConfig(createEvictionDisabledNearCacheConfig());

        Config config = getHDConfig(smallInstanceConfig(), STANDARD, memorySize);
        config.addMapConfig(mapConfig);
        config.addMapConfig(evictionDisableNearCachedMap);

        return createHazelcastInstance(config);
    }

    private static NearCacheConfig createNearCacheConfig() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setInvalidateOnChange(false)
                .setCacheLocalEntries(true)
                .getEvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);
        return nearCacheConfig;
    }

    private static NearCacheConfig createEvictionDisabledNearCacheConfig() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig();
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);
        return nearCacheConfig;
    }
}
