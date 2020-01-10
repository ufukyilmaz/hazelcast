package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.Config;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.memory.MemoryUnit.KILOBYTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDNearCacheNativeOOMETest extends HazelcastTestSupport {

    private IMap mainMap;
    private HDNearCache mainNearCache;

    private IMap otherMap;
    private HDNearCache otherNearCache;

    private HazelcastInstance node;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setInvalidateOnChange(false)
                .setCacheLocalEntries(true)
                .getEvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        MapConfig mapConfig = new MapConfig("test*");
        mapConfig.setNearCacheConfig(nearCacheConfig);

        Config config = getHDConfig(smallInstanceConfig(), STANDARD, new MemorySize(512, KILOBYTES));
        config.addMapConfig(mapConfig);

        node = createHazelcastInstance(config);

        mainMap = node.getMap("test-main");
        mainNearCache = ((HDNearCache) ((NearCachedMapProxyImpl) mainMap).getNearCache());

        otherMap = node.getMap("test-other");
        otherNearCache = ((HDNearCache) ((NearCachedMapProxyImpl) otherMap).getNearCache());
    }

    @After
    public void tearDown() {
        node.getLifecycleService().shutdown();
    }

    @Test
    public void putWithoutNativeOOME() {
        mainMap.set(1, new byte[1024]);
        mainMap.get(1);

        int size = mainNearCache.size();
        assertEquals("size: " + size, 1, size);

        long evictions = mainNearCache.getNearCacheStats().getEvictions();
        assertEquals("eviction count: " + evictions, 0, evictions);

        long forcedEvictionCount = mainNearCache.getForcedEvictionCount();
        assertEquals("forced eviction count: " + forcedEvictionCount, 0, forcedEvictionCount);
    }

    @Test
    public void putWithNativeOOME_withSuccessfulEvictionOnSameNearCache() {
        for (int i = 0; i < 1024; i++) {
            mainMap.set(i, new byte[1024]);
            mainMap.get(i);
        }

        int size = mainNearCache.size();
        assertTrue("size: " + size, size > 0);

        long evictions = mainNearCache.getNearCacheStats().getEvictions();
        assertTrue("eviction count: " + evictions, evictions > 0);

        long forcedEvictionCount = mainNearCache.getForcedEvictionCount();
        assertTrue("forced eviction count: " + forcedEvictionCount, forcedEvictionCount > 0);
    }

    @Test
    public void putWithNativeOOME_withSuccessfulEvictionOnOtherNearCache() {
        // 1. first populate other map's near cache.
        for (int i = 0; i < 1024; i++) {
            otherMap.set(i, new byte[1024]);
            otherMap.get(i);
        }

        // 2. then populate main map's near cache.
        for (int i = 0; i < 1024; i++) {
            mainMap.set(i, new byte[1024]);
            mainMap.get(i);
        }


        long forcedEvictionCount = otherNearCache.getForcedEvictionCount();
        assertTrue("forced eviction count: " + forcedEvictionCount, forcedEvictionCount > 0);
    }

    @Test
    public void putWithNativeOOME_withUnsuccessfulEvictionOnOtherNearCache() {

    }

    @Test
    public void putWithNativeOOME_noCandidatesForEviction_afterSuccessfulCompaction() {

    }

    @Test
    public void putWithNativeOOME_removeKey_afterUnsuccessfulMemoryCompaction() {

    }
}
