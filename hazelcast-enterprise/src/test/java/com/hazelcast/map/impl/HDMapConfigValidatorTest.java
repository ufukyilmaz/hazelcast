package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.SYSPROP_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.String.format;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDMapConfigValidatorTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test_getMap_throwsIllegalArgumentException_whenNativeMemoryConfigDisabled() throws Exception {
        String mapName = randomName();
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(getInt(1, getHotRestartFreeNativeMemoryPercentage()));
        maxSizeConfig.setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE);

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setInMemoryFormat(NATIVE);
        mapConfig.getHotRestartConfig().setEnabled(true);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setEvictionPolicy(LRU);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Enable native memory config to use NATIVE in-memory-format for the map ["
                + mapConfig.getName() + "]");

        node.getMap(mapName);
    }

    @Test
    public void test_getMap_onNativeFormattedNearCache_throwsIllegalArgumentException_whenNativeMemoryConfigDisabled() {
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE).getEvictionConfig()
                .setEvictionPolicy(LFU)
                .setMaximumSizePolicy(FREE_NATIVE_MEMORY_SIZE);

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Enable native memory config to use NATIVE in-memory-format for near cache");

        node.getMap(mapName);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenFreeHeapSize() {
        testSupportedMapMaxSizePolicies(MaxSizePolicy.FREE_HEAP_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenFreeHeapPercentage() {
        testSupportedMapMaxSizePolicies(MaxSizePolicy.FREE_HEAP_PERCENTAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenUsedHeapSize() {
        testSupportedMapMaxSizePolicies(MaxSizePolicy.USED_HEAP_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenUsedHeapPercentage() {
        testSupportedMapMaxSizePolicies(USED_HEAP_PERCENTAGE);
    }

    @Test
    public void testUsedHeapPercentageMaxSizePolicy_whenInMemoryFormat_BINARY() {
        testSupportedMapConfig(LRU, USED_HEAP_PERCENTAGE, BINARY);
    }

    @Test
    public void testNearCacheEvictionPolicy_throwsException_whenRandom() {
        testSupportedNearCacheEvictionPolicies(EvictionPolicy.RANDOM);
    }

    @Test
    public void testNearCacheEvictionPolicy_throwsException_whenNone() {
        testSupportedNearCacheEvictionPolicies(EvictionPolicy.NONE);
    }

    @Test
    public void testHotRestartsEnabledMap_throwsException_when_FREE_NATIVE_MEMORY_PERCENTAGE_isSmallerThan_threshold() {
        String mapName = randomName();

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        int hotRestartFreeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();
        int localSizeConfig = getInt(1, hotRestartFreeNativeMemoryPercentage);
        maxSizeConfig.setSize(localSizeConfig);
        maxSizeConfig.setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE);

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setInMemoryFormat(NATIVE);
        mapConfig.getHotRestartConfig().setEnabled(true);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setEvictionPolicy(LRU);

        Config config = new Config();
        config.addMapConfig(mapConfig);
        config.getNativeMemoryConfig().setEnabled(true);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(format(
                "There is a global limit on the minimum free native memory, settable by the system property"
                        + " %s, whose value is currently %d percent. The map %s has Hot Restart enabled, but is configured"
                        + " with %d percent, lower than the allowed minimum.",
                SYSPROP_HOTRESTART_FREE_NATIVE_MEMORY_PERCENTAGE, hotRestartFreeNativeMemoryPercentage,
                mapConfig.getName(), localSizeConfig));

        node.getMap(mapName);
    }

    private void testSupportedMapMaxSizePolicies(MaxSizePolicy maxSizePolicy) {
        testSupportedHDMapConfig(LRU, maxSizePolicy);
    }

    private void testSupportedHDMapConfig(EvictionPolicy evictionPolicy, MaxSizePolicy maxSizePolicy) {
        testSupportedMapConfig(evictionPolicy, maxSizePolicy, NATIVE);
    }

    private void testSupportedMapConfig(EvictionPolicy evictionPolicy,
                                        MaxSizePolicy maxSizePolicy,
                                        InMemoryFormat inMemoryFormat) {
        String mapName = randomMapName();
        Config config = new Config();
        config.getMapConfig(mapName)
                .setInMemoryFormat(inMemoryFormat)
                .setEvictionPolicy(evictionPolicy).getMaxSizeConfig().setMaxSizePolicy(maxSizePolicy);

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }

    private void testSupportedNearCacheEvictionPolicies(EvictionPolicy evictionPolicy) {
        testSupportedNearCacheConfig(evictionPolicy, EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    private void testSupportedNearCacheConfig(EvictionPolicy evictionPolicy, EvictionConfig.MaxSizePolicy maxSizePolicy) {
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE).getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(maxSizePolicy);

        Config config = new Config();
        config.getNativeMemoryConfig().setEnabled(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Near-cache");

        node.getMap(mapName);
    }
}
