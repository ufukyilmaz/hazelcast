package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE;
import static com.hazelcast.config.EvictionPolicy.LFU;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE;
import static com.hazelcast.spi.properties.GroupProperty.HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.util.RandomPicker.getInt;
import static java.lang.String.format;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDMapConfigValidatorTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetMap_throwsIllegalArgumentException_whenNativeMemoryConfigDisabled() {
        String mapName = randomName();


        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(getInt(1, new HazelcastProperties(new Properties()).getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE)))
                .setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE);

        HotRestartConfig hotRestartConfig = new HotRestartConfig()
                .setEnabled(true);

        MapConfig mapConfig = new MapConfig(mapName)
                .setInMemoryFormat(NATIVE)
                .setMaxSizeConfig(maxSizeConfig)
                .setEvictionPolicy(LRU)
                .setHotRestartConfig(hotRestartConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Enable native memory config to use NATIVE in-memory-format for the map [" + mapName + "]");
        node.getMap(mapName);
    }

    @Test
    public void testGetMap_onNativeFormattedNearCache_throwsIllegalArgumentException_whenNativeMemoryConfigDisabled() {
        String mapName = randomMapName();

        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(LFU)
                .setMaximumSizePolicy(FREE_NATIVE_MEMORY_SIZE);

        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInMemoryFormat(NATIVE)
                .setEvictionConfig(evictionConfig);

        MapConfig mapConfig = new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Enable native memory config to use NATIVE in-memory-format for Near Cache");
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
    public void testHotRestartsEnabledMap_throwsException_when_FREE_NATIVE_MEMORY_PERCENTAGE_isSmallerThan_threshold() {
        String mapName = randomName();

        int hotRestartFreeNativeMemoryPercentage
                = new HazelcastProperties(new Properties()).getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);

        int localSizeConfig = getInt(1, hotRestartFreeNativeMemoryPercentage);

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(localSizeConfig)
                .setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE);

        HotRestartConfig hotRestartConfig = new HotRestartConfig()
                .setEnabled(true);

        MapConfig mapConfig = new MapConfig(mapName)
                .setInMemoryFormat(NATIVE)
                .setMaxSizeConfig(maxSizeConfig)
                .setEvictionPolicy(LRU)
                .setHotRestartConfig(hotRestartConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);
        config.getNativeMemoryConfig().setEnabled(true);

        HazelcastInstance node = createHazelcastInstance(config);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(format(
                "There is a global limit on the minimum free native memory, configurable by the system property %s,"
                        + " whose value is currently %d percent. The map %s has Hot Restart enabled,"
                        + " but is configured with %d percent, which is lower than the allowed minimum.",
                HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE, hotRestartFreeNativeMemoryPercentage, mapName, localSizeConfig));
        node.getMap(mapName);
    }

    private void testSupportedMapMaxSizePolicies(MaxSizePolicy maxSizePolicy) {
        testSupportedHDMapConfig(LRU, maxSizePolicy);
    }

    private void testSupportedHDMapConfig(EvictionPolicy evictionPolicy, MaxSizePolicy maxSizePolicy) {
        testSupportedMapConfig(evictionPolicy, maxSizePolicy, NATIVE);
    }

    private void testSupportedMapConfig(EvictionPolicy evictionPolicy, MaxSizePolicy maxSizePolicy,
                                        InMemoryFormat inMemoryFormat) {
        String mapName = randomMapName();

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setMaxSizePolicy(maxSizePolicy);

        MapConfig mapConfig = new MapConfig(mapName)
                .setInMemoryFormat(inMemoryFormat)
                .setEvictionPolicy(evictionPolicy)
                .setMaxSizeConfig(maxSizeConfig);

        Config config = new Config()
                .addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }
}
