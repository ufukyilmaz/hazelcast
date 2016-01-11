package com.hazelcast.map.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDMapConfigValidatorTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testMapEvictionPolicy_throwsException_whenRandom() throws Exception {
        testSupportedMapEvictionPolicies(EvictionPolicy.RANDOM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenFreeHeapSize() throws Exception {
        testSupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenFreeHeapPercentage() throws Exception {
        testSupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenUsedHeapSize() throws Exception {
        testSupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenUsedHeapPercentage() throws Exception {
        testSupportedMapMaxSizePolicies(USED_HEAP_PERCENTAGE);
    }

    @Test
    public void testUsedHeapPercentageMaxSizePolicy_whenInMemoryFormat_BINARY() throws Exception {
        testSupportedMapConfig(LRU, USED_HEAP_PERCENTAGE, BINARY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheEvictionPolicy_throwsException_whenRandom() throws Exception {
        testSupportedNearCacheEvictionPolicies(EvictionPolicy.RANDOM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheEvictionPolicy_throwsException_whenNone() throws Exception {
        testSupportedNearCacheEvictionPolicies(EvictionPolicy.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheMaxSizePolicy_throwsException_whenEntryCount() throws Exception {
        testSupportedNearCacheMaxSizePolicies(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
    }

    private void testSupportedMapEvictionPolicies(EvictionPolicy evictionPolicy) {
        testSupportedHDMapConfig(evictionPolicy, MaxSizeConfig.MaxSizePolicy.PER_NODE);
    }

    private void testSupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        testSupportedHDMapConfig(LRU, maxSizePolicy);
    }

    private void testSupportedHDMapConfig(EvictionPolicy evictionPolicy,
                                          MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        testSupportedMapConfig(evictionPolicy, maxSizePolicy, NATIVE);
    }

    private void testSupportedMapConfig(EvictionPolicy evictionPolicy,
                                        MaxSizeConfig.MaxSizePolicy maxSizePolicy, InMemoryFormat inMemoryFormat) {
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

    private void testSupportedNearCacheMaxSizePolicies(EvictionConfig.MaxSizePolicy maxSizePolicy) {
        testSupportedNearCacheConfig(LRU, maxSizePolicy);
    }

    private void testSupportedNearCacheConfig(EvictionPolicy evictionPolicy, EvictionConfig.MaxSizePolicy maxSizePolicy) {
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(NATIVE).getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(maxSizePolicy);

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }


}