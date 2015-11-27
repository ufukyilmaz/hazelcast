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

import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDMapConfigValidatorTest extends HazelcastTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void testMapEvictionPolicy_throwsException_whenRandom() throws Exception {
        testUnsupportedMapEvictionPolicies(EvictionPolicy.RANDOM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenFreeHeapSize() throws Exception {
        testUnsupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.FREE_HEAP_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenFreeHeapPercentage() throws Exception {
        testUnsupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenUsedHeapSize() throws Exception {
        testUnsupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapMaxSizePolicy_throwsException_whenUsedHeapPercentage() throws Exception {
        testUnsupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheEvictionPolicy_throwsException_whenRandom() throws Exception {
        testUnsupportedNearCacheEvictionPolicies(EvictionPolicy.RANDOM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheEvictionPolicy_throwsException_whenNone() throws Exception {
        testUnsupportedNearCacheEvictionPolicies(EvictionPolicy.NONE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNearCacheMaxSizePolicy_throwsException_whenEntryCount() throws Exception {
        testUnsupportedNearCacheMaxSizePolicies(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
    }

    private void testUnsupportedMapEvictionPolicies(EvictionPolicy evictionPolicy) {
        testUnsupportedMapConfig(evictionPolicy, MaxSizeConfig.MaxSizePolicy.PER_NODE);
    }

    private void testUnsupportedMapMaxSizePolicies(MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        testUnsupportedMapConfig(EvictionPolicy.LRU, maxSizePolicy);
    }

    private void testUnsupportedMapConfig(EvictionPolicy evictionPolicy, MaxSizeConfig.MaxSizePolicy maxSizePolicy) {
        String mapName = randomMapName();
        Config config = new Config();
        config.getMapConfig(mapName)
                .setInMemoryFormat(NATIVE)
                .setEvictionPolicy(evictionPolicy).getMaxSizeConfig().setMaxSizePolicy(maxSizePolicy);


        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }

    private void testUnsupportedNearCacheEvictionPolicies(EvictionPolicy evictionPolicy) {
        testUnsupportedNearCacheConfig(evictionPolicy, EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    private void testUnsupportedNearCacheMaxSizePolicies(EvictionConfig.MaxSizePolicy maxSizePolicy) {
        testUnsupportedNearCacheConfig(EvictionPolicy.LRU, maxSizePolicy);
    }

    private void testUnsupportedNearCacheConfig(EvictionPolicy evictionPolicy, EvictionConfig.MaxSizePolicy maxSizePolicy) {
        String mapName = randomMapName();

        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE).getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaximumSizePolicy(maxSizePolicy);

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        node.getMap(mapName);
    }


}