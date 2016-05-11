package com.hazelcast.cache.eviction;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class})
public class ForcedEvictionTest extends CacheTestSupport {

    private static final int INSTANCE_COUNT = 1;
    private HazelcastInstance hazelcastInstance;

    private TestHazelcastInstanceFactory factory = getInstanceFactory(INSTANCE_COUNT);

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }
    protected TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        return createHazelcastInstanceFactory(instanceCount);
    }

    @Override
    protected void onSetup() {
        Config config = createConfig();
        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            hazelcastInstances[i] = factory.newHazelcastInstance(config);
        }
        hazelcastInstance = hazelcastInstances[0];
    }

    @Override
    protected void onTearDown() {
    }

    @Test(timeout = 12000000)
    public void testForcedEviction() {
        int testDurationSeconds = 30;

        long deadLine = System.currentTimeMillis() + (testDurationSeconds * 1000);
        ICache<Integer, Integer> cache = createCache();
        for (int i = 0; System.currentTimeMillis() < deadLine; i++) {
            cache.put(i, i);
        }

        //intentionally no assert. It's enough when the test does not throw NativeOutOfMemoryError
    }

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "2");

        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(16, MemoryUnit.MEGABYTES));
        return config;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = super.createCacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        EvictionConfig evictionConfig = new EvictionConfig();

        //rely on forced eviction
        evictionConfig.setSize(100);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

}