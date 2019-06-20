package com.hazelcast.cache.eviction;

import com.hazelcast.cache.CacheTestSupport;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class})
public class ForcedEvictionTest extends CacheTestSupport {

    private static final int INSTANCE_COUNT = 1;

    private TestHazelcastInstanceFactory factory = getInstanceFactory(INSTANCE_COUNT);

    private HazelcastInstance hazelcastInstance;

    private TestHazelcastInstanceFactory getInstanceFactory(int instanceCount) {
        return createHazelcastInstanceFactory(instanceCount);
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
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
        LongGauge forceEvictionCount = getMetricsRegistry().newLongGauge(
                findMetricWithGivenStringAndSuffix(cache.getName(), ".forceEvictionCount"));
        assertThat(forceEvictionCount.read(), greaterThan(0L));

        // intentionally no other assert, it's enough when the test does not throw NativeOutOfMemoryError
    }

    private MetricsRegistry getMetricsRegistry() {
        return ((HazelcastInstanceProxy) hazelcastInstance).getOriginal().node.nodeEngine.getMetricsRegistry();
    }

    private String findMetricWithGivenStringAndSuffix(String string, String suffix) {
        MetricsRegistry registry = getMetricsRegistry();
        for (String name : registry.getNames()) {
            if (name.endsWith(suffix) && name.contains(string)) {
                return name;
            }
        }
        throw new IllegalArgumentException("Could not find a metric with the given suffix " + suffix);
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
