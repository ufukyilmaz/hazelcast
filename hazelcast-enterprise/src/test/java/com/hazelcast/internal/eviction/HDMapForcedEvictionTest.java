package com.hazelcast.internal.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class})
public class HDMapForcedEvictionTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "enterpriseMap";

    private HazelcastInstance hazelcastInstance;

    @Before
    public void onSetup() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "2");

        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(16, MemoryUnit.MEGABYTES));

        config.addMapConfig(createMapConfig());
        hazelcastInstance = createHazelcastInstance(config);
    }

    @After
    public void onTearDown() {
        hazelcastInstance.shutdown();
    }

    @Test(timeout = 12000000)
    public void testForcedEviction() {
        int testDurationSeconds = 30;

        long deadLine = System.currentTimeMillis() + (testDurationSeconds * 1000);
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);
        for (int i = 0; System.currentTimeMillis() < deadLine; i++) {
            map.put(i, i);
        }
        LongGauge forceEvictionCount = getMetricsRegistry().newLongGauge(
                findMetricWithGivenStringAndSuffix(MAP_NAME, ".forceEvictionCount"));
        assertThat(forceEvictionCount.read(), greaterThan(0L));

        // intentionally no other assert, it's enough when the test does not throw NativeOutOfMemoryError
    }

    private MapConfig createMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.setName(MAP_NAME);

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setSize(100);
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        mapConfig.setMaxSizeConfig(maxSizeConfig);
        mapConfig.setEvictionPolicy(EvictionPolicy.RANDOM);
        return mapConfig;
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
}
