package com.hazelcast.internal.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class})
public class HDMapForcedEvictionTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "enterpriseMap";

    private HazelcastInstance hazelcastInstance;

    @Before
    public void onSetup() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

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

        // ".forceEvictionCount" is a dynamic metric, so we need a metric
        // collection to take place before we can read out non-zero value
        // from this gauge
        // creating it before the time-consuming part of the test to ensure
        // there is at least one metric collection before the assertion
        LongGauge forceEvictionCount = getMetricsRegistry().newLongGauge("map[" + MAP_NAME + "].forceEvictionCount");
        long deadLine = System.currentTimeMillis() + (testDurationSeconds * 1000);
        IMap<Integer, Integer> map = hazelcastInstance.getMap(MAP_NAME);
        for (int i = 0; System.currentTimeMillis() < deadLine; i++) {
            map.put(i, i);
        }

        assertThat(forceEvictionCount.read(), greaterThan(0L));

        // intentionally no other assert, it's enough when the test does not throw NativeOutOfMemoryError
    }

    private MapConfig createMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        mapConfig.setName(MAP_NAME);

        mapConfig.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.RANDOM)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(100);

        return mapConfig;
    }

    private MetricsRegistry getMetricsRegistry() {
        return ((HazelcastInstanceProxy) hazelcastInstance).getOriginal().node.nodeEngine.getMetricsRegistry();
    }

}
