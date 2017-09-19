package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class HDQueryCompatibilityTest extends HazelcastTestSupport {

    @Test
    public void assert_compat_38_and_39_NATIVE_noIndex() {
        assert_compat_38_and_39(InMemoryFormat.NATIVE);
    }

    @Test
    public void assert_compat_38_and_39_BINARY_noIndex() {
        assert_compat_38_and_39(InMemoryFormat.BINARY);
    }

    @Test
    public void assert_compat_38_and_39_OBJECT_noIndex() {
        assert_compat_38_and_39(InMemoryFormat.OBJECT);
    }

    @Test
    public void assert_compat_38_and_39_NATIVE_withIndex() {
        assert_compat_38_and_39(InMemoryFormat.NATIVE, new MapIndexConfig("power", false));
    }

    @Test
    public void assert_compat_38_and_39_BINARY_withIndex() {
        assert_compat_38_and_39(InMemoryFormat.BINARY, new MapIndexConfig("power", false));
    }

    @Test
    public void assert_compat_38_and_39_OBJECT_withIndex() {
        assert_compat_38_and_39(InMemoryFormat.OBJECT, new MapIndexConfig("power", false));
    }

    @Test
    public void assert_compat_38_and_39_NATIVE_withIndex_ordered() {
        assert_compat_38_and_39(InMemoryFormat.NATIVE, new MapIndexConfig("power", true));
    }

    @Test
    public void assert_compat_38_and_39_BINARY_withIndex_ordered() {
        assert_compat_38_and_39(InMemoryFormat.BINARY, new MapIndexConfig("power", true));
    }

    @Test
    public void assert_compat_38_and_39_OBJECT_withIndex_ordered() {
        assert_compat_38_and_39(InMemoryFormat.OBJECT, new MapIndexConfig("power", true));
    }


    public void assert_compat_38_and_39(InMemoryFormat inMemoryFormat, MapIndexConfig... indexConfigs) {
        // GIVEN CONFIG
        String[] versions = new String[]{"3.8", "3.9"};
        TestHazelcastInstanceFactory factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        Config config = getHDConfig();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setInMemoryFormat(inMemoryFormat);
        for (MapIndexConfig mic : indexConfigs) {
            mapConfig.addMapIndexConfig(mic);
        }
        config.addMapConfig(mapConfig);

        HazelcastInstance[] instances = factory.newInstances(getConfig());
        assertClusterSizeEventually(versions.length, instances[0]);

        // GIVEN VALUES
        IMap<Integer, Car> map = instances[0].getMap("default");
        for (int i = 0; i < 100; i++) {
            map.put(i, new Car(i));
        }

        // WHEN & THEN
        Collection result = map.values(Predicates.greaterEqual("power", 50));
        assertEquals(50, result.size());

        map = instances[1].getMap("default");
        result = map.values(Predicates.greaterEqual("power", 50));
        assertEquals(50, result.size());

        // TEAR-DOWN
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    public static class Car implements ICar, Serializable {

        private final long power;

        Car(long power) {
            this.power = power;
        }

        @Override
        public long getPower() {
            return power;
        }
    }

    @SuppressWarnings("unused")
    public interface ICar {

        long getPower();
    }
}
