package com.hazelcast.map.impl.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.RELEASED_VERSIONS;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class HDQueryCompatibilityTest {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public MapIndexConfig mapIndexConfig;

    private TestHazelcastInstanceFactory factory;
    private String[] versions;
    private HazelcastInstance[] instances;
    private String mapName = randomMapName();

    @Parameters(name = "inMemoryFormat:{0}, index:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE, null},
                {NATIVE, new MapIndexConfig("power", false)},
                {NATIVE, new MapIndexConfig("power", true)},
                {BINARY, null},
                {BINARY, new MapIndexConfig("power", false)},
                {BINARY, new MapIndexConfig("power", true)},
                {OBJECT, null},
                {OBJECT, new MapIndexConfig("power", false)},
                {OBJECT, new MapIndexConfig("power", true)},
        });
    }

    @Before
    public void setup() {
        // GIVEN CONFIG
        versions = new String[]{
                RELEASED_VERSIONS[0],
                CURRENT_VERSION,
        };
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        Config config = getHDConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        if (mapIndexConfig != null) {
            mapConfig.addMapIndexConfig(mapIndexConfig);
        }
        config.addMapConfig(mapConfig);

        instances = factory.newInstances(config);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testQueryCompatible_whenPreviousAndCurrentMembers() {
        assertClusterSizeEventually(versions.length, instances[0]);

        // GIVEN VALUES
        IMap<Integer, Car> map = instances[0].getMap(mapName);
        for (int i = 0; i < 100; i++) {
            map.put(i, new Car(i));
        }

        // WHEN & THEN
        Collection result = map.values(Predicates.greaterEqual("power", 50));
        assertEquals(50, result.size());

        map = instances[1].getMap(mapName);
        result = map.values(Predicates.greaterEqual("power", 50));
        assertEquals(50, result.size());
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
