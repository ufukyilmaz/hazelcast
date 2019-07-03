package com.hazelcast.map.hotrestart;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class MapHotRestartSlowTest extends AbstractMapHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2} clusterSize:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, 1},
                {InMemoryFormat.BINARY, KEY_COUNT, true, false, 3},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, 1},
                {InMemoryFormat.NATIVE, KEY_COUNT, true, false, 3},
        });
    }

    @Parameter(4)
    public int clusterSize;

    private IMap<Integer, String> map;
    private boolean addIndex;

    @Test
    public void testPut() {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>();

        fillMap(expectedMap);

        int expectedSize = map.size();
        resetFixture();

        assertEquals(expectedSize, map.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String expected = expectedMap.get(key);
            assertEquals("Invalid value in map after restart", expected, map.get(key));
        }
    }

    @Test
    public void testRemove() {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillMap(expectedMap);

        Random random = new Random();
        for (int i = 0; i < KEY_COUNT / 10; i++) {
            int key = random.nextInt(KEY_COUNT);
            if (map.remove(key) != null) {
                expectedMap.remove(key);
            }
        }

        int expectedSize = map.size();
        resetFixture();

        assertEquals(expectedSize, map.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String expected = expectedMap.get(key);
            if (expected == null) {
                assertNull("Removed value found in map after restart", map.get(key));
            } else {
                assertEquals("Invalid value in map after restart", expected, map.get(key));
            }
        }
    }

    @Test
    public void testPutRemove() {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>(KEY_COUNT);

        Random random = new Random();
        for (int i = 0; i < 3; i++) {
            fillMapAndRemoveRandom(expectedMap, random);
        }

        int expectedSize = map.size();
        resetFixture();

        assertEquals(expectedSize, map.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String expected = expectedMap.get(key);
            if (expected == null) {
                assertNull("Removed value found in map after restart", map.get(key));
            } else {
                assertEquals("Invalid value in map after restart", expected, map.get(key));
            }
        }
    }

    @Test
    public void mapProxy_shouldBeCreated_afterHotRestart() {
        newInstances(clusterSize);
        map = createMap();
        fillMap(new HashMap<Integer, String>());

        HazelcastInstance[] instances = restartInstances(clusterSize);
        for (HazelcastInstance instance : instances) {
            InternalProxyService proxyService = getNodeEngineImpl(instance).getProxyService();
            Collection<String> names = proxyService.getDistributedObjectNames(MapService.SERVICE_NAME);
            assertThat(names, hasItem(mapName));
        }

        map = createMap();
        assertEquals(KEY_COUNT, map.size());
    }

    @Test
    public void mapProxy_shouldBeCreated_afterHotRestart_withIndex() {
        addIndex = true;
        mapProxy_shouldBeCreated_afterHotRestart();
    }

    @Test
    public void testKeySet_emptyMap_issue1270() {
        resetFixture();

        // hr-store created on non-partition thread
        Set result = map.keySet();
        assertEquals(0, result.size());

        // verify hr-store worked properly
        map.put(1, "value");
        assertEquals(1, map.size());
        resetFixture();
        assertEquals("value", map.get(1));
    }

    @Test
    public void testQuery_emptyMap_issue1270() {
        resetFixture();

        // hr-store created on non-partition thread
        Collection result = map.values(Predicates.alwaysTrue());
        assertEquals(0, result.size());

        // verify hr-store worked properly
        map.put(1, "value");
        assertEquals(1, map.size());
        resetFixture();
        assertEquals("value", map.get(1));
    }

    @Test
    public void testAggregation_emptyMap_issue1270() {
        resetFixture();

        // hr-store created on non-partition thread
        String result = map.aggregate(Aggregators.<Map.Entry<Integer, String>, String>comparableMax());
        assertNull(result);

        // verify hr-store worked properly
        map.put(1, "value");
        assertEquals(1, map.size());
        resetFixture();
        assertEquals("value", map.get(1));
    }

    private void fillMapAndRemoveRandom(Map<Integer, String> expectedMap, Random random) {
        fillMap(expectedMap);

        for (int i = 0; i < KEY_COUNT / 10; i++) {
            int key = random.nextInt(KEY_COUNT);
            if (map.remove(key) != null) {
                expectedMap.remove(key);
            }
        }
    }

    private void fillMap(Map<Integer, String> expectedMap) {
        for (int i = 0; i < 3; i++) {
            for (int key = 0; key < KEY_COUNT; key++) {
                String value = randomString();
                map.put(key, value);
                expectedMap.put(key, value);
            }
        }
    }

    private void resetFixture() {
        restartInstances(clusterSize);
        map = createMap();
    }

    @Override
    Config makeConfig(int backupCount) {
        Config config = super.makeConfig(backupCount);
        MapConfig mapConfig = config.getMapConfig(mapName);
        if (addIndex) {
            mapConfig.addMapIndexConfig(new MapIndexConfig(QueryConstants.THIS_ATTRIBUTE_NAME.value(), false));
        }
        return config;
    }
}
