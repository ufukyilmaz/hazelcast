package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
public class MapHotRestartTest extends AbstractMapHotRestartTest {

    private static final int KEY_COUNT = 1000;
    @Parameterized.Parameter(3)
    public int clusterSize;
    private IMap<Integer, String> map;

    @Parameterized.Parameters(name = "memoryFormat:{0},clusterSize:{3}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false, 1},
                {InMemoryFormat.BINARY, KEY_COUNT, false, 3},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, 1},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, 3}
        });
    }

    @Test
    public void testPut() throws Exception {
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
    public void testRemove() throws Exception {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillMap(expectedMap);

        Random random = new Random();
        for (int i = 0; i < KEY_COUNT / 10; i++) {
            final int key = random.nextInt(KEY_COUNT);
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
    public void testPutRemove() throws Exception {
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

    private void fillMapAndRemoveRandom(Map<Integer, String> expectedMap, Random random) {
        fillMap(expectedMap);

        for (int i = 0; i < KEY_COUNT / 10; i++) {
            final int key = random.nextInt(KEY_COUNT);
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

    private void resetFixture() throws Exception {
        restartInstances(clusterSize);
        map = createMap();
    }
}
