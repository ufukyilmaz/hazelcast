package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapOperationsTest extends AbstractMapHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.OBJECT, KEY_COUNT, false, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, false},
                {InMemoryFormat.OBJECT, KEY_COUNT, false, false, true},
        });
    }

    private IMap<Integer, String> map;

    @Override
    protected void setupMapInternal() {
        map = createMap(newHazelcastInstance());
        warmupCacheAndHotRestart();
    }

    private void warmupCacheAndHotRestart() {
        for (int i = 0; i < 3; i++) {
            for (int key = 0; key < KEY_COUNT; key++) {
                map.put(key, randomString());
            }
        }
    }

    @Test
    public void testPut() {
        String value = randomString();
        map.put(0, value);
        map.put(KEY_COUNT, value);

        Assert.assertEquals(value, map.get(0));
        Assert.assertEquals(value, map.get(KEY_COUNT));
    }

    @Test
    public void testReplace() {
        String value = randomString();
        map.put(0, value);
        map.put(KEY_COUNT, value);

        assertEquals(value, map.replace(0, randomString()));
        assertEquals(value, map.replace(KEY_COUNT, randomString()));
    }

    @Test
    public void testReplace_fail() {
        map.remove(0);
        map.remove(KEY_COUNT);

        assertNull(map.replace(0, randomString()));
        assertNull(map.replace(KEY_COUNT, randomString()));
    }

    @Test
    public void testReplaceIfSame() {
        String value = randomString();
        map.put(0, value);
        map.put(KEY_COUNT, value);

        assertTrue(map.replace(0, value, randomString()));
        assertTrue(map.replace(KEY_COUNT, value, randomString()));
    }

    @Test
    public void testReplaceIfSame_fail() {
        String value = randomString();
        map.put(0, value);
        map.put(KEY_COUNT, value);

        assertFalse(map.replace(0, value + "X", randomString()));
        assertFalse(map.replace(KEY_COUNT, value + "X", randomString()));
    }

    @Test
    public void testRemove() {
        String value = randomString();
        map.put(KEY_COUNT, value);

        assertNotNull(map.remove(0));
        assertNotNull(map.remove(KEY_COUNT));

        assertNull(map.get(0));
        assertNull(map.get(KEY_COUNT));
    }

    @Test
    public void testRemove_fail() {
        map.remove(0);
        map.remove(KEY_COUNT);

        assertNull(map.remove(0));
        assertNull(map.remove(KEY_COUNT));
    }

    @Test
    public void testRemoveIfSame() {
        String value = randomString();
        map.put(0, value);
        map.put(KEY_COUNT, value);

        assertTrue(map.remove(0, value));
        assertTrue(map.remove(KEY_COUNT, value));
    }

    @Test
    public void testRemoveIfSame_fail() {
        String value = randomString();
        map.put(0, value);
        map.put(KEY_COUNT, value);

        assertFalse(map.remove(0, value + "X"));
        assertFalse(map.remove(KEY_COUNT, value + "X"));
    }

    @Test
    public void testContainsKey() {
        String value = randomString();
        map.remove(0);
        map.put(KEY_COUNT, value);

        assertFalse(map.containsKey(0));
        assertTrue(map.containsKey(KEY_COUNT));
    }

    @Test
    public void testPutIfAbsent() {
        map.remove(0);
        map.remove(KEY_COUNT);

        String value = randomString();
        assertNull(map.putIfAbsent(0, value));
        assertNull(map.putIfAbsent(KEY_COUNT, value));
    }

    @Test
    public void testPutIfAbsent_fail() {
        map.put(0, randomString());
        map.put(KEY_COUNT, randomString());

        String value = randomString();
        assertNotNull(map.putIfAbsent(0, value));
        assertNotNull(map.putIfAbsent(KEY_COUNT, value));
    }

    @Test
    public void testPut_withExpiry() {
        map.put(0, randomString(), 1, TimeUnit.MILLISECONDS);

        assertTrueEventually(() -> assertNull(map.get(0)));
    }

    @Test
    public void testIterator() {
        for (int key = 0; key < KEY_COUNT; key += 10) {
            map.remove(key);
        }

        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            Assert.assertNotNull(entry.getKey());
            Assert.assertNotNull(entry.getValue());
        }
    }

    @Test
    public void testSize() {
        int mod = 10;
        for (int key = 0; key < KEY_COUNT; key += mod) {
            map.remove(key);
        }
        int removed = KEY_COUNT / mod;

        assertEquals(KEY_COUNT - removed, map.size());
    }

    @Test
    public void testExecuteOnKey() {
        map.remove(0);

        Object value = map.executeOnKey(0, Entry::getValue);
        assertNull(value);
    }

    @Test
    public void testGetEntryView() {
        map.remove(0);

        assertNull(map.getEntryView(0));
    }

    @Test
    public void testGetAll() {
        map.remove(0);

        HashSet<Integer> keys = new HashSet<>();
        keys.add(0);

        Map<Integer, String> results = map.getAll(keys);
        assertEquals(0, results.size());
    }

    @Test
    public void testEvict() {
        map.remove(0);

        assertFalse(map.evict(0));
    }
}
