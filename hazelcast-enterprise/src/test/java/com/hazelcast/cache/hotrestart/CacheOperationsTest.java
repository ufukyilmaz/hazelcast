package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore
public class CacheOperationsTest extends AbstractCacheHotRestartTest {

    private static final int KEY_COUNT = 1000;

    private ICache<Integer, String> cache;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {InMemoryFormat.BINARY, KEY_COUNT, false},
                {InMemoryFormat.OBJECT, KEY_COUNT, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false}
        });
    }

    @Override
    void setupInternal() {
        newHazelcastInstance();
        cache = createCache(newHazelcastInstance());
        warmupCacheAndHotRestart();
    }

    private void warmupCacheAndHotRestart() {
        for (int i = 0; i < 3; i++) {
            for (int key = 0; key < KEY_COUNT; key++) {
                cache.put(key, randomString());
            }
        }
    }

    @Test
    public void testPut() {
        String value = randomString();
        cache.put(0, value);
        cache.put(KEY_COUNT, value);

        Assert.assertEquals(value, cache.get(0));
        Assert.assertEquals(value, cache.get(KEY_COUNT));
    }

    @Test
    public void testReplace() {
        String value = randomString();
        cache.put(0, value);
        cache.put(KEY_COUNT, value);

        assertTrue(cache.replace(0, randomString()));
        assertTrue(cache.replace(KEY_COUNT, randomString()));
    }

    @Test
    public void testReplace_fail() {
        cache.remove(0);
        cache.remove(KEY_COUNT);

        assertFalse(cache.replace(0, randomString()));
        assertFalse(cache.replace(KEY_COUNT, randomString()));
    }

    @Test
    public void testReplaceIfSame() {
        String value = randomString();
        cache.put(0, value);
        cache.put(KEY_COUNT, value);

        assertTrue(cache.replace(0, value, randomString()));
        assertTrue(cache.replace(KEY_COUNT, value, randomString()));
    }

    @Test
    public void testReplaceIfSame_fail() {
        String value = randomString();
        cache.put(0, value);
        cache.put(KEY_COUNT, value);

        assertFalse(cache.replace(0, value + "X", randomString()));
        assertFalse(cache.replace(KEY_COUNT, value + "X", randomString()));
    }

    @Test
    public void testRemove() {
        String value = randomString();
        cache.put(KEY_COUNT, value);

        assertTrue(cache.remove(0));
        assertTrue(cache.remove(KEY_COUNT));

        assertNull(cache.get(0));
        assertNull(cache.get(KEY_COUNT));
    }

    @Test
    public void testRemove_fail() {
        cache.remove(0);
        cache.remove(KEY_COUNT);

        assertFalse(cache.remove(0));
        assertFalse(cache.remove(KEY_COUNT));
    }

    @Test
    public void testRemoveIfSame() {
        String value = randomString();
        cache.put(0, value);
        cache.put(KEY_COUNT, value);

        assertTrue(cache.remove(0, value));
        assertTrue(cache.remove(KEY_COUNT, value));
    }

    @Test
    public void testRemoveIfSame_fail() {
        String value = randomString();
        cache.put(0, value);
        cache.put(KEY_COUNT, value);

        assertFalse(cache.remove(0, value + "X"));
        assertFalse(cache.remove(KEY_COUNT, value + "X"));
    }

    @Test
    public void testContainsKey() {
        String value = randomString();
        cache.remove(0);
        cache.put(KEY_COUNT, value);

        assertFalse(cache.containsKey(0));
        assertTrue(cache.containsKey(KEY_COUNT));
    }

    @Test
    public void testPutIfAbsent() {
        cache.remove(0);
        cache.remove(KEY_COUNT);

        String value = randomString();
        assertTrue(cache.putIfAbsent(0, value));
        assertTrue(cache.putIfAbsent(KEY_COUNT, value));
    }

    @Test
    public void testPutIfAbsent_fail() {
        cache.put(0, randomString());
        cache.put(KEY_COUNT, randomString());

        String value = randomString();
        assertFalse(cache.putIfAbsent(0, value));
        assertFalse(cache.putIfAbsent(KEY_COUNT, value));
    }

    @Test
    public void testPutAll() {
        cache.put(0, randomString());
        cache.put(KEY_COUNT, randomString());

        Map<Integer, String> values = new HashMap<Integer, String>();
        for (int i = 0; i < KEY_COUNT; i++) {
            values.put(i, randomString());
        }
        cache.putAll(values);

        for (int i = 0; i < KEY_COUNT; i++) {
            assertNotNull(cache.get(i));
        }
    }

    @Test
    public void testPut_withExpiry() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        cache.put(0, randomString(), expiryPolicy);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(0));
            }
        });
    }

    @Test
    public void testIterator() {
        for (int key = 0; key < KEY_COUNT; key += 10) {
            cache.remove(key);
        }

        for (Cache.Entry<Integer, String> entry : cache) {
            Assert.assertNotNull(entry.getKey());
            Assert.assertNotNull(entry.getValue());
        }
    }

    @Test
    public void testSize() {
        final int mod = 10;
        for (int key = 0; key < KEY_COUNT; key += mod) {
            cache.remove(key);
        }
        int removed = KEY_COUNT / mod;

        assertEquals(KEY_COUNT - removed, cache.size());
    }
}
