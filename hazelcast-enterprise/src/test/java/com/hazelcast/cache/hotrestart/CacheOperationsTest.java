package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheOperationsTest extends AbstractCacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.OBJECT, KEY_COUNT, false, false},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false},
        });
    }

    private ICache<Integer, String> cache;

    @Override
    protected void setupCacheInternal() {
        newHazelcastInstance();
        cache = createCache(newHazelcastInstance());
        warmupCacheAndHotRestart();
    }

    private void  warmupCacheAndHotRestart() {
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

        assertEquals(value, cache.get(0));
        assertEquals(value, cache.get(KEY_COUNT));
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
            public void run() {
                assertNull(cache.get(0));
            }
        });
    }

    @Test
    public void testSetExpiryPolicySingle() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        cache.put(0, randomString());
        cache.setExpiryPolicy(0, expiryPolicy);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(0));
            }
        });
    }

    @Test
    public void testSetExpiryPolicyMultiple() {
        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.MILLISECONDS);
        Set<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            cache.put(i, randomString());
            keys.add(i);
        }
        cache.setExpiryPolicy(keys, expiryPolicy);
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
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());
        }
    }

    @Test
    public void testSize() {
        int mod = 10;
        for (int key = 0; key < KEY_COUNT; key += mod) {
            cache.remove(key);
        }
        int removed = KEY_COUNT / mod;

        assertEquals(KEY_COUNT - removed, cache.size());
    }
}
