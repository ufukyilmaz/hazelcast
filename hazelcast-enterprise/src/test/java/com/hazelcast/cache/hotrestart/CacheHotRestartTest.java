package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.proxyservice.InternalProxyService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartTest extends AbstractCacheHotRestartTest {

    @Parameters(name = "memoryFormat:{0} fsync:{2} clusterSize:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, 1},
                {InMemoryFormat.BINARY, KEY_COUNT, false, false, 3},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, 1},
                {InMemoryFormat.NATIVE, KEY_COUNT, false, false, 3},
        });
    }

    @Parameter(4)
    public int clusterSize;

    private ICache<Integer, String> cache;

    @Test
    public void testPut() {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>();

        fillCache(expectedMap);

        int expectedSize = cache.size();
        resetFixture();

        assertEquals(expectedSize, cache.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String expected = expectedMap.get(key);
            assertEquals("Invalid value in cache after restart", expected, cache.get(key));
        }
    }

    @Test
    public void testRemove() {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>();
        fillCache(expectedMap);

        Random random = new Random();
        for (int i = 0; i < KEY_COUNT / 10; i++) {
            int key = random.nextInt(KEY_COUNT);
            if (cache.remove(key)) {
                expectedMap.remove(key);
            }
        }

        int expectedSize = cache.size();
        resetFixture();

        assertEquals(expectedSize, cache.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String expected = expectedMap.get(key);
            if (expected == null) {
                assertNull("Removed value found in cache after restart", cache.get(key));
            } else {
                assertEquals("Invalid value in cache after restart", expected, cache.get(key));
            }
        }
    }

    @Test
    public void testPutRemove() {
        resetFixture();

        Map<Integer, String> expectedMap = new HashMap<Integer, String>(KEY_COUNT);

        Random random = new Random();
        for (int i = 0; i < 3; i++) {
            fillCacheAndRemoveRandom(expectedMap, random);
        }

        int expectedSize = cache.size();
        resetFixture();

        assertEquals(expectedSize, cache.size());

        for (int key = 0; key < KEY_COUNT; key++) {
            String expected = expectedMap.get(key);
            if (expected == null) {
                assertNull("Removed value found in cache after restart", cache.get(key));
            } else {
                assertEquals("Invalid value in cache after restart", expected, cache.get(key));
            }
        }
    }

    @Test
    public void cacheProxy_shouldBeCreated_afterHotRestart() {
        newInstances(clusterSize);
        cache = createCache();
        String fullCacheName = cache.getPrefixedName();
        fillCache(new HashMap<Integer, String>());

        HazelcastInstance[] instances = restartInstances(clusterSize);
        for (HazelcastInstance instance : instances) {
            InternalProxyService proxyService = getNodeEngineImpl(instance).getProxyService();
            Collection<String> names = proxyService.getDistributedObjectNames(ICacheService.SERVICE_NAME);
            assertThat(names, hasItem(fullCacheName));
        }
    }

    private void fillCacheAndRemoveRandom(Map<Integer, String> expectedMap, Random random) {
        fillCache(expectedMap);

        for (int i = 0; i < KEY_COUNT / 10; i++) {
            int key = random.nextInt(KEY_COUNT);
            if (cache.remove(key)) {
                expectedMap.remove(key);
            }
        }
    }

    private void fillCache(Map<Integer, String> expectedMap) {
        for (int i = 0; i < 3; i++) {
            for (int key = 0; key < KEY_COUNT; key++) {
                String value = randomString();
                cache.put(key, value);
                expectedMap.put(key, value);
            }
        }
    }

    private void resetFixture() {
        restartInstances(clusterSize);
        cache = createCache();
    }
}
