package com.hazelcast.client.map.querycache;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IFunction;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientQueryCacheTest extends HazelcastTestSupport {

    @BeforeClass
    public static void setUp() throws Exception {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testQueryCache_whenIncludeValueEnabled() throws Exception {
        boolean includeValue = true;
        testQueryCache(includeValue);
    }

    @Test
    public void testQueryCache_whenIncludeValueDisabled() throws Exception {
        boolean includeValue = false;
        testQueryCache(includeValue);
    }

    @Test
    public void testQueryCache_whenInitialPopulationEnabled() throws Exception {
        boolean enableInitialPopulation = true;
        int numberOfElementsToBePutToIMap = 1000;
        int expectedSizeOfQueryCache = numberOfElementsToBePutToIMap;

        testWithInitialPopulation(enableInitialPopulation,
                expectedSizeOfQueryCache, numberOfElementsToBePutToIMap);
    }

    @Test
    public void testQueryCache_whenInitialPopulationDisabled() throws Exception {
        boolean enableInitialPopulation = false;
        int numberOfElementsToBePutToIMap = 1000;
        int expectedSizeOfQueryCache = 0;

        testWithInitialPopulation(enableInitialPopulation,
                expectedSizeOfQueryCache, numberOfElementsToBePutToIMap);
    }


    @Test
    public void testQueryCache_withLocalListener() throws Exception {
        String mapName = randomString();
        String queryCacheName = randomString();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) client.getMap(mapName);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }
        final AtomicInteger countAddEvent = new AtomicInteger();
        final AtomicInteger countRemoveEvent = new AtomicInteger();

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(queryCacheName, new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                countAddEvent.incrementAndGet();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                countRemoveEvent.incrementAndGet();
            }
        }, new SqlPredicate("this > 20"), true);


        for (int i = 0; i < 30; i++) {
            map.remove(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, queryCache.size());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Count of add events wrong!", 9, countAddEvent.get());
            }
        });
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("Count of remove events wrong!", 9, countRemoveEvent.get());
            }
        });
    }


    @Test
    public void testQueryCacheCleared_afterCalling_IMap_evictAll() throws Exception {
        String cacheName = randomString();
        final IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) getMap();
        final QueryCache<Integer, Integer> queryCache
                = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        IFunction evictAll = new IFunction() {
            @Override
            public Object apply(Object ignored) {
                map.evictAll();
                return null;
            }
        };

        assertQueryCacheSizeEventually(0, evictAll, queryCache);
    }

    private void assertQueryCacheSizeEventually(final int expected, final IFunction function, final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                if (function != null) {
                    function.apply(null);
                }
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task);
    }

    private IEnterpriseMap<Integer, Integer> getMap() {
        String mapName = randomString();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        return (IEnterpriseMap) client.getMap(mapName);
    }


    @Test
    public void testQueryCacheCleared_afterCalling_IMap_clear() throws Exception {
        String cacheName = randomString();
        final IEnterpriseMap<Integer, Integer> map = getMap();
        final QueryCache<Integer, Integer> queryCache
                = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        IFunction clear = new IFunction() {

            @Override
            public Object apply(Object ignored) {
                map.clear();
                return null;
            }
        };

        assertQueryCacheSizeEventually(0, clear, queryCache);
    }

    @Test
    public void testDestroy_emptiesQueryCache() throws Exception {
        String cacheName = randomString();
        final IEnterpriseMap<Integer, Integer> map = getMap();
        final QueryCache<Integer, Integer> queryCache
                = map.getQueryCache(cacheName, TruePredicate.INSTANCE, false);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        queryCache.destroy();

        int size = queryCache.size();

        assertEquals(0, size);

    }

    private void testWithInitialPopulation(boolean enableInitialPopulation,
                                           int expectedSize, int numberOfElementsToPut) {
        String mapName = randomString();
        String cacheName = randomName();

        ClientConfig config = getConfig(cacheName, enableInitialPopulation, mapName);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) client.getMap(mapName);
        for (int i = 0; i < numberOfElementsToPut; i++) {
            map.put(i, i);
        }
        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);

        assertEquals(expectedSize, queryCache.size());
    }

    private void testQueryCache(boolean includeValue) {
        String mapName = randomString();
        String queryCacheName = randomString();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) client.getMap(mapName);

        for (int i = 0; i < 50; i++) {
            map.put(i, i);
        }
        Predicate predicate = new SqlPredicate("this > 5 AND this < 100");
        QueryCache<Integer, Integer> cache = map.getQueryCache(queryCacheName, predicate, includeValue);

        for (int i = 50; i < 100; i++) {
            map.put(i, i);
        }

        int expected = 94;
        assertQueryCacheSize(expected, cache);
    }

    private ClientConfig getConfig(String queryCacheName, boolean enableInitialPopulation, String mapName) {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig
                .setPopulate(enableInitialPopulation)
                .getPredicateConfig().setImplementation(TruePredicate.INSTANCE);

        return addConfig(queryCacheConfig, mapName);
    }

    private ClientConfig addConfig(QueryCacheConfig queryCacheConfig, String mapName) {
        ClientConfig clientConfig = new ClientConfig();
        Map<String, Map<String, QueryCacheConfig>> queryCacheConfigs = clientConfig.getQueryCacheConfigs();
        Map<String, QueryCacheConfig> queryCacheConfigMap = queryCacheConfigs.get(mapName);
        if (queryCacheConfigMap == null) {
            queryCacheConfigMap = new HashMap<String, QueryCacheConfig>();
            queryCacheConfigs.put(mapName, queryCacheConfigMap);
        }
        queryCacheConfigMap.put(queryCacheConfig.getName(), queryCacheConfig);
        return clientConfig;
    }

    private void assertQueryCacheSize(final int expected, final QueryCache cache) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, cache.size());
            }
        }, 20);
    }
}
