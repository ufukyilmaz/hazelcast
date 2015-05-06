package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class IEnterpriseMapTest extends HazelcastTestSupport {

    private final Config config = new Config();

    @Test
    public void test_getQueryCache() throws Exception {
        String cacheName = randomString();
        String mapName = randomString();

        initConfig(mapName, cacheName);

        HazelcastInstance node = createHazelcastInstance(config);
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) node.getMap(mapName);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName);

        for (int i = 1000; i < 1007; i++) {
            map.put(i, i);
        }

        assertQueryCacheSizeEventually(7, queryCache);
    }


    @Test
    public void test_getQueryCache_isNull_withoutPredefinedConfig() throws Exception {
        String cacheName = randomString();
        String mapName = randomString();

        HazelcastInstance node = createHazelcastInstance();
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) node.getMap(mapName);
        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName);

        assertNull("QueryCache should be null without any QueryCacheConfig", queryCache);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task, 5);
    }


    private Config initConfig(String mapName, String cacheName) {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig
                .setPopulate(false)
                .getPredicateConfig().setImplementation(TruePredicate.INSTANCE);

        return addConfig(mapName, queryCacheConfig);
    }

    private Config addConfig(String mapName, QueryCacheConfig queryCacheConfig) {
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);

        return config.addMapConfig(mapConfig);
    }

}