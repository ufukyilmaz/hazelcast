package com.hazelcast.map.impl.querycache;


import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class QueryCacheConfigTest extends HazelcastTestSupport {

    @Test
    public void testDifferentQueryCacheInstancesObtained_whenIMapConfiguredWithWildCard() throws Exception {

        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("cache");
        cacheConfig.getPredicateConfig().setSql("__key > 10");

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("test*");
        mapConfig.addQueryCacheConfig(cacheConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IEnterpriseMap<Integer, Integer> map1 = (IEnterpriseMap) node.getMap("test1");
        IEnterpriseMap<Integer, Integer> map2 = (IEnterpriseMap) node.getMap("test2");

        QueryCache<Integer, Integer> queryCache1 = map1.getQueryCache("cache");
        QueryCache<Integer, Integer> queryCache2 = map2.getQueryCache("cache");


        for (int i = 0; i < 20; i++) {
            map1.put(i, i);
        }

        for (int i = 0; i < 30; i++) {
            map2.put(i, i);
        }


        assertQueryCacheSizeEventually(9, queryCache1);
        assertQueryCacheSizeEventually(19, queryCache2);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache cache) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, cache.size());
            }
        });
    }
}
