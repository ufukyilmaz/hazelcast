package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class QueryCacheGuaranteesTest extends HazelcastTestSupport {

    @Test
    public void continuesToReceiveEvents_afterNodeJoins() throws Exception {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT, "271");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(100);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);

        HazelcastInstance node = instanceFactory.newHazelcastInstance(config);
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) node.getMap(mapName);

        final QueryCache<Integer, Integer> queryCache
                = map.getQueryCache(queryCacheName, new SqlPredicate("this > 20"), true);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }

        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map2 = node2.getMap(mapName);

        for (int i = 100; i < 120; i++) {
            map2.put(i, i);
        }

        HazelcastInstance node3 = instanceFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map3 = node3.getMap(mapName);

        for (int i = 150; i < 157; i++) {
            map3.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(36, queryCache.size());
            }
        });
    }

    @Test
    public void continuesToReceiveEvents_afterNodeShutdown() throws Exception {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);
        Config config = new Config();

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(100);
        queryCacheConfig.setDelaySeconds(6);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);

        HazelcastInstance node = instanceFactory.newHazelcastInstance(config);
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) node.getMap(mapName);

        final QueryCache<Integer, Integer> queryCache
                = map.getQueryCache(queryCacheName, new SqlPredicate("this > 20"), true);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }

        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map2 = node2.getMap(mapName);

        for (int i = 200; i < 220; i++) {
            map2.put(i, i);
        }

        HazelcastInstance node3 = instanceFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> map3 = node3.getMap(mapName);

        for (int i = 350; i < 357; i++) {
            map3.put(i, i);
        }

        node3.shutdown();

        for (int i = 220; i < 227; i++) {
            map2.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(43, queryCache.size());
            }
        });
    }
}
