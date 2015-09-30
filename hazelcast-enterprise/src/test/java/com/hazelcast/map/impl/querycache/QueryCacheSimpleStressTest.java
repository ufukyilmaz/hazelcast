package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class QueryCacheSimpleStressTest extends HazelcastTestSupport {

    private final String mapName = randomString();
    private final String cacheName = randomString();
    private final Config config = new Config();
    private final int numberOfElementsToPut = 10000;

    @Before
    public void setUp() throws Exception {

        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        evictionConfig.setSize(Integer.MAX_VALUE);
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig
                .setBufferSize(30)
                .setDelaySeconds(2)
                .setBatchSize(2)
                .setPopulate(true)
                .getPredicateConfig().setImplementation(TruePredicate.INSTANCE);
        queryCacheConfig.setEvictionConfig(evictionConfig);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        config.addMapConfig(mapConfig);
        config.setProperty(GroupProperty.ELASTIC_MEMORY_ENABLED, "false");
    }

    @Test
    public void testStress() throws Exception {
        final IEnterpriseMap<Integer, Integer> map = getMap();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfElementsToPut; i++) {
                    map.put(i, i);
                }
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                queryCache.tryRecover();
            }
        }, true);

        thread.join();

        assertQueryCacheSizeEventually(numberOfElementsToPut, queryCache);
    }

    private <K, V> IEnterpriseMap<K, V> getMap() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config);
        HazelcastInstance node = instances[0];
        return (IEnterpriseMap) node.getMap(mapName);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache queryCache) {
        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, queryCache.size());
            }
        };

        assertTrueEventually(task, 20);
    }
}
