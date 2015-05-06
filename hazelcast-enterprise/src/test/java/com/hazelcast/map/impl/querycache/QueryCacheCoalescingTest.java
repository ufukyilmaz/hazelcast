package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class QueryCacheCoalescingTest extends HazelcastTestSupport {

    @Test
    public void testCoalescingModeWorks() throws Exception {
        final int populationCount = 500;

        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HazelcastInstance node = createHazelcastInstance(config);
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) node.getMap(mapName);

        final CountDownLatch addCount = new CountDownLatch(1);
        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addEntryListener(new EntryAddedListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                addCount.countDown();
            }
        }, false);

        for (int i = 0; i < populationCount; i++) {
            map.put(0, i);
        }

        assertOpenEventually(addCount, 10);
    }

    private Config getConfig(String mapName, String cacheName) {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        cacheConfig.setCoalesce(true);
        cacheConfig.setBatchSize(64);
        cacheConfig.setBufferSize(64);
        cacheConfig.setDelaySeconds(60);
        mapConfig.addQueryCacheConfig(cacheConfig);
        return config;
    }

}
