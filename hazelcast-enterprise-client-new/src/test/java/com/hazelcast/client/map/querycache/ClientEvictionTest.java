package com.hazelcast.client.map.querycache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

@Ignore
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientEvictionTest extends HazelcastTestSupport {

    @BeforeClass
    public static void setUp() throws Exception {
        Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMaxSizeEvictionWorks() throws Exception {
        final int maxSize = 1000;
        final int populationCount = 5000;

        String mapName = randomString();
        String cacheName = randomString();

        ClientConfig clientConfig = new ClientConfig();

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        EvictionConfig evictionConfig = cacheConfig.getEvictionConfig();
        evictionConfig.setSize(maxSize);
        evictionConfig.setEvictionPolicy(EvictionPolicy.LFU);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);

        clientConfig.addQueryCacheConfig(mapName, cacheConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IEnterpriseMap<Integer, Integer> map = (IEnterpriseMap) client.getMap(mapName);

        // expecting at least populationCount - maxSize + 10 evicted entries according to max size.
        // 10 states an error margin since eviction does not sweep precise number of entries.
        int margin = 10;
        final CountDownLatch evictedCount = new CountDownLatch(populationCount - maxSize - margin);
        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TruePredicate.INSTANCE, true);
        cache.addEntryListener(new EntryEvictedListener() {
            @Override
            public void entryEvicted(EntryEvent event) {
                evictedCount.countDown();
            }
        }, false);


        for (int i = 0; i < populationCount; i++) {
            map.put(i, i);
        }

        assertOpenEventually(evictedCount);
        assertQueryCacheEvicted(maxSize, margin, cache);

    }

    private void assertQueryCacheEvicted(int maxSize, int margin, QueryCache<Integer, Integer> cache) {
        int size = cache.size();
        assertTrue("cache size = " + size + ", should be smaller than max size = " + maxSize, size < maxSize + margin);
    }


    private <K, V> IMap<K, V> getMap(Config config, String mapName) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        HazelcastInstance node = instances[0];
        return node.getMap(mapName);
    }
}
