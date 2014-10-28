package com.hazelcast.cache;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheStatsTest extends AbstractCacheTest {

    private HazelcastInstance instance;

    @Override
    protected void onSetup() {
        Config config = createConfig();
        /*
         * *** NOTE ***
         *
         * Because of local cache stats are tested in this test,
         * only one instance must be used
         */
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        instance = factory.newHazelcastInstance(config);
    }

    @Override
    protected void onTearDown() {
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    @Test
    public void testStats() throws InterruptedException, ExecutionException {
        ICache cache = createCache();

        final int PUTS = 1000;
        final int HITS = 30;
        final int MISSES = 20;
        final int REMOVES = 10;

        for (int i = 0; i < PUTS; i++) {
            cache.put(i, i);
        }

        for (int i = 0; i < HITS; i++) {
            cache.get(i);
        }

        for (int i = 0; i < MISSES; i++) {
            cache.get(i - 1000);
        }

        for (int i = 0; i < REMOVES; i++) {
            cache.remove(i);
        }

        CacheStatistics stats = cache.getLocalCacheStatistics();
        assertEquals(MISSES, stats.getCacheMisses());
        assertEquals(HITS, stats.getCacheHits());
        assertEquals(PUTS, stats.getCachePuts());
        assertEquals(HITS + MISSES, stats.getCacheGets());
        assertEquals(REMOVES, stats.getCacheRemovals());
    }

    /*
    @Test
    public void testStatsMultiInstances() throws InterruptedException, ExecutionException {
        final String name = "test";
        Config config = new Config();
        config.setProperties(getProperties());

        config.setNativeMemoryConfig(getMemoryConfig());
        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.setAllowUnsafe(true);

        CacheManager cacheManager = new HazelcastCachingProvider().getCacheManager();
        ICache cache = (ICache) cacheManager.getCache(name);

        CacheManager cacheManager2 = new HazelcastCachingProvider().getCacheManager();
        ICache cache2 = (ICache) cacheManager2.getCache(name);

        int puts = 1000;
        for (int i = 0; i < puts; i++) {
            cache.put(i, i);
        }

        int hit = 30;
        int miss = 20;
        for (int i = 0; i < hit; i++) {
            cache.get(i);
        }

        for (int i = 0; i < miss; i++) {
            cache.get(i - 1000);
        }

        int removes = 10;
        for (int i = 0; i < removes; i++) {
            cache.remove(i);
        }

        CacheStatistics stats1 = cache.getLocalCacheStatistics();
        assertEquals(miss, stats1.getCacheMisses());
        assertEquals(hit, stats1.getCacheHits());
        assertEquals(puts, stats1.getCachePuts());
        assertEquals(hit + miss, stats1.getCacheGets());
        assertEquals(removes, stats1.getCacheRemovals());

        int puts2 = 10000;
        for (int i = 0; i < puts2; i++) {
            cache2.put(i, i);
        }

        int hit2 = 300;
        int miss2 = 2000;
        for (int i = 0; i < hit2; i++) {
            cache2.get(i);
        }

        for (int i = 0; i < miss2; i++) {
            cache2.get(i - 10000);
        }

        int removes2 = 100;
        for (int i = 0; i < removes2; i++) {
            cache2.remove(i);
        }

        CacheStatistics stats2 = cache.getLocalCacheStatistics();
        assertEquals(miss2, stats2.getCacheMisses());
        assertEquals(hit2, stats2.getCacheHits());
        assertEquals(puts2, stats2.getCachePuts());
        assertEquals(hit2 + miss2, stats2.getCacheGets());
        assertEquals(removes2, stats2.getCacheRemovals());
    }
    */

}
