package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastCachingProvider;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheStatsTest extends AbstractCacheTest {

    private HazelcastInstance instance;

    @Override
    protected void onSetup() {
        Config config = createConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
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

        CacheStatistics stats = cache.getLocalCacheStatistics();
        assertEquals(miss, stats.getCacheMisses());
        assertEquals(hit, stats.getCacheHits());
        assertEquals(puts, stats.getCachePuts());
        assertEquals(hit + miss, stats.getCacheGets());
        assertEquals(removes, stats.getCacheRemovals());
    }

    @Test
    public void testStatsMultiInstances() throws InterruptedException, ExecutionException {
        final String name = "test";
        Config config = new Config();
        config.setProperties(AbstractCacheTest.getDefaultProperties());

        config.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
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

}
