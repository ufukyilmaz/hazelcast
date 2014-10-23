package com.hazelcast.cache;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheStatsTest {

    @After
    public void shutdown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testStats() throws InterruptedException, ExecutionException {
        final String name = "test";
        Config config = new Config();
        config.setProperties(AbstractCacheTest.getDefaultProperties());

        config.setNativeMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.setAllowUnsafe(true);
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        CacheManager cacheManager = new HazelcastCachingProvider(instance).getCacheManager();
        ICache cache = (ICache) cacheManager.getCache(name);
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

        CacheStats stats = cache.getStats();
        assertEquals(miss, stats.getMisses());
        assertEquals(hit, stats.getHits());
        assertEquals(puts, stats.getPuts());
        assertEquals(hit + miss, stats.getGets());
        assertEquals(removes, stats.getRemoves());
        assertEquals(puts - removes, stats.getOwnedEntryCount());

    }

    @Test
    public void testStatsMultiInstances() throws InterruptedException, ExecutionException {
        final String name = "test";
        Config config = new Config();
        config.setProperties(AbstractCacheTest.getDefaultProperties());

        config.setNativeMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
        SerializationConfig serializationConfig = config.getSerializationConfig();
        serializationConfig.setAllowUnsafe(true);

        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);

        CacheManager cacheManager = new HazelcastCachingProvider(instance1).getCacheManager();
        ICache cache = (ICache) cacheManager.getCache(name);

        CacheManager cacheManager2 = new HazelcastCachingProvider(instance2).getCacheManager();
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

        CacheStats stats1 = cache.getStats();
        assertEquals(miss, stats1.getMisses());
        assertEquals(hit, stats1.getHits());
        assertEquals(puts, stats1.getPuts());
        assertEquals(hit + miss, stats1.getGets());
        assertEquals(removes, stats1.getRemoves());

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

        CacheStats stats2 = cache2.getStats();
        assertEquals(miss2, stats2.getMisses());
        assertEquals(hit2, stats2.getHits());
        assertEquals(puts2, stats2.getPuts());
        assertEquals(hit2 + miss2, stats2.getGets());
        assertEquals(removes2, stats2.getRemoves());

    }

}
