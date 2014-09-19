//package com.hazelcast.client.cache;
//
//import com.hazelcast.cache.AbstractCacheTest;
//import com.hazelcast.cache.CacheStats;
//import com.hazelcast.cache.HazelcastCachingProvider;
//import com.hazelcast.cache.ICache;
//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.config.Config;
//import com.hazelcast.config.InMemoryFormat;
//import com.hazelcast.config.NearCacheConfig;
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.test.HazelcastSerialClassRunner;
//import com.hazelcast.test.annotation.QuickTest;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//import org.junit.runner.RunWith;
//
//import javax.cache.Cache;
//import javax.cache.CacheManager;
//import java.util.UUID;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//@RunWith(HazelcastSerialClassRunner.class)
//@Category(QuickTest.class)
//public class LocalUpdatePolicyTest {
//
//    protected static final int NEAR_CACHE_SIZE = 5;
//    private HazelcastInstance hazelcastInstance;
//
//    @Before
//	public void setUp() throws Exception {
//        ClientConfig clientConfig = new ClientConfig();
//        clientConfig.setProperties(AbstractCacheTest.getDefaultProperties());
//
//        NearCacheConfig nc = new NearCacheConfig();
//        nc.setMaxSize(NEAR_CACHE_SIZE);
//        nc.setEvictionPolicy("LRU");
//        nc.setInMemoryFormat(InMemoryFormat.OBJECT);
//        nc.setName("AOS_test_small.*");
//        nc.setTimeToLiveSeconds(60);
//        nc.setMaxIdleSeconds(600);
//        nc.setInvalidateOnChange(true);
//        nc.setLocalUpdatePolicy(NearCacheConfig.LocalUpdatePolicy.CACHE);
//        clientConfig.addNearCacheConfig("AOS_test_small.*", nc);
//
//        clientConfig.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
//
//        Config config = new Config();
//        config.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
//
//        Hazelcast.newHazelcastInstance(config);
//
//        hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);
//    }
//
//    @After
//    public void shutdown() {
//        HazelcastClient.shutdownAll();
//        Hazelcast.shutdownAll();
//    }
//
//    /**
//     * Test that by using NearCacheConfig.LocalUpdatePolicy.CACHE,
//     * as soon as we put() an item in the cache, it is immediately available in the near-cache.
//     */
//    @Test
//    public void testSyncCase() throws ExecutionException, InterruptedException {
//        tryNearCache(false);
//    }
//
//
//    /**
//     * Test that by using NearCacheConfig.LocalUpdatePolicy.CACHE,
//     * as soon as we putAsync() an item in the cache, followed by future.get(),
//     * it is immediately available in the near-cache.
//     */
//    @Test
//    public void testAsyncCase() throws ExecutionException, InterruptedException {
//        tryNearCache(true);
//    }
//
//    public void tryNearCache(boolean async) throws ExecutionException, InterruptedException {
//        String key = UUID.randomUUID().toString();
//
//        CacheManager manager = new HazelcastCachingProvider(hazelcastInstance).getCacheManager();
//        Cache<Object, Object> jcache = manager.getCache("AOS_test_small." + UUID.randomUUID().toString());
//
//        ICache icache = jcache.unwrap(ICache.class);
//
//        CacheStats stats = icache.getStats();
//        long originalNearCacheHits = stats.getNearCacheStats().getHits();
//
//        for (int i = 0; i < 10; i++) {
//            if (async) {
//                Future future = icache.putAsync(key + i, "value" + i, 10000L, TimeUnit.MILLISECONDS);
//                future.get();
//            } else {
//                icache.put(key + i, "value" + i, 10000L, TimeUnit.MILLISECONDS);
//            }
//        }
//
//        for (int i = 9; i >= 0; i--) {
//            assertEquals("value" + i, icache.get(key + i));
//        }
//
//        assertEquals(10, icache.size());
//
//        assertRange("We should have filled up the near cache which has size " + NEAR_CACHE_SIZE,
//                    4, 6, icache.getStats().getNearCacheStats().getOwnedEntryCount());
//
//        long nearCacheHits = icache.getStats().getNearCacheStats().getHits() - originalNearCacheHits;
//        assertRange("Because we retrieved in reverse order, we should be seeing " + NEAR_CACHE_SIZE + " near cache hits",
//                    4, 6, nearCacheHits);
//
//    }
//
//    private void assertRange(String message, double expectedLow, double expectedHigh, double actual) {
//        assertTrue(message+": expectedLow="+expectedLow+"; actual="+actual, actual >= expectedLow);
//        assertTrue(message+": expectedHigh: " + expectedLow + "; actual=" + actual, actual <= expectedHigh);
//    }
//
//}
//
