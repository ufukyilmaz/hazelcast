//package com.hazelcast.client.cache;
//
//import com.hazelcast.cache.AbstractCacheTest;
//import com.hazelcast.cache.CacheLoadTest;
//import com.hazelcast.cache.HazelcastCachingProvider;
//import com.hazelcast.cache.ICache;
//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.test.HazelcastSerialClassRunner;
//import com.hazelcast.test.annotation.SlowTest;
//import org.junit.After;
//import org.junit.experimental.categories.Category;
//import org.junit.runner.RunWith;
//
//import javax.cache.CacheManager;
//
///**
// * @author mdogan 02/06/14
// */
//@RunWith(HazelcastSerialClassRunner.class)
//@Category(SlowTest.class)
//public class ClientCacheLoadTest extends CacheLoadTest {
//
//    @Override
//    protected ICache createCache(String name) {
//        ClientConfig clientConfig = new ClientConfig();
//        clientConfig.setSerializationConfig(AbstractCacheTest.getDefaultSerializationConfig());
//
//        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//        CacheManager cacheManager = new HazelcastCachingProvider(client).getCacheManager();
//        return cacheManager.getCache(name).unwrap(ICache.class);
//    }
//
//    @After
//    public void tearDown() {
//        HazelcastClient.shutdownAll();
//        Hazelcast.shutdownAll();
//    }
//}
