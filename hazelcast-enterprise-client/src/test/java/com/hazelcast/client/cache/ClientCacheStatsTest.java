///*
// * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
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
//import com.hazelcast.config.SerializationConfig;
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.monitor.NearCacheStats;
//import com.hazelcast.test.HazelcastSerialClassRunner;
//import com.hazelcast.test.annotation.QuickTest;
//import org.junit.After;
//import org.junit.Test;
//import org.junit.experimental.categories.Category;
//import org.junit.runner.RunWith;
//
//import javax.cache.CacheManager;
//import java.util.HashMap;
//import java.util.concurrent.ExecutionException;
//
//import static org.junit.Assert.assertEquals;
//
///**
// * @author enesakar 2/12/14
// */
//
//@RunWith(HazelcastSerialClassRunner.class)
//@Category(QuickTest.class)
//public class ClientCacheStatsTest {
//
//    @After
//    public void shutdown() {
//        HazelcastClient.shutdownAll();
//        Hazelcast.shutdownAll();
//    }
//
//    @Test
//    public void testStats() throws InterruptedException, ExecutionException {
//        final String name = "test";
//        Config config = new Config();
//        config.setProperties(AbstractCacheTest.getDefaultProperties());
//
//        config.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
//        SerializationConfig serializationConfig = config.getSerializationConfig();
//        serializationConfig.setAllowUnsafe(true);
//        Hazelcast.newHazelcastInstance(config);
//
//        ClientConfig clientConfig = new ClientConfig();
//        clientConfig.setProperties(AbstractCacheTest.getDefaultProperties());
//
//        HashMap<String, NearCacheConfig> nearCacheConfigMap = new HashMap<String, NearCacheConfig>();
//        NearCacheConfig nearCacheConfig = new NearCacheConfig();
//        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OFFHEAP);
//        nearCacheConfigMap.put(name, nearCacheConfig);
//        clientConfig.setNearCacheConfigMap(nearCacheConfigMap);
//        clientConfig.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
//
//        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//        CacheManager cacheManager = new HazelcastCachingProvider(client).getCacheManager();
//        ICache cache = (ICache) cacheManager.getCache(name);
//        int puts = 1000;
//        for (int i = 0; i < puts; i++) {
//            cache.put(i, i);
//        }
//
//        int hit = 30;
//        int nearCacheHit = 10;
//        int miss = 20;
//        for (int i = 0; i < hit; i++) {
//            cache.get(i);
//        }
//
//        for (int i = 0; i < nearCacheHit; i++) {
//            cache.get(i);
//        }
//
//        for (int i = 0; i < miss; i++) {
//            cache.get(i - 1000);
//        }
//
//        int removes = 10;
//        for (int i = 0; i < removes; i++) {
//            cache.remove(i);
//        }
//
//        CacheStats stats = cache.getStats();
//        assertEquals(miss, stats.getMisses());
//        assertEquals(hit, stats.getHits());
//        assertEquals(puts, stats.getPuts());
//        assertEquals(hit + miss, stats.getGets());
//        assertEquals(removes, stats.getRemoves());
//
//        NearCacheStats nearCacheStats = stats.getNearCacheStats();
//        assertEquals(hit-removes+miss, nearCacheStats.getOwnedEntryCount());
//        assertEquals(hit + miss, nearCacheStats.getMisses());
//        assertEquals(nearCacheHit, nearCacheStats.getHits());
//    }
//
//    @Test
//    public void testStatsMultiClients() throws InterruptedException, ExecutionException {
//        final String name = "test";
//        Config config = new Config();
//        config.setProperties(AbstractCacheTest.getDefaultProperties());
//
//        config.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
//        SerializationConfig serializationConfig = config.getSerializationConfig();
//        serializationConfig.setAllowUnsafe(true);
//
//        Hazelcast.newHazelcastInstance(config);
//        Hazelcast.newHazelcastInstance(config);
//
//        ClientConfig clientConfig = new ClientConfig();
//        clientConfig.setProperties(AbstractCacheTest.getDefaultProperties());
//
//        HashMap<String, NearCacheConfig> nearCacheConfigMap = new HashMap<String, NearCacheConfig>();
//        NearCacheConfig nearCacheConfig = new NearCacheConfig();
//        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OFFHEAP);
//        nearCacheConfigMap.put(name, nearCacheConfig);
//        clientConfig.setNearCacheConfigMap(nearCacheConfigMap);
//        clientConfig.setOffHeapMemoryConfig(AbstractCacheTest.getDefaultMemoryConfig());
//
//        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
//        CacheManager cacheManager = new HazelcastCachingProvider(client).getCacheManager();
//        ICache cache = (ICache) cacheManager.getCache(name);
//
//        HazelcastInstance client2 = HazelcastClient.newHazelcastClient(clientConfig);
//        CacheManager cacheManager2 = new HazelcastCachingProvider(client2).getCacheManager();
//        ICache cache2 = (ICache) cacheManager2.getCache(name);
//
//        int puts = 1000;
//        for (int i = 0; i < puts; i++) {
//            cache.put(i, i);
//        }
//
//        int hit = 30;
//        int nearCacheHit = 10;
//        int miss = 20;
//        for (int i = 0; i < hit; i++) {
//            cache.get(i);
//        }
//
//        for (int i = 0; i < nearCacheHit; i++) {
//            cache.get(i);
//        }
//
//        for (int i = 0; i < miss; i++) {
//            cache.get(i - 1000);
//        }
//
//        int removes = 10;
//        for (int i = 0; i < removes; i++) {
//            cache.remove(i);
//        }
//
//        CacheStats stats1 = cache.getStats();
//        assertEquals(miss, stats1.getMisses());
//        assertEquals(hit, stats1.getHits());
//        assertEquals(puts, stats1.getPuts());
//        assertEquals(hit + miss, stats1.getGets());
//        assertEquals(removes, stats1.getRemoves());
//        NearCacheStats nearCacheStats = stats1.getNearCacheStats();
//
//        assertEquals(hit-removes+miss, nearCacheStats.getOwnedEntryCount());
//        assertEquals(hit+miss, nearCacheStats.getMisses());
//        assertEquals(nearCacheHit, nearCacheStats.getHits());
//
//        int puts2 = 10000;
//        for (int i = 0; i < puts2; i++) {
//            cache2.put(i, i);
//        }
//
//        int hit2 = 300;
//        int nearCacheHit2 = 100;
//        int miss2 = 2000;
//        for (int i = 0; i < hit2; i++) {
//            cache2.get(i);
//        }
//
//        for (int i = 0; i < nearCacheHit2; i++) {
//            cache2.get(i);
//        }
//
//        for (int i = 0; i < miss2; i++) {
//            cache2.get(i - 10000);
//        }
//
//        int removes2 = 100;
//        for (int i = 0; i < removes2; i++) {
//            cache2.remove(i);
//        }
//
//        CacheStats stats2 = cache2.getStats();
//        assertEquals(miss2, stats2.getMisses());
//        assertEquals(hit2, stats2.getHits());
//        assertEquals(puts2, stats2.getPuts());
//        assertEquals(hit2 + miss2, stats2.getGets());
//        assertEquals(removes2, stats2.getRemoves());
//
//        NearCacheStats nearCacheStats2 = stats2.getNearCacheStats();
//        assertEquals(hit2 - removes2 + miss2, nearCacheStats2.getOwnedEntryCount());
//        assertEquals(hit2 + miss2, nearCacheStats2.getMisses());
//        assertEquals(nearCacheHit2, nearCacheStats2.getHits());
//    }
//
//}
