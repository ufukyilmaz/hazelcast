package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheNativeMemoryListenerTest extends HazelcastTestSupport {

    static final int TEST_TIME = 3;

    HazelcastServerCachingProvider provider;
    Cache cache;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        config.getNativeMemoryConfig().setEnabled(true);

        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        provider = HazelcastServerCachingProvider.createCachingProvider(instance);
        CacheManager cacheManager = provider.getCacheManager();
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setSize(90);
        evictionConfig.setMaxSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        cacheConfig.setEvictionConfig(evictionConfig);
        String cacheName = randomString();
        cache = cacheManager.createCache(cacheName, cacheConfig);
    }

    @After
    public void tearDown() {
        provider.close();
    }


    @Test
    public void testDuplicateEventPublishing_while_put() {
        final AtomicInteger counter = new AtomicInteger();
        MutableCacheEntryListenerConfiguration configuration = new MutableCacheEntryListenerConfiguration(
                FactoryBuilder.factoryOf(new TestListener(counter, true)), null, true, true);
        cache.registerCacheEntryListener(configuration);

        final int count = 10;
        for (int i = 0; i < count; i++) {
            cache.put(i, i);
        }

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals("Created events are duplicate", count, counter.get());
            }
        }, TEST_TIME);
    }

    @Test
    public void testDuplicateEventPublishing_while_remove() {
        final AtomicInteger counter = new AtomicInteger();
        MutableCacheEntryListenerConfiguration configuration = new MutableCacheEntryListenerConfiguration(
                FactoryBuilder.factoryOf(new TestListener(counter, false)), null, true, true);
        cache.registerCacheEntryListener(configuration);

        final int count = 10;
        for (int i = 0; i < count; i++) {
            cache.put(i, i);
        }

        for (int i = 0; i < count; i++) {
            cache.remove(i);
        }

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                Assert.assertEquals("Removed events are duplicate", count, counter.get());
            }
        }, TEST_TIME);
    }

    public static class TestListener
            implements CacheEntryCreatedListener, CacheEntryRemovedListener, Serializable {

        private final AtomicInteger counter;
        private final boolean create;

        public TestListener(AtomicInteger counter, boolean create) {
            this.counter = counter;
            this.create = create;
        }

        @Override
        public void onCreated(Iterable iterable) throws CacheEntryListenerException {
            if (!create) {
                return;
            }
            for (Object o : iterable) {
                counter.incrementAndGet();
            }
        }

        @Override
        public void onRemoved(Iterable iterable) throws CacheEntryListenerException {
            if (create) {
                return;
            }
            for (Object o : iterable) {
                counter.incrementAndGet();
            }
        }
    }

}
