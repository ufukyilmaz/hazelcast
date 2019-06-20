package com.hazelcast.cache;

import com.hazelcast.cache.hidensity.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.hidensity.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.hidensity.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.hidensity.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.hidensity.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.hidensity.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.hidensity.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.hidensity.operation.CacheReplaceOperation;
import com.hazelcast.cache.hidensity.operation.CacheSizeOperation;
import com.hazelcast.cache.hidensity.operation.CacheSizeOperationFactory;
import com.hazelcast.cache.hidensity.operation.HiDensityCacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEventListener;
import com.hazelcast.cache.impl.CacheKeyIterationResult;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.expiry.TouchedExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.HDTestSupport.getICache;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheTest extends AbstractCacheTest {

    private HazelcastInstance instance;

    @Override
    protected void onSetup() {
        Config config = createConfig();
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

    /**
     * Test to reproduce issue 129 (https://github.com/hazelcast/hazelcast-enterprise/issues/129).
     * <p>
     * The test code is originally taken from there.
     */
    @Test(expected = IllegalStateException.class)
    public void cacheCreateShouldFailWithInformativeMessageIfNativeMemoryIsNotEnabled() {
        TestHazelcastInstanceFactory factory = null;
        try {
            factory = new TestHazelcastInstanceFactory(1);
            HazelcastInstance hz = factory.newHazelcastInstance();
            HazelcastServerCachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hz);

            CacheConfig<String, String> cacheConfig = new CacheConfig<String, String>()
                    .setInMemoryFormat(InMemoryFormat.NATIVE)
                    .setEvictionConfig(new EvictionConfig(99, USED_NATIVE_MEMORY_PERCENTAGE, EvictionPolicy.LRU));

            // create cache should fail here with an informative exception
            Cache<String, String> cache = getICache(provider.getCacheManager(), cacheConfig, "test");

            // trigger cache record store creation by accessing cache
            // since cache record stores are created as lazy when they are accessed
            cache.put("key", "value");
        } finally {
            if (factory != null) {
                factory.shutdownAll();
            }
        }
    }

    @Test
    public void testPutGetRemoveReplace() throws InterruptedException, ExecutionException {
        ICache<String, String> cache = createCache();

        cache.put("key1", "value1");
        assertEquals("value1", cache.get("key1"));

        assertEquals("value1", cache.getAndPut("key1", "value2"));
        assertEquals(1, cache.size());

        assertTrue(cache.remove("key1"));

        cache.put("key1", "value3");
        assertFalse(cache.remove("key1", "xx"));
        assertTrue(cache.remove("key1", "value3"));
        assertNull(cache.get("key1"));

        assertTrue(cache.putIfAbsent("key1", "value1"));
        assertFalse(cache.putIfAbsent("key1", "value1"));
        assertEquals("value1", cache.getAndRemove("key1"));
        assertNull(cache.get("key1"));

        cache.put("key1", "value1");
        assertTrue(cache.containsKey("key1"));

        assertFalse(cache.replace("key2", "value2"));
        assertTrue(cache.replace("key1", "value2"));
        assertEquals("value2", cache.get("key1"));

        assertFalse(cache.replace("key1", "xx", "value3"));
        assertTrue(cache.replace("key1", "value2", "value3"));
        assertEquals("value3", cache.get("key1"));

        assertEquals("value3", cache.getAndReplace("key1", "value4"));
        assertEquals("value4", cache.get("key1"));
    }

    @Test
    public void testAsyncGetPutRemove() throws InterruptedException, ExecutionException {
        final ICache<String, String> cache = createCache();
        final String key = "key";
        cache.put(key, "value1");
        Future f = cache.getAsync(key);
        assertEquals("value1", f.get());

        cache.putAsync(key, "value2");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals("value2", cache.get(key));
            }
        });

        f = cache.getAndPutAsync(key, "value3");
        assertEquals("value2", f.get());
        assertEquals("value3", cache.get(key));

        f = cache.removeAsync("key2");
        assertFalse((Boolean) f.get());
        f = cache.removeAsync(key);
        assertTrue((Boolean) f.get());

        cache.put(key, "value4");
        f = cache.getAndRemoveAsync("key2");
        assertNull(f.get());
        f = cache.getAndRemoveAsync(key);
        assertEquals("value4", f.get());
    }

    @Test
    public void testExpiration() {
        CacheConfig<Integer, String> config = createCacheConfig(CACHE_NAME);
        final SimpleExpiryListener<Integer, String> listener = new SimpleExpiryListener<Integer, String>();
        MutableCacheEntryListenerConfiguration<Integer, String> listenerConfiguration =
                new MutableCacheEntryListenerConfiguration<Integer, String>(
                        FactoryBuilder.factoryOf(listener), null, true, true);

        config.addCacheEntryListenerConfiguration(listenerConfiguration);
        config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new HazelcastExpiryPolicy(100, 100, 100)));

        Cache<Integer, String> instanceCache = createCache(config);

        instanceCache.put(1, "value");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, listener.expired.get());
            }
        });
        assertEquals(0, ((ICache<Integer, String>) instanceCache).size());
    }

    @Test
    public void testClear() {
        ICache<String, String> cache = createCache();
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testRemoveAll() {
        ICache<String, String> cache = createCache();
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        cache.removeAll();
        assertEquals(0, cache.size());
    }

    @Test
    public void testSetExpiryPolicyReturnsTrue() {
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        assertTrue(cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.FIVE_MINUTES)));
    }

    @Test
    public void testSetExpiryPolicyReturnsFalse_whenKeyDoesNotExist() {
        ICache<Integer, String> cache = createCache();
        assertFalse(cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.FIVE_MINUTES)));
    }

    @Test
    public void testSetExpiryPolicyReturnsFalse_whenKeyIsAlreadyExpired() {
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value", new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1)));
        sleepAtLeastSeconds(5);
        assertFalse(cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.FIVE_MINUTES)));
    }

    @Test
    public void testRecordExpiryPolicyTakesPrecedenceOverCachePolicy() {
        final int UPDATED_TTL = 1000;

        Duration generalDuration = new Duration(TimeUnit.DAYS, 1);
        Duration modifiedDuration = new Duration(TimeUnit.MILLISECONDS, UPDATED_TTL);
        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>();
        cacheConfig.setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(generalDuration));

        ICache<Integer, String> cache = createCache(cacheConfig);
        cache.put(1, "value");
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(modifiedDuration));

        sleepAtLeastMillis(UPDATED_TTL + 1);

        assertNull(cache.get(1));
    }

    @Test
    public void testRecordExpiryPolicyTakesPrecedence() {
        final int TTL = 1000;

        Duration modifiedDuration = new Duration(TimeUnit.MILLISECONDS, TTL);

        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(modifiedDuration));

        sleepAtLeastMillis(TTL + 1);

        assertNull(cache.get(1));
    }

    @Test
    public void test_whenExpiryPolicyIsOverriden_thenNewPolicyIsInEffect() {
        final int TTL = 1000;
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        Duration expiryDuration = new Duration(TimeUnit.MILLISECONDS, TTL);
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(expiryDuration));
        cache.setExpiryPolicy(1, new TouchedExpiryPolicy(Duration.ETERNAL));

        sleepAtLeastMillis(TTL + 1);

        assertEquals("value", cache.get(1));
    }

    @Test
    public void test_CustomExpiryPolicyIsUsedWhenEntryIsUpdated() {
        ICache<Integer, String> cache = createCache();
        cache.put(1, "value");
        cache.setExpiryPolicy(1, new HazelcastExpiryPolicy(10000, 10000, 10000));
        sleepAtLeastSeconds(5);
        cache.put(1, "value2");
        sleepAtLeastSeconds(5);

        assertEquals("value2", cache.get(1));
    }

    @Test
    public void testInvalidationListenerCallCount() {
        final ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            cache.put(entry.getKey(), entry.getValue());
        }

        // Verify that put works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        final AtomicInteger counter = new AtomicInteger(0);

        registerInvalidationListener(new CacheEventListener() {
            @Override
            public void handleEvent(Object eventObject) {
                if (eventObject instanceof Invalidation) {
                    Invalidation event = (Invalidation) eventObject;
                    if (null == event.getKey() && cache.getPrefixedName().equals(event.getName())) {
                        counter.incrementAndGet();
                    }
                }
            }
        }, cache.getPrefixedName());

        cache.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(1, counter.get());
            }
        }, 2);

        // Make sure that the callback is not called for a while
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(counter.get() <= 1);
            }
        }, 3);
    }

    protected Map<String, String> createAndFillEntries() {
        final int ENTRY_COUNT_PER_PARTITION = 3;
        Node node = getNode(instance);
        int partitionCount = node.getPartitionService().getPartitionCount();
        Map<String, String> entries = new HashMap<String, String>(partitionCount * ENTRY_COUNT_PER_PARTITION);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int i = 0; i < ENTRY_COUNT_PER_PARTITION; i++) {
                String key = generateKeyForPartition(instance, partitionId);
                String value = generateRandomString(16);
                entries.put(key, value);
            }
        }

        return entries;
    }

    private void registerInvalidationListener(CacheEventListener cacheEventListener, String name) {
        HazelcastInstanceProxy hzInstance = (HazelcastInstanceProxy) this.instance;
        hzInstance.getOriginal().node.getNodeEngine().getEventService()
                .registerListener(ICacheService.SERVICE_NAME, name, cacheEventListener);
    }

    protected ExpiryPolicy ttlToExpiryPolicy(long ttl, TimeUnit timeUnit) {
        return new ModifiedExpiryPolicy(new Duration(timeUnit, ttl));
    }

    @Test
    public void testPutWithTtl() throws ExecutionException, InterruptedException {
        final ICache<String, String> cache = createCache();
        final String key = "key";
        cache.put(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());

        cache.putAsync(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());

        cache.put(key, "value2");
        Object o = cache.getAndPut(key, "value3", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertEquals("value2", o);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());

        cache.put(key, "value4");
        Future f = cache.getAndPutAsync(key, "value5", ttlToExpiryPolicy(1, TimeUnit.SECONDS));
        assertEquals("value4", f.get());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(cache.get(key));
            }
        });
        assertEquals(0, cache.size());
    }

    @Test
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testIterator() {
        ICache<Integer, Integer> cache = createCache();
        int size = 1111;
        int multiplier = 11;
        for (int i = 0; i < size; i++) {
            cache.put(i, i * multiplier);
        }

        int[] keys = new int[size];
        int k = 0;
        Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
        while (iter.hasNext()) {
            Cache.Entry<Integer, Integer> e = iter.next();
            int key = e.getKey();
            int value = e.getValue();
            assertEquals(key * multiplier, value);
            keys[k++] = key;
        }
        assertEquals(size, k);

        Arrays.sort(keys);
        for (int i = 0; i < size; i++) {
            assertEquals(i, keys[i]);
        }
    }

    @Test
    public void testIteratorRemove() {
        ICache<Integer, Integer> cache = createCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }
        assertEquals(0, cache.size());
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorIllegalRemove() {
        ICache<Integer, Integer> cache = createCache();
        int size = 10;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
        if (iter.hasNext()) {
            iter.remove();
        }
    }

    @Test
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testIteratorDuringInsertion() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache<Integer, Integer> cache = createCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
            @Override
            public void run() {
                Random rand = new Random();
                while (!stop.get()) {
                    int i = rand.nextInt();
                    try {
                        cache.put(i, i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        // Give chance to thread for starting
        sleepSeconds(1);

        try {
            int k = 0;
            Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
            while (iter.hasNext()) {
                Cache.Entry<Integer, Integer> e = iter.next();
                int key = e.getKey();
                int value = e.getValue();
                assertEquals(key, value);
                k++;
            }
            assertTrue(k >= size);
        } finally {
            stop.set(true);
            thread.join();
        }
    }

    @Test
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testIteratorDuringUpdate() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache<Integer, Integer> cache = createCache();
        final int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
            @Override
            public void run() {
                Random rand = new Random();
                while (!stop.get()) {
                    int i = rand.nextInt(size);
                    try {
                        cache.put(i, -i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        // Give chance to thread for starting
        sleepSeconds(1);

        try {
            int k = 0;
            Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
            while (iter.hasNext()) {
                Cache.Entry<Integer, Integer> e = iter.next();
                int key = e.getKey();
                int value = e.getValue();
                assertTrue("Key: " + key + ", Value: " + value, key == Math.abs(value));
                k++;
            }
            assertEquals(size, k);
        } finally {
            stop.set(true);
            thread.join();
        }
    }

    @Test
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testIteratorDuringRemoval() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache<Integer, Integer> cache = createCache();
        final int size = 2222;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
            @Override
            public void run() {
                Random rand = new Random();
                while (!stop.get()) {
                    int i = rand.nextInt(size);
                    try {
                        cache.remove(i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        // Give chance to thread for starting
        sleepSeconds(1);

        try {
            int k = 0;
            Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
            while (iter.hasNext()) {
                Cache.Entry<Integer, Integer> e = iter.next();
                int key = e.getKey();
                Integer value = e.getValue();
                if (value != null) {
                    assertEquals(key, value.intValue());
                }
                k++;
            }
            assertTrue(k <= size);
        } finally {
            stop.set(true);
            thread.join();
        }
    }

    // Issue https://github.com/hazelcast/hazelcast-enterprise/issues/296
    @Test
    public void testCacheLoadAll() throws InterruptedException {
        Factory<CacheLoader<String, Integer>> cacheLoaderFactory = new CacheLoaderFactory();

        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(100);

        CacheConfiguration<String, Integer> configuration = new CacheConfig<String, Integer>()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig)
                .setCacheLoaderFactory(cacheLoaderFactory);

        ICache<String, Integer> cache = getICache(cacheManager, configuration, "test");

        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf(i), i);
        }

        MyCompletionListener completionListener = new MyCompletionListener();
        cache.loadAll(map.keySet(), true, completionListener);

        assertTrue(completionListener.done.await(1, TimeUnit.MINUTES));
        assertNull("Got error: " + completionListener.error, completionListener.error.get());
    }

    private static class CacheLoaderFactory implements Factory<CacheLoader<String, Integer>> {
        @Override
        public CacheLoader<String, Integer> create() {
            return new CacheLoader<String, Integer>() {
                @Override
                public Integer load(String key) throws CacheLoaderException {
                    return Integer.parseInt(key);
                }

                @Override
                public Map<String, Integer> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
                    Map<String, Integer> map = new HashMap<String, Integer>();
                    for (String key : keys) {
                        map.put(key, load(key));
                    }
                    return map;
                }
            };
        }
    }

    private static class MyCompletionListener implements CompletionListener {

        final CountDownLatch done = new CountDownLatch(1);
        final AtomicReference<Exception> error = new AtomicReference<Exception>();

        @Override
        public void onCompletion() {
            done.countDown();
        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
            error.set(e);
            done.countDown();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHiDensityCacheDataSerializerHook() throws Exception {
        HiDensityCacheDataSerializerHook hook = new HiDensityCacheDataSerializerHook();
        DataSerializableFactory factory = hook.createFactory();

        assertTrue(factory.create(HiDensityCacheDataSerializerHook.CONTAINS_KEY)
                instanceof CacheContainsKeyOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.PUT_IF_ABSENT)
                instanceof CachePutIfAbsentOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.GET_AND_REMOVE)
                instanceof CacheGetAndRemoveOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.REPLACE)
                instanceof CacheReplaceOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.GET_AND_REPLACE)
                instanceof CacheGetAndReplaceOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.SIZE)
                instanceof CacheSizeOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.SIZE_FACTORY)
                instanceof CacheSizeOperationFactory);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ITERATE)
                instanceof CacheKeyIteratorOperation);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ITERATION_RESULT)
                instanceof CacheKeyIterationResult);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.LOAD_ALL_FACTORY)
                instanceof CacheLoadAllOperationFactory);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ENTRY_PROCESSOR)
                instanceof CacheEntryProcessorOperation);
        factory.create(Integer.MIN_VALUE);
    }

    @Test
    public void testNativeCache_UsedNativeMemory_beforeAndAfterShutdown() {
        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig(CACHE_NAME, InMemoryFormat.NATIVE);
        Cache<Integer, Integer> cache = getICache(cacheManager, cacheConfig, CACHE_NAME);
        final int count = 100;
        for (int i = 0; i < count; i++) {
            cache.put(i, i);
        }

        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(instance).getNodeExtension();
        MemoryStats memoryStats = nodeExtension.getMemoryManager().getMemoryStats();

        long minUsedMemory = count * 8L; // int key + int value
        assertThat(memoryStats.getUsedNative(), greaterThanOrEqualTo(minUsedMemory));

        instance.shutdown();

        assertEquals(0, memoryStats.getUsedNative());
    }

    public static class SimpleExpiryListener<K, V>
            implements CacheEntryExpiredListener<K, V>, Serializable {

        public AtomicInteger expired = new AtomicInteger();

        public SimpleExpiryListener() {
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> cacheEntryEvents)
                throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> cacheEntryEvent : cacheEntryEvents) {
                expired.incrementAndGet();
            }
        }
    }
}
