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
import com.hazelcast.cache.impl.CacheKeyIteratorResult;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListener;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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

    // Test to reproduce issue 129 (https://github.com/hazelcast/hazelcast-enterprise/issues/129) and
    // test code is originally taken from here.
    @Test(expected = IllegalStateException.class)
    public void cacheCreateShouldFailWithInformativeMessageIfNativeMemoryIsNotEnabled() {
        TestHazelcastInstanceFactory factory = null;
        try {
            factory = new TestHazelcastInstanceFactory(1);
            HazelcastInstance hz = factory.newHazelcastInstance();
            HazelcastServerCachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(hz);

            CacheConfig cacheConfig = new CacheConfig();
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

            // create cache should fail here with an informative exception
            Cache cache = provider.getCacheManager().createCache("test", cacheConfig);

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
        ICache cache = createCache();

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
        final ICache cache = createCache();
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
    public void testClear() {
        ICache cache = createCache();
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    public void testRemoveAll() {
        ICache cache = createCache();
        for (int i = 0; i < 10; i++) {
            cache.put("key" + i, "value" + i);
        }
        cache.removeAll();
        assertEquals(0, cache.size());
    }

    protected ExpiryPolicy ttlToExpiryPolicy(long ttl, TimeUnit timeUnit) {
        return new ModifiedExpiryPolicy(new Duration(timeUnit, ttl));
    }

    @Test
    public void testPutWithTtl() throws ExecutionException, InterruptedException {
        final ICache cache = createCache();
        final String key = "key";
        cache.put(key, "value1", ttlToExpiryPolicy(1, TimeUnit.SECONDS));

        assertTrueEventually(new AssertTask() {
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
    public void testIterator() {
        ICache cache = createCache();
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
        ICache cache = createCache();
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
        ICache cache = createCache();
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
    public void testIteratorDuringInsertion() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache cache = createCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
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
    public void testIteratorDuringUpdate() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache cache = createCache();
        final int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
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
    public void testIteratorDuringRemoval() throws InterruptedException {
        final AtomicBoolean stop = new AtomicBoolean(false);
        final ICache cache = createCache();
        final int size = 2222;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
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
        EvictionConfig evictionConfig =
                new EvictionConfig()
                        .setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                        .setSize(100);

        Factory<CacheLoader> cacheLoaderFactory = new CacheLoaderFactory();

        CacheConfiguration configuration =
                new CacheConfig()
                    .setInMemoryFormat(InMemoryFormat.NATIVE)
                    .setEvictionConfig(evictionConfig)
                    .setCacheLoaderFactory(cacheLoaderFactory);

        ICache cache = (ICache) cacheManager.createCache("test", configuration);

        Map map = new HashMap();
        for (int i = 0; i < 10; i++) {
            map.put(String.valueOf(i), i);
        }

        MyCompletionListener completionListener = new MyCompletionListener();
        cache.loadAll(map.keySet(), true, completionListener);

        Assert.assertTrue(completionListener.done.await(1, TimeUnit.MINUTES));
        Assert.assertNull("Got error: " + completionListener.error, completionListener.error.get());
    }

    private static class CacheLoaderFactory implements Factory<CacheLoader> {
        @Override
        public CacheLoader create() {
            return new CacheLoader() {
                @Override
                public Object load(Object key) throws CacheLoaderException {
                    return String.valueOf(key);
                }

                @Override
                public Map loadAll(Iterable keys) throws CacheLoaderException {
                    Map map = new HashMap();
                    for (Object key : keys) {
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
                instanceof CacheKeyIteratorResult);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.LOAD_ALL_FACTORY)
                instanceof CacheLoadAllOperationFactory);
        assertTrue(factory.create(HiDensityCacheDataSerializerHook.ENTRY_PROCESSOR)
                instanceof CacheEntryProcessorOperation);
        factory.create(Integer.MIN_VALUE);
    }

}
