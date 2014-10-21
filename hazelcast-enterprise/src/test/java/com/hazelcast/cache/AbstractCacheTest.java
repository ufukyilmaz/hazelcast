package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.OffHeapMemoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.StringUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 02/06/14
 */
public abstract class AbstractCacheTest extends HazelcastTestSupport {

    protected static final String CACHE_NAME_PROPERTY = "cacheName";
    protected static final String IN_MEMORY_FORMAT_PROPERTY = "inMemoryFormat";

    protected static final String DEFAULT_CACHE_NAME = "CACHE";
    protected static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT =
            InMemoryFormat.OFFHEAP;

    protected static final String CACHE_NAME;
    protected static final InMemoryFormat IN_MEMORY_FORMAT;

    protected CachingProvider cachingProvider;
    protected CacheManager cacheManager;

    protected abstract HazelcastInstance getHazelcastInstance();

    protected abstract void onSetup();

    protected abstract void onTearDown();

    static {
        String cacheNamePropertyValue = System.getProperty(CACHE_NAME_PROPERTY);
        if (StringUtil.isNullOrEmpty(cacheNamePropertyValue)) {
            CACHE_NAME = DEFAULT_CACHE_NAME;
        } else {
            CACHE_NAME = cacheNamePropertyValue;
        }

        String cacheStorageTypePropertyValue = System.getProperty(IN_MEMORY_FORMAT_PROPERTY);
        if (StringUtil.isNullOrEmpty(cacheStorageTypePropertyValue)) {
            IN_MEMORY_FORMAT = DEFAULT_IN_MEMORY_FORMAT;
        } else {
            IN_MEMORY_FORMAT = InMemoryFormat.valueOf(cacheStorageTypePropertyValue);
        }
    }

    public static Config createConfig() {
        Config config = new Config();
        config.setProperties(getDefaultProperties());
        config.setOffHeapMemoryConfig(getDefaultMemoryConfig());
        config.setSerializationConfig(getDefaultSerializationConfig());
        return config;
    }

    public static CacheConfig createCacheConfig(String cacheName) {
        return createCacheConfig(cacheName, IN_MEMORY_FORMAT);
    }

    public static CacheConfig createCacheConfig(String cacheName,
                                                InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        return cacheConfig;
    }

    public static OffHeapMemoryConfig getDefaultMemoryConfig() {
        MemorySize memorySize = new MemorySize(256, MemoryUnit.MEGABYTES);
        return
                new OffHeapMemoryConfig()
                        .setAllocatorType(OffHeapMemoryConfig.MemoryAllocatorType.STANDARD)
                        .setSize(memorySize).setEnabled(true)
                        .setMinBlockSize(16).setPageSize(1 << 20);
    }

    public static SerializationConfig getDefaultSerializationConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setAllowUnsafe(true).setUseNativeByteOrder(true);
        return serializationConfig;
    }

    public static Properties getDefaultProperties() {
        Properties props = new Properties();
        props.setProperty(GroupProperties.PROP_PARTITION_COUNT, "111");
        props.setProperty(GroupProperties.PROP_SOCKET_BIND_ANY, "false");
        props.setProperty(GroupProperties.PROP_MAX_WAIT_SECONDS_BEFORE_JOIN, "0");
        props.setProperty(GroupProperties.PROP_GENERIC_OPERATION_THREAD_COUNT, "2");
        props.setProperty(GroupProperties.PROP_PARTITION_OPERATION_THREAD_COUNT, "4");
        props.setProperty(GroupProperties.PROP_LOGGING_TYPE, "log4j");
        return props;
    }

    protected ICache createCache() {
        Cache<Object, Object> cache = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        return cache.unwrap(ICache.class);
    }

    @Before
    public void setup() {
        onSetup();
        cachingProvider = HazelcastServerCachingProvider.createCachingProvider(getHazelcastInstance());
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public void tearDown() {
        if (cacheManager != null) {
            Iterable<String> cacheNames = cacheManager.getCacheNames();
            for (String name : cacheNames) {
                cacheManager.destroyCache(name);
            }
        }
        onTearDown();
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

    //@Test
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

    //@Test
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

    //@Test(expected = IllegalStateException.class)
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

    //@Test
    public void testIteratorDuringInsertion() throws InterruptedException {
        final ICache cache = createCache();
        int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
            public void run() {
                Random rand = new Random();
                while (!isInterrupted()) {
                    int i = rand.nextInt();
                    try {
                        cache.putAsync(i, i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

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

        thread.interrupt();
        thread.join(10000);
    }

    //@Test
    public void testIteratorDuringUpdate() throws InterruptedException {
        final ICache cache = createCache();
        final int size = 1111;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
            public void run() {
                Random rand = new Random();
                while (!isInterrupted()) {
                    int i = rand.nextInt(size);
                    try {
                        cache.putAsync(i, -i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

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

        thread.interrupt();
        thread.join(10000);
    }

    //@Test
    public void testIteratorDuringRemoval() throws InterruptedException {
        final ICache cache = createCache();
        final int size = 2222;
        for (int i = 0; i < size; i++) {
            cache.put(i, i);
        }

        final Thread thread = new Thread() {
            public void run() {
                Random rand = new Random();
                while (!isInterrupted()) {
                    int i = rand.nextInt(size);
                    try {
                        cache.removeAsync(i);
                        LockSupport.parkNanos(1);
                    } catch (Throwable ignored) {
                    }
                }
            }
        };
        thread.start();

        int k = 0;
        Iterator<Cache.Entry<Integer, Integer>> iter = cache.iterator();
        while (iter.hasNext()) {
            Cache.Entry<Integer, Integer> e = iter.next();
            int key = e.getKey();
            int value = e.getValue();
            assertEquals(key, value);
            k++;
        }
        assertTrue(k <= size);

        thread.interrupt();
        thread.join(10000);
    }

}
