package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.Clock;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HiDensityNearCacheTest extends NearCacheTestSupport {

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;

    @Override
    protected NearCacheContext createNearCacheContext() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
        return new NearCacheContext(
                new EnterpriseSerializationServiceBuilder()
                        .setMemoryManager(memoryManager)
                        .build(), null);
    }

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.destroy();
            memoryManager = null;
        }
    }

    @Override
    protected NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return new HiDensityNearCache<Integer, String>(name,
                nearCacheConfig,
                createNearCacheContext(),
                nearCacheRecordStore);
    }

    @Test
    public void getNearCacheName() {
        doGetNearCacheName();
    }

    @Test
    public void getFromNearCache() {
        doGetFromNearCache();
    }

    @Test
    public void putFromNearCache() {
        doPutFromNearCache();
    }

    @Test
    public void removeFromNearCache() {
        doRemoveFromNearCache();
    }

    @Test
    public void invalidateFromNearCache() {
        doInvalidateFromNearCache();
    }

    @Test
    public void configureInvalidateOnChangeForNearCache() {
        doConfigureInvalidateOnChangeForNearCache();
    }

    @Test
    public void clearNearCache() {
        doClearNearCache();
    }

    @Test
    public void destroyNearCache() {
        doDestroyNearCache();
    }

    @Test
    public void configureInMemoryFormatForNearCache() {
        doConfigureInMemoryFormatForNearCache();
    }

    @Test
    public void getNearCacheStatsFromNearCache() {
        doGetNearCacheStatsFromNearCache();
    }

    @Test
    public void selectToSaveFromNearCache() {
        doSelectToSaveFromNearCache();
    }

    private String getValuePrefix() {
        Random random = new Random();
        byte[] bytes = new byte[128];
        random.nextBytes(bytes);
        return new String(bytes) + "_";
    }

    @Test
    public void putAndGetOnSoManyRecordsWithoutOOME() {
        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        NearCacheContext nearCacheContext = createNearCacheContext();
        NearCache<Integer, String> nearCache =
                new HiDensityNearCache<Integer, String>(
                        DEFAULT_NEAR_CACHE_NAME,
                        nearCacheConfig,
                        nearCacheContext);

        final String VALUE_PREFIX = getValuePrefix();
        final long finishTime = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; Clock.currentTimeMillis() < finishTime; i++) {
            String value = VALUE_PREFIX + i;
            nearCache.put(i, value);
            assertEquals(value, nearCache.get(i));
        }
    }

    @Test
    public void putRemoveAndGetOnSoManyRecordsFromMultipleThreadsWithoutOOME() {
        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        NearCacheContext nearCacheContext = createNearCacheContext();
        final NearCache<Integer, String> nearCache =
                new HiDensityNearCache<Integer, String>(
                        DEFAULT_NEAR_CACHE_NAME,
                        nearCacheConfig,
                        nearCacheContext);

        final int THREAD_COUNT = 9;
        final int MAX_RECORD_COUNT = 10000000;
        final String VALUE_PREFIX = getValuePrefix();

        // Do initial load from main thread
        for (int i = 0; i < MAX_RECORD_COUNT / 10; i++) {
            String value = VALUE_PREFIX + "value-" + i;
            nearCache.put(i, value);
            assertEquals(value, nearCache.get(i));
        }

        ExecutorService ex = Executors.newFixedThreadPool(THREAD_COUNT);
        final long finishTime = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; i < THREAD_COUNT; i++) {
            if (i % 3 == 0) {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        Random random = new Random();
                        while (Clock.currentTimeMillis() < finishTime) {
                            int key = random.nextInt(MAX_RECORD_COUNT);
                            String value = VALUE_PREFIX + "value-" + key;
                            nearCache.put(key, value);
                            sleepMillis(10);
                        }
                    }
                });
            } else if (i % 3 == 1) {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        Random random = new Random();
                        while (Clock.currentTimeMillis() < finishTime) {
                            int key = random.nextInt(MAX_RECORD_COUNT);
                            nearCache.remove(key);
                            sleepMillis(100);
                        }
                    }
                });
            } else {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        while (Clock.currentTimeMillis() < finishTime) {
                            Random random = new Random();
                            int key = random.nextInt(MAX_RECORD_COUNT);
                            String expectedValue = VALUE_PREFIX + "value-" + key;
                            String value = nearCache.get(key);
                            if (value != null) {
                                assertEquals(expectedValue, value);
                            }
                            sleepMillis(10);
                        }
                    }
                });
            }
        }

        try {
            ex.awaitTermination(2 * TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
