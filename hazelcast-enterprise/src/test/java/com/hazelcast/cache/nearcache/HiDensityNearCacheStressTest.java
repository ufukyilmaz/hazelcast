package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.EvictionConfig;
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
public class HiDensityNearCacheStressTest extends NearCacheTestSupport {

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
                        .build(),
                createNearCacheExecutor());
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(name, inMemoryFormat);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);
        nearCacheConfig.setEvictionConfig(evictionConfig);
        return nearCacheConfig;
    }

    @After
    public void tearDown() {
        super.tearDown();
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

    private String getValuePrefix() {
        Random random = new Random();
        byte[] bytes = new byte[512];
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
    public void recoverFromNativeOutOfMemory() {
        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        NearCacheContext nearCacheContext = createNearCacheContext();
        NearCache<Integer, byte[]> nearCache =
                new HiDensityNearCache<Integer, byte[]>(
                        DEFAULT_NEAR_CACHE_NAME,
                        nearCacheConfig,
                        nearCacheContext);

        final long finishTime = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; Clock.currentTimeMillis() < finishTime; i++) {
            byte[] value = new byte[128 * 1024 * 1024];
            nearCache.put(i, value);
            //Each put after 1st one should cause NativeOutOfMemory and force evict the previous entry
            assertEquals(1, nearCache.size());
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
