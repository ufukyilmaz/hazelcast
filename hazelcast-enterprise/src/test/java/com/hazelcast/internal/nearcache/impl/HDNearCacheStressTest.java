package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
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
public class HDNearCacheStressTest extends NearCacheTestSupport {

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService ess;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
        ess = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
    }

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
            memoryManager = null;
        }
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createNearCacheConfig(name, inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    private NearCacheManager newNearCacheManager() {
        return new EnterpriseNearCacheManager(ess, executionService.getGlobalTaskScheduler(), null, properties);
    }

    @Override
    protected NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return new HDNearCache<>(name, nearCacheConfig, newNearCacheManager(), nearCacheRecordStore, ess,
                executionService.getGlobalTaskScheduler(), null, properties);
    }

    private String getValuePrefix() {
        Random random = new Random();
        byte[] bytes = new byte[512];
        random.nextBytes(bytes);
        return new String(bytes) + "_";
    }

    @Test
    public void putAndGetOnSoManyRecordsWithoutOOME() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        NearCache<Data, String> nearCache = new HDNearCache<>(DEFAULT_NEAR_CACHE_NAME, nearCacheConfig,
                newNearCacheManager(), ess, executionService.getGlobalTaskScheduler(), null, properties);
        nearCache.initialize();

        String valuePrefix = getValuePrefix();
        long finishTime = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; Clock.currentTimeMillis() < finishTime; i++) {
            String value = valuePrefix + i;
            Data keyData = ess.toData(i);
            nearCache.put(keyData, keyData, value, ess.toData(value));
            String actual = nearCache.get(keyData);
            // getting null from near cache is a valid case here, if near cache is full at `put`.
            if (actual != null) {
                assertEquals(value, actual);
            }
        }
    }

    @Test
    public void recoverFromNativeOutOfMemory() {
        MemorySize valueSize = new MemorySize(1, MemoryUnit.MEGABYTES);
        MemorySize memorySize = new MemorySize(32, MemoryUnit.MEGABYTES);
        PoolingMemoryManager mm = new PoolingMemoryManager(memorySize);
        try {
            mm.registerThread(Thread.currentThread());

            NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
            NearCache<Integer, byte[]> nearCache = new HDNearCache<>(DEFAULT_NEAR_CACHE_NAME,
                    nearCacheConfig, newNearCacheManager(), ess, executionService.getGlobalTaskScheduler(), null, properties);
            nearCache.initialize();

            byte[] value = new byte[(int) valueSize.bytes()];
            int iterationCount = (int) (2 * (memorySize.bytes() / valueSize.bytes()));
            for (int i = 0; i < iterationCount; i++) {
                nearCache.put(i, ess.toData(i), value, ess.toData(value));
            }
        } finally {
            mm.dispose();
        }
    }

    @Test
    public void putRemoveAndGetOnSoManyRecordsFromMultipleThreadsWithoutOOME() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        final NearCache<Data, String> nearCache = new HDNearCache<>(DEFAULT_NEAR_CACHE_NAME,
                nearCacheConfig, newNearCacheManager(), ess, executionService.getGlobalTaskScheduler(), null, properties);
        nearCache.initialize();

        final int threadCount = 9;
        final int maxRecordCount = 10000000;
        final String valuePrefix = getValuePrefix();

        final long finishTime1 = Clock.currentTimeMillis() + TIMEOUT;
        // do initial load from main thread
        for (int i = 0; i < maxRecordCount / 10 && Clock.currentTimeMillis() < finishTime1; i++) {
            String value = valuePrefix + "value-" + i;
            Data keyData = ess.toData(i);
            nearCache.put(keyData, keyData, value, ess.toData(value));
            String actual = nearCache.get(keyData);
            // getting null from near cache is a valid case here, if near cache is full at `put`.
            if (actual != null) {
                assertEquals(nearCache.getNearCacheStats().toString(), value, actual);
            }
        }

        ExecutorService ex = Executors.newFixedThreadPool(threadCount);
        final long finishTime2 = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; i < threadCount; i++) {
            if (i % 3 == 0) {
                ex.execute(() -> {
                    Random random = new Random();
                    while (Clock.currentTimeMillis() < finishTime2) {
                        int key = random.nextInt(maxRecordCount);
                        String value = valuePrefix + "value-" + key;
                        Data keyData = ess.toData(key);
                        nearCache.put(keyData, keyData, value, ess.toData(value));
                        sleepMillis(10);
                    }
                });
            } else if (i % 3 == 1) {
                ex.execute(() -> {
                    Random random = new Random();
                    while (Clock.currentTimeMillis() < finishTime2) {
                        int key = random.nextInt(maxRecordCount);
                        Data keyData = ess.toData(key);
                        nearCache.invalidate(keyData);
                        sleepMillis(100);
                    }
                });
            } else {
                ex.execute(() -> {
                    while (Clock.currentTimeMillis() < finishTime2) {
                        Random random = new Random();
                        int key = random.nextInt(maxRecordCount);
                        String expectedValue = valuePrefix + "value-" + key;
                        Data keyData = ess.toData(key);
                        String value = nearCache.get(keyData);
                        if (value != null) {
                            assertEquals(expectedValue, value);
                        }
                        sleepMillis(10);
                    }
                });
            }
        }

        try {
            ex.shutdown();
            ex.awaitTermination(2 * TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
