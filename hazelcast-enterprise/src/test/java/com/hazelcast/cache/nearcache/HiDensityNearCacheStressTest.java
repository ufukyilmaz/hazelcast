package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.Clock;
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
public class HiDensityNearCacheStressTest extends NearCacheTestSupport {

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(1);

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
    }

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
            memoryManager = null;
        }
    }

    @Override
    protected NearCacheContext createNearCacheContext() {
        return createNearCacheContext(memoryManager);
    }

    protected NearCacheContext createNearCacheContext(HazelcastMemoryManager memoryManager) {
        EnterpriseSerializationService serializationService = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();

        return new NearCacheContext(serializationService, createNearCacheExecutor());
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);

        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(name, inMemoryFormat);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        return nearCacheConfig;
    }

    @Override
    protected NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return new HiDensityNearCache<Integer, String>(name, nearCacheConfig, createNearCacheContext(), nearCacheRecordStore);
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
        NearCacheContext nearCacheContext = createNearCacheContext();
        NearCache<Integer, String> nearCache = new HiDensityNearCache<Integer, String>(
                DEFAULT_NEAR_CACHE_NAME,
                nearCacheConfig,
                nearCacheContext);

        String VALUE_PREFIX = getValuePrefix();
        long finishTime = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; Clock.currentTimeMillis() < finishTime; i++) {
            String value = VALUE_PREFIX + i;
            nearCache.put(i, value);
            assertEquals(value, nearCache.get(i));
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
            NearCacheContext nearCacheContext = createNearCacheContext(mm);
            NearCache<Integer, byte[]> nearCache = new HiDensityNearCache<Integer, byte[]>(
                    DEFAULT_NEAR_CACHE_NAME,
                    nearCacheConfig,
                    nearCacheContext);

            byte[] value = new byte[(int) valueSize.bytes()];
            int iterationCount = (int) (2 * (memorySize.bytes() / valueSize.bytes()));
            for (int i = 0; i < iterationCount; i++) {
                nearCache.put(i, value);
            }
        } finally {
            mm.dispose();
        }
    }

    @Test
    public void putRemoveAndGetOnSoManyRecordsFromMultipleThreadsWithoutOOME() {
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        NearCacheContext nearCacheContext = createNearCacheContext();
        final NearCache<Integer, String> nearCache = new HiDensityNearCache<Integer, String>(
                DEFAULT_NEAR_CACHE_NAME,
                nearCacheConfig,
                nearCacheContext);

        final int threadCount = 9;
        final int maxRecordCount = 10000000;
        final String valuePrefix = getValuePrefix();

        final long finishTime1 = Clock.currentTimeMillis() + TIMEOUT;
        // Do initial load from main thread
        for (int i = 0; i < maxRecordCount / 10 && Clock.currentTimeMillis() < finishTime1; i++) {
            String value = valuePrefix + "value-" + i;
            nearCache.put(i, value);
            assertEquals(value, nearCache.get(i));
        }

        ExecutorService ex = Executors.newFixedThreadPool(threadCount);
        final long finishTime2 = Clock.currentTimeMillis() + TIMEOUT;
        for (int i = 0; i < threadCount; i++) {
            if (i % 3 == 0) {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        Random random = new Random();
                        while (Clock.currentTimeMillis() < finishTime2) {
                            int key = random.nextInt(maxRecordCount);
                            String value = valuePrefix + "value-" + key;
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
                        while (Clock.currentTimeMillis() < finishTime2) {
                            int key = random.nextInt(maxRecordCount);
                            nearCache.remove(key);
                            sleepMillis(100);
                        }
                    }
                });
            } else {
                ex.execute(new Runnable() {
                    @Override
                    public void run() {
                        while (Clock.currentTimeMillis() < finishTime2) {
                            Random random = new Random();
                            int key = random.nextInt(maxRecordCount);
                            String expectedValue = valuePrefix + "value-" + key;
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
