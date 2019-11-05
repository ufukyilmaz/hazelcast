package com.hazelcast.cache.impl.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.spi.properties.GroupProperty.HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE;
import static java.util.Arrays.asList;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheHotRestartEvictionTest extends AbstractCacheHotRestartTest {

    private static final int KEY_RANGE = 1024 * 1024 * 32;
    private static final int MIN_VALUE_SIZE = 32;
    private static final int MAX_VALUE_SIZE = 4096;

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, 25000, false, true, false},
                {InMemoryFormat.BINARY, 25000, false, true, false},
                {InMemoryFormat.BINARY, 25000, false, true, true},
        });
    }

    @Override
    MemorySize getNativeMemorySize() {
        return new MemorySize(256, MemoryUnit.MEGABYTES);
    }

    @Test
    public void testEviction() {
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, byte[]> cache = createCache(hz);

        int threadCount = 4;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Throwable> failures = synchronizedList(new ArrayList<Throwable>());

        for (int i = 0; i < threadCount; i++) {
            spawn(new CacheTask(keyRange, cache, latch, failures));
        }
        assertOpenEventually(latch, TimeUnit.MINUTES.toSeconds(10));

        if (!failures.isEmpty()) {
            throw new HotRestartException(failures.size() + " failures!", failures.get(0));
        }

        int iterated = 0;
        int expectedSize = cache.size();
        for (Cache.Entry<Integer, byte[]> entry : cache) {
            assertNotNull(entry.getValue());
            iterated++;
        }
        assertEquals(expectedSize, iterated);

        // acquire some samples
        int samples = 5000;
        Random random = new Random();
        Map<Integer, byte[]> expected = new HashMap<Integer, byte[]>(samples);
        for (int i = 0; i < samples; i++) {
            int key = random.nextInt(KEY_RANGE);
            byte[] value = cache.get(key);
            if (value != null) {
                expected.put(key, value);
            }
        }

        hz = restartInstances(1)[0];
        cache = createCache(hz);

        assertEquals(expectedSize, cache.size());

        for (Map.Entry<Integer, byte[]> entry : expected.entrySet()) {
            byte[] expectedValue = entry.getValue();
            byte[] actualValue = cache.get(entry.getKey());
            assertArrayEquals("Expected: " + expectedValue.length, expectedValue, actualValue);
        }
    }

    @Test
    public void freeNativeMemoryPercentageCannotBeLessThanItsMinimumLimit() {
        assumeTrue("This test is just for NATIVE in-memory configuration", memoryFormat == InMemoryFormat.NATIVE);

        HazelcastInstance hz = newHazelcastInstance();
        HazelcastProperties properties = getNode(hz).getProperties();
        int freeNativeMemoryPercentage = properties.getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(freeNativeMemoryPercentage / 2)
                .setMaxSizePolicy(FREE_NATIVE_MEMORY_PERCENTAGE)
                .setEvictionPolicy(LRU);
        ICache<Integer, byte[]> cache = createCache(hz, evictionConfig);
        try {
            // config is checked while creating cache record store, so to trigger it we are putting to cache
            cache.put(1, new byte[1]);
            fail("Free native memory percentage cannot be less than " + freeNativeMemoryPercentage);
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void evictionShouldBeTriggeredWhenFreeNativeMemoryPercentageIsReached() {
        assumeTrue("This test is just for NATIVE in-memory configuration", memoryFormat == InMemoryFormat.NATIVE);

        HazelcastInstance hz = newHazelcastInstance();
        Node node = getNode(hz);
        HazelcastProperties properties = getNode(hz).getProperties();
        int freeNativeMemoryPercentage = properties.getInteger(HOT_RESTART_FREE_NATIVE_MEMORY_PERCENTAGE);
        ICache<String, byte[]> cache = createCache(hz);

        int partitionCount = node.getPartitionService().getPartitionCount();
        EnterpriseSerializationService ss = (EnterpriseSerializationService) node.getSerializationService();
        MemoryStats memoryStats = ss.getMemoryManager().getMemoryStats();
        EnterpriseCacheService cacheService = node.nodeEngine.getService(ICacheService.SERVICE_NAME);
        HiDensityStorageInfo cacheInfo = cacheService.getOrCreateHiDensityCacheInfo("/hz/" + cache.getName());

        long maxNativeMemory = memoryStats.getMaxNative();
        long minFreeNativeMemory = (long) (maxNativeMemory * freeNativeMemoryPercentage / 100f);
        // entrySize is 1 MB
        int entrySize = 1024 * 1024;
        // the entryCount is in fact less than this
        long entryCountToFillUpMemory = maxNativeMemory / entrySize;

        byte[] smallValue = new byte[1];
        for (int i = 0; i < partitionCount; i++) {
            cache.put(generateKeyForPartition(hz, i), smallValue);
        }
        // now there is at least one element in each partition, so there is always an evictable element in each partition

        byte[] value = new byte[entrySize];
        for (int i = 0; i < entryCountToFillUpMemory; i++) {
            long forceEvictionCountBefore = cacheInfo.getForceEvictionCount();
            int sizeBefore = cache.size();

            long actualMinFreeNativeMemory = memoryStats.getFreeNative();
            boolean evictionShouldBeTriggeredBecauseOfMinFreeMemory = actualMinFreeNativeMemory < minFreeNativeMemory;

            int j = i % partitionCount;
            cache.put(generateKeyForPartition(hz, j), value);

            long forceEvictionCountAfter = cacheInfo.getForceEvictionCount();
            int sizeAfter = cache.size();

            if (forceEvictionCountBefore == forceEvictionCountAfter) {
                // Forced eviction might be applied to other partitions owned by same thread
                // so these partitions might be totally empty.
                // In this case, no eviction is applied to that partition based record store since there is no entry.
                // At the moment, we simply finish the test but we can improve this test later.
                break;
            }

            if (evictionShouldBeTriggeredBecauseOfMinFreeMemory) {
                assertEquals("When eviction is triggered, size after put cannot be more than before put", sizeAfter, sizeBefore);
            }
        }
    }

    private static class CacheTask implements Runnable {

        private final List<Throwable> failures;
        private final CountDownLatch latch;
        private final int operationCount;
        private final Cache<Integer, byte[]> cache;

        CacheTask(int operationCount, Cache<Integer, byte[]> cache, CountDownLatch latch, List<Throwable> failures) {
            this.operationCount = operationCount;
            this.cache = cache;
            this.failures = failures;
            this.latch = latch;
        }

        @Override
        public void run() {
            Random random = new Random();
            try {
                for (int i = 0; i < operationCount; i++) {
                    int key = random.nextInt(KEY_RANGE);
                    byte[] value = randomValue(random);
                    cache.put(key, value);
                }
                for (int i = 0; i < operationCount / 1000; i += 1000) {
                    int key = random.nextInt(KEY_RANGE);
                    cache.remove(key);
                }
            } catch (Throwable t) {
                failures.add(t);
            } finally {
                latch.countDown();
            }
        }

        private static byte[] randomValue(Random random) {
            int valueSize = random.nextInt(MAX_VALUE_SIZE - MIN_VALUE_SIZE) + MIN_VALUE_SIZE;
            byte[] value = new byte[valueSize];
            random.nextBytes(value);
            return value;
        }
    }
}
