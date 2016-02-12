package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.hidensity.HiDensityStorageInfo;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.hotrestart.HotRestartException;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.eviction.HotRestartEvictionHelper.getHotRestartFreeNativeMemoryPercentage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartEvictionTest extends AbstractCacheHotRestartTest {

    private static final int KEY_RANGE = 1024 * 1024 * 32;
    private static final int MIN_VALUE_SIZE = 32;
    private static final int MAX_VALUE_SIZE = 4096;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.NATIVE, 25000, true},
                {InMemoryFormat.BINARY, 25000, true}
        });
    }

    @Test
    public void test() throws Exception {
        HazelcastInstance hz = newHazelcastInstance();
        ICache<Integer, byte[]> cache = createCache(hz);

        final int threadCount = 4;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final List<Throwable> failures = Collections.synchronizedList(new ArrayList<Throwable>());

        for (int i = 0; i < threadCount; i++) {
            new Thread(new CacheTask(keyRange, cache, latch, failures)).start();
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
        final int samples = 5000;
        Random random = new Random();
        Map<Integer, byte[]> expected = new HashMap<Integer, byte[]>(samples);
        for (int i = 0; i < samples; i++) {
            int key = random.nextInt(KEY_RANGE);
            byte[] value = cache.get(key);
            if (value != null) {
                expected.put(key, value);
            }
        }

        hz = restartHazelcastInstance(hz);
        cache = createCache(hz);

        assertEquals(expectedSize, cache.size());

        for (Map.Entry<Integer, byte[]> entry : expected.entrySet()) {
            byte[] expectedValue = entry.getValue();
            byte[] actualValue = cache.get(entry.getKey());
            assertArrayEquals("Expected: " + expectedValue.length, expectedValue, actualValue);
        }
    }

    private static byte[] randomValue(Random random) {
        int valueSize = random.nextInt(MAX_VALUE_SIZE - MIN_VALUE_SIZE) + MIN_VALUE_SIZE;
        final byte[] value = new byte[valueSize];
        random.nextBytes(value);
        return value;
    }

    @Override
    MemorySize getNativeMemorySize() {
        return new MemorySize(256, MemoryUnit.MEGABYTES);
    }

    private static class CacheTask implements Runnable {
        private final List<Throwable> failures;
        private final CountDownLatch latch;
        private final int operationCount;
        private final Cache cache;

        public CacheTask(int operationCount, Cache cache, CountDownLatch latch, List<Throwable> failures) {
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
    }

    @Test
    public void freeNativeMemoryPercentageCannotBeLessThanItsMinimumLimit() {
        if (memoryFormat == InMemoryFormat.NATIVE) {
            int freeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();
            HazelcastInstance hz = newHazelcastInstance();
            EvictionConfig evictionConfig =
                    new EvictionConfig(freeNativeMemoryPercentage / 2,
                            EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE,
                            EvictionPolicy.LRU);
            ICache<Integer, byte[]> cache = createCache(hz, evictionConfig);
            try {
                // Config is checked while creating cache record store, so to trigger it we are putting to cache.
                cache.put(1, new byte[1]);
                fail("Free native memory percentage cannot be less than " + freeNativeMemoryPercentage);
            } catch (IllegalArgumentException expected) {
                expected.printStackTrace();
            }
        }
    }

    @Test
    public void evictionShouldBeTriggeredWhenFreeNativeMemoryPercentageIsReached() {
        if (memoryFormat == InMemoryFormat.NATIVE) {
            int freeNativeMemoryPercentage = getHotRestartFreeNativeMemoryPercentage();
            HazelcastInstance hz = newHazelcastInstance();
            ICache cache = createCache(hz);

            Node node = TestUtil.getNode(hz);
            int partitionCount = node.getPartitionService().getPartitionCount();
            EnterpriseSerializationService ss =
                    (EnterpriseSerializationService) node.getSerializationService();
            MemoryStats memoryStats = ss.getMemoryManager().getMemoryStats();
            EnterpriseCacheService cacheService = node.nodeEngine.getService(ICacheService.SERVICE_NAME);
            HiDensityStorageInfo cacheInfo = cacheService.getOrCreateHiDensityCacheInfo("/hz/" + cache.getName());

            long maxNativeMemory = memoryStats.getMaxNativeMemory();
            long minFreeNativeMemory = (long) (maxNativeMemory
                    * freeNativeMemoryPercentage / 100f);
            int entrySize = 1024 * 1024; // 1MB
            long entryCountToFillUpMemory = maxNativeMemory / entrySize; // In fact, it is less than this

            byte[] smallValue = new byte[1];
            for (int i = 0; i < partitionCount; i++) {
                cache.put(generateKeyForPartition(hz, i), smallValue);
            }
            // Now there is at last one element in each partition,
            // so there is always evictable element in each partition.

            byte[] value = new byte[entrySize];
            for (int i = 0; i < entryCountToFillUpMemory; i++) {
                long forceEvictionCountBefore = cacheInfo.getForceEvictionCount();
                int sizeBefore = cache.size();

                long actualMinFreeNativeMemory = memoryStats.getFreeNativeMemory();
                boolean evictionShouldBeTriggeredBecauseOfMinFreeMemory = actualMinFreeNativeMemory < minFreeNativeMemory;

                int j = i % partitionCount;
                cache.put(generateKeyForPartition(hz, j), value);

                long forceEvictionCountAfter = cacheInfo.getForceEvictionCount();
                int sizeAfter = cache.size();

                if (forceEvictionCountBefore == forceEvictionCountAfter) {
                    // Force eviction might be applied to other partitions owned by same thread
                    // so these partitions might be totally empty.
                    // In this case, no eviction is applied to that partition based record store since there is no entry.
                    // At the moment, we simply finish the test but we can improve this test later.
                    break;
                }

                if (evictionShouldBeTriggeredBecauseOfMinFreeMemory) {
                    assertEquals("When eviction is triggered, size after put cannot be more than before put",
                            sizeAfter, sizeBefore);
                }
            }
        }
    }

}
