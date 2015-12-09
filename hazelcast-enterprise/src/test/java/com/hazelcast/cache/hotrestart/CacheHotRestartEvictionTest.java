package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartEvictionTest extends AbstractCacheHotRestartTest {

    private static final int KEY_RANGE = 1024 * 1024 * 32;
    private static final int MIN_VALUE_SIZE = 32;
    private static final int MAX_VALUE_SIZE = 4096;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
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
}
