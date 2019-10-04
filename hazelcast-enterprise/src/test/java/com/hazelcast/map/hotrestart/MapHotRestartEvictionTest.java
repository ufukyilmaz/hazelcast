package com.hazelcast.map.hotrestart;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.hotrestart.HotRestartException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapHotRestartEvictionTest extends AbstractMapHotRestartTest {

    private static final int KEY_RANGE = 1024 * 1024 * 32;
    private static final int MIN_VALUE_SIZE = 32;
    private static final int MAX_VALUE_SIZE = 4096;

    @Parameters(name = "memoryFormat:{0} fsync:{2} encrypted:{4}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.NATIVE, 100000, false, true, false},
                {InMemoryFormat.BINARY, 100000, false, true, false},
                {InMemoryFormat.NATIVE, 100000, false, true, true},
        });
    }

    @Override
    MemorySize getNativeMemorySize() {
        return new MemorySize(256, MemoryUnit.MEGABYTES);
    }

    @Test
    public void testEviction() {
        HazelcastInstance hz = newHazelcastInstance();
        IMap<Integer, byte[]> map = createMap(hz);

        int threadCount = 4;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Throwable> failures = synchronizedList(new ArrayList<Throwable>());

        for (int i = 0; i < threadCount; i++) {
            spawn(new MapTask(keyRange, map, latch, failures));
        }
        assertOpenEventually(latch, TimeUnit.MINUTES.toSeconds(10));

        if (!failures.isEmpty()) {
            throw new HotRestartException(failures.size() + " failures!", failures.get(0));
        }

        int iterated = 0;
        int expectedSize = map.size();
        for (Map.Entry<Integer, byte[]> entry : map.entrySet()) {
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
            byte[] value = map.get(key);
            if (value != null) {
                expected.put(key, value);
            }
        }

        hz = restartInstances(1)[0];
        map = createMap(hz);

        assertEquals(expectedSize, map.size());

        for (Map.Entry<Integer, byte[]> entry : expected.entrySet()) {
            byte[] expectedValue = entry.getValue();
            byte[] actualValue = map.get(entry.getKey());
            assertArrayEquals("Expected: " + expectedValue.length, expectedValue, actualValue);
        }
    }

    private static class MapTask implements Runnable {

        private final List<Throwable> failures;
        private final CountDownLatch latch;
        private final int operationCount;
        private final IMap<Integer, byte[]> map;

        MapTask(int operationCount, IMap<Integer, byte[]> map, CountDownLatch latch, List<Throwable> failures) {
            this.operationCount = operationCount;
            this.map = map;
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
                    map.put(key, value);
                }
                for (int i = 0; i < operationCount / 1000; i += 1000) {
                    int key = random.nextInt(KEY_RANGE);
                    map.remove(key);
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
