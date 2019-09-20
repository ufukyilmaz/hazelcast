package com.hazelcast.elastic.map;

import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.FutureUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ConcurrentElasticHashMapTest extends HazelcastTestSupport {

    private static final int KEY_RANGE = 1000;

    private static final Random random = new Random();

    private static HazelcastMemoryManager memoryManager;
    private ConcurrentElasticHashMap<Integer, Integer> map;

    @Before
    public void setUp() {
        memoryManager = new PoolingMemoryManager(new MemorySize(32, MemoryUnit.MEGABYTES));
        EnterpriseSerializationService serializationService = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
        map = new ConcurrentElasticHashMap<Integer, Integer>(serializationService, memoryManager);
    }

    @After
    public void tearDown() {
        memoryManager.dispose();
    }

    private static int newKey(int keyRange) {
        return random.nextInt(keyRange);
    }

    private static int getValueOfKey(int key) {
        return key * key;
    }

    private static WorkerOperationType generateWorkerType(int id) {
        return WorkerOperationType.values()[id % WorkerOperationType.values().length];
    }

    @Test
    public void test_basicOperations() throws Exception {
        final int WORKER_COUNT = 2 * WorkerOperationType.values().length;

        List<Future> futures = new ArrayList<Future>(WORKER_COUNT);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch completedLatch = new CountDownLatch(WORKER_COUNT);

        for (int i = 0; i < WORKER_COUNT; i++) {
            futures.add(spawn(new CEHMWorker(generateWorkerType(i), map, stop, completedLatch)));
        }

        sleepAtLeastMillis(TimeUnit.SECONDS.toMillis(10));

        stop.set(true);
        completedLatch.await();

        FutureUtil.checkAllDone(futures);

        int size = map.size();
        if (size > 0) {
            assertFalse(map.isEmpty());
        } else {
            assertTrue(map.isEmpty());
        }

        map.clear();

        assertEquals(0, map.size());

        map.dispose();
    }

    enum WorkerOperationType {

        GET,
        PUT,
        PUT_IF_ABSENT,
        SET,
        REPLACE,
        REMOVE,
        CONTAINS
    }

    private static class CEHMWorker implements Runnable {

        private final WorkerOperationType workerOperationType;
        private final ConcurrentElasticHashMap<Integer, Integer> map;
        private final AtomicBoolean stop;
        private final CountDownLatch completedLatch;

        private CEHMWorker(WorkerOperationType workerOperationType,
                           ConcurrentElasticHashMap<Integer, Integer> map,
                           AtomicBoolean stop,
                           CountDownLatch completedLatch) {
            this.workerOperationType = workerOperationType;
            this.map = map;
            this.stop = stop;
            this.completedLatch = completedLatch;
        }

        @Override
        public void run() {
            try {
                switch (workerOperationType) {
                    case GET:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            Integer value = map.get(key);
                            if (value != null) {
                                assertEquals(value.intValue(), getValueOfKey(key));
                            }
                        }
                        break;

                    case PUT:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            int value = getValueOfKey(key);
                            Integer oldValue = map.put(key, value);
                            if (oldValue != null) {
                                assertEquals(oldValue.intValue(), value);
                            }
                        }
                        break;

                    case PUT_IF_ABSENT:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            int value = getValueOfKey(key);
                            Integer oldValue = map.putIfAbsent(key, value);
                            if (oldValue != null) {
                                assertEquals(oldValue.intValue(), value);
                            }
                        }
                        break;

                    case SET:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            int value = getValueOfKey(key);
                            map.set(key, value);
                        }
                        break;

                    case REMOVE:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            Integer oldValue = map.remove(key);
                            if (oldValue != null) {
                                assertEquals(oldValue.intValue(), getValueOfKey(key));
                            }
                        }
                        break;

                    case REPLACE:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            int value = getValueOfKey(key);
                            Integer oldValue = map.replace(key, value);
                            if (oldValue != null) {
                                assertEquals(oldValue.intValue(), value);
                            }
                        }
                        break;

                    case CONTAINS:
                        while (!stop.get()) {
                            int key = newKey(KEY_RANGE);
                            int value = getValueOfKey(key);
                            map.containsKey(key);
                            map.containsValue(value);
                        }
                        break;
                }
            } finally {
                completedLatch.countDown();
            }
        }
    }
}
