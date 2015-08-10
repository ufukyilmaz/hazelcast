package com.hazelcast.cache;

import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecord;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.function.LongLongConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.memory.MemorySize.toPrettyString;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class CacheMemoryLeakStressTest extends HazelcastTestSupport {

    static {
        System.setProperty(StandardMemoryManager.PROPERTY_DEBUG_ENABLED, "true");
    }

    private static final long TIMEOUT = TimeUnit.SECONDS.toMillis(60);
    private static final MemoryAllocatorType ALLOCATOR_TYPE = MemoryAllocatorType.STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Test
    public void test() throws InterruptedException {
        final Config config = new Config();
        NativeMemoryConfig memoryConfig = config.getNativeMemoryConfig();
        memoryConfig.setEnabled(true).setAllocatorType(ALLOCATOR_TYPE).setSize(MEMORY_SIZE);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        CacheManager cacheManager = HazelcastServerCachingProvider.createCachingProvider(hz).getCacheManager();
        final String cacheName = randomName();
        final ICache cache = (ICache) cacheManager.createCache(cacheName, getConfiguration());

        // warm-up
        cache.size();

        final AtomicBoolean done = new AtomicBoolean(false);

        final Thread bouncingThread = new Thread() {
            public void run() {
                while (!done.get()) {
                    HazelcastInstance hz = factory.newHazelcastInstance(config);
                    sleepSeconds(10);
                    terminateInstance(hz);
                    sleepSeconds(5);
                }
            }
        };
        bouncingThread.start();

        final int threads = 8;
        final CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            new WorkerThread(cache, latch).start();
        }

        assertOpenEventually(latch, TIMEOUT * 2);
        done.set(true);
        bouncingThread.join();

        cache.destroy();

        try {
            assertTrueEventually(new AssertFreeMemoryTask(hz, hz2), 10);
        } catch (AssertionError e) {
            dumpNativeMemory(hz);
            dumpNativeMemory(hz2);
            throw e;
        }
    }

    private static CacheConfig getConfiguration() {
        return new CacheConfig().setBackupCount(1).setInMemoryFormat(InMemoryFormat.NATIVE).setEvictionConfig(
                new EvictionConfig().setSize(95).setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE));
    }

    private static class WorkerThread extends Thread {
        private final ICache cache;
        private final CountDownLatch latch;
        private final Random rand = new Random();

        public WorkerThread(ICache cache, CountDownLatch latch) {
            this.cache = cache;
            this.latch = latch;
        }

        public void run() {
            final int keyRange = 10000000;

            int counter = 0;
            long start = System.currentTimeMillis();
            while (true) {
                try {
                    int key = rand.nextInt(keyRange);
                    int op = rand.nextInt(10);
                    doOp(op, key);
                } catch (NativeOutOfMemoryError e) {
                    EmptyStatement.ignore(e);
                }

                if (++counter % 5000 == 0 && (start + TIMEOUT) < System.currentTimeMillis()) {
                    break;
                }
            }
            latch.countDown();
        }

        private void doOp(int op, int key) {
            switch (op) {
                case 0:
                    cache.put(key, newValue());
                    break;

                case 1:
                    cache.remove(key);
                    break;

                case 2:
                    cache.replace(key, newValue());
                    break;

                case 4:
                    cache.putIfAbsent(key, newValue());
                    break;

                case 5:
                    cache.getAndPut(key, newValue());
                    break;

                case 6:
                    cache.getAndRemove(key);
                    break;

                case 7:
                    cache.getAndReplace(key, newValue());
                    break;

                case 8:
                    Object current = cache.get(key);
                    if (current != null) {
                        cache.replace(key, current, newValue());
                    } else {
                        cache.replace(key, newValue());
                    }
                    break;

                case 9:
                    current = cache.get(key);
                    if (current != null) {
                        cache.remove(key, current);
                    } else {
                        cache.remove(key);
                    }
                    break;

                default:
                    cache.put(key, newValue());
            }
        }

        private byte[] newValue() {
            byte[] value = new byte[16 + rand.nextInt(1 << 12)]; // up to 4k
            rand.nextBytes(value);
            return value;
        }
    }

    private static class AssertFreeMemoryTask extends AssertTask {
        final MemoryStats memoryStats;
        final MemoryStats memoryStats2;

        public AssertFreeMemoryTask(HazelcastInstance hz, HazelcastInstance hz2) {
            memoryStats = getNode(hz).hazelcastInstance.getMemoryStats();
            memoryStats2 = getNode(hz2).hazelcastInstance.getMemoryStats();
        }

        @Override
        public void run() throws Exception {
            String message =
                    "Node1: " + toPrettyString(memoryStats.getUsedNativeMemory())
                    + ", Node2: " + toPrettyString(memoryStats2.getUsedNativeMemory());

            Assert.assertEquals(message, 0, memoryStats.getUsedNativeMemory());
            Assert.assertEquals(message, 0, memoryStats2.getUsedNativeMemory());
        }
    }

    private static void dumpNativeMemory(HazelcastInstance hz) {
        Node node = getNode(hz);
        EnterpriseSerializationService ss = (EnterpriseSerializationService) node.getSerializationService();
        MemoryManager memoryManager = ss.getMemoryManager();

        if (!(memoryManager instanceof StandardMemoryManager)) {
            System.err.println("Cannot dump memory for " + memoryManager);
            return;
        }

        StandardMemoryManager standardMemoryManager = (StandardMemoryManager) memoryManager;
        standardMemoryManager.forEachAllocatedBlock(new LongLongConsumer() {

            private int k;

            @Override
            public void accept(long key, long value) {
                if (value == HiDensityNativeMemoryCacheRecord.SIZE) {
                    HiDensityNativeMemoryCacheRecord record = new HiDensityNativeMemoryCacheRecord(null, key);
                    System.err.println(
                            (++k) + ". Record Address: " + key + " (Value Address: " + record.getValueAddress() + ")");
                } else if (value == 13) {
                    System.err.println((++k) + ". Key Address: " + key);
                } else {
                    System.err.println((++k) + ". Value Address: " + key + ", size: " + value);
                }
            }
        });
    }
}
