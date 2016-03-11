package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheHotRestartBackupTest extends AbstractCacheHotRestartTest {

    private static final int KEY_COUNT = 1000;

    private ICache<Integer, String> cache;

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {InMemoryFormat.BINARY, KEY_COUNT, false},
                {InMemoryFormat.NATIVE, KEY_COUNT, false}
        });
    }

    private int clusterSize;
    private int backupCount;

    @Override
    void setupInternal() {
        clusterSize = 3;
        backupCount = clusterSize - 1;
    }

    @Test
    public void test_whenClusterIsStable() throws Exception {
        HazelcastInstance[] instances = newInstances(clusterSize);
        warmUpPartitions(instances);

        for (HazelcastInstance instance : instances) {
            cache = createCache(instance, backupCount);
        }

        Random random = new Random();
        fillCacheAndRemoveRandom(random);

        waitAllForSafeState(instances);

        CacheConfig config = cache.getConfiguration(CacheConfig.class);
        assertExpectedTotalCacheSize(instances, config.getNameWithPrefix());
    }

    @Test
    public void test_afterMigration() throws Exception {
        HazelcastInstance hz = newHazelcastInstance();
        cache = createCache(hz, backupCount);

        Random random = new Random();
        fillCacheAndRemoveRandom(random);

        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        instances[0] = hz;

        for (int i = 1; i < clusterSize; i++) {
            HazelcastInstance instance = newHazelcastInstance();
            instances[i] = instance;
            createCache(instance, backupCount);
        }

        CacheConfig config = cache.getConfiguration(CacheConfig.class);
        assertExpectedTotalCacheSize(instances, config.getNameWithPrefix());
    }

    private void assertExpectedTotalCacheSize(final HazelcastInstance[] instances, final String nameWithPrefix) {
        int expectedSize = cache.size() * clusterSize;
        assertTrueEventually(new CacheOwnedEntryAssertTask(instances, nameWithPrefix, expectedSize));
    }

    private void fillCacheAndRemoveRandom(Random random) {
        for (int key = 0; key < KEY_COUNT; key++) {
            String value = randomString();
            cache.put(key, value);
        }
        cache.remove(0);
        for (int i = 0; i < KEY_COUNT / 10; i++) {
            final int key = random.nextInt(KEY_COUNT);
            cache.remove(key);
        }
        cache.put(0, randomString());
    }

    private static class CacheOwnedEntryAssertTask extends AssertTask {
        private final HazelcastInstance[] instances;
        private final String nameWithPrefix;
        private final int expectedSize;

        public CacheOwnedEntryAssertTask(HazelcastInstance[] instances, String nameWithPrefix, int expectedSize) {
            this.instances = instances;
            this.nameWithPrefix = nameWithPrefix;
            this.expectedSize = expectedSize;
        }

        @Override
        public void run() throws Exception {
            final AtomicInteger actualSize = new AtomicInteger();
            final int partitionCount = getNode(instances[0]).getPartitionService().getPartitionCount();

            final CountDownLatch latch = new CountDownLatch(instances.length * partitionCount);

            for (HazelcastInstance instance : instances) {
                NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
                InternalOperationService operationService = nodeEngine.getOperationService();
                EnterpriseCacheService service = nodeEngine.getService(EnterpriseCacheService.SERVICE_NAME);

                for (int i = 0; i < partitionCount; i++) {
                    operationService.execute(new PartitionedCacheSizeTask(service, nameWithPrefix, i, actualSize, latch));
                }
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(expectedSize, actualSize.get());
        }

    }

    // partition specific task to retrieve size of cache-record-store
    private static class PartitionedCacheSizeTask implements PartitionSpecificRunnable {
        private final EnterpriseCacheService service;
        private final String nameWithPrefix;
        private final int partitionId;
        private final AtomicInteger actualSize;
        private final CountDownLatch latch;

        public PartitionedCacheSizeTask(EnterpriseCacheService service, String nameWithPrefix, int partitionId,
                AtomicInteger actualSize, CountDownLatch latch) {
            this.service = service;
            this.nameWithPrefix = nameWithPrefix;
            this.partitionId = partitionId;
            this.actualSize = actualSize;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                ICacheRecordStore recordStore = service.getRecordStore(nameWithPrefix, partitionId);
                if (recordStore != null) {
                    int size = recordStore.size();
                    actualSize.addAndGet(size);
                }
            } finally {
                latch.countDown();
            }
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }
    }
}
