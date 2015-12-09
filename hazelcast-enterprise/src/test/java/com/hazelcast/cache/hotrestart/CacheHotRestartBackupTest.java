package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertEquals;

@RunParallel
@RunWith(HazelcastTestRunner.class)
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
    private ICache[] caches;

    @Override
    void setupInternal() {
        clusterSize = 3;
        backupCount = clusterSize - 1;
        caches = new ICache[clusterSize];
    }

    @Test
    public void test_whenClusterIsStable() throws Exception {
        HazelcastInstance[] instances = newInstances(clusterSize);
        warmUpPartitions(instances);

        int k = 0;
        for (HazelcastInstance instance : instances) {
            caches[k++] = createCache(instance, backupCount);
        }
        cache = caches[caches.length - 1];

        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            fillCacheAndRemoveRandom(random);
        }

        waitAllForSafeState(instances);

        assertExpectedTotalCacheSize(caches);
    }

    @Test
    public void test_afterMigration() throws Exception {
        HazelcastInstance hz = newHazelcastInstance();
        cache = createCache(hz, backupCount);

        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            fillCacheAndRemoveRandom(random);
        }

        ICache[] caches = new ICache[clusterSize];
        caches[0] = cache;
        for (int i = 1; i < clusterSize; i++) {
            HazelcastInstance instance = newHazelcastInstance();
            caches[i] = createCache(instance, backupCount);
        }

        assertExpectedTotalCacheSize(caches);
    }

    private void assertExpectedTotalCacheSize(final ICache[] caches) {
        final int expectedSize = cache.size() * clusterSize;

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int actualSize = 0;
                for (ICache c : caches) {
                    long ownedEntryCount = c.getLocalCacheStatistics().getOwnedEntryCount();
                    actualSize += ownedEntryCount;
                }
                assertEquals(expectedSize, actualSize);
            }
        });
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
}
