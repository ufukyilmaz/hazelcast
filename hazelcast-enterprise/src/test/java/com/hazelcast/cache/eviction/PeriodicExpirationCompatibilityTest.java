package com.hazelcast.cache.eviction;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestTaskExecutorUtil.runOnPartitionThread;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class PeriodicExpirationCompatibilityTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule clearTaskPeriodProperty = set(PROP_TASK_PERIOD_SECONDS, "1");

    private final int PARTITION_COUNT = 101;
    private final int KEY_RANGE = 5000;

    private CompatibilityTestHazelcastInstanceFactory factory = new CompatibilityTestHazelcastInstanceFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Override
    protected Config getConfig() {
        return super.getConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
    }

    @Test
    public void test() throws InterruptedException {
        final String cacheName = "testCache";
        factory = new CompatibilityTestHazelcastInstanceFactory();
        HazelcastInstance[] instances = factory.newInstances(getConfig());
        int currentVersionIndex = CompatibilityTestHazelcastInstanceFactory.getKnownPreviousVersionsCount();

        waitClusterForSafeState(instances[currentVersionIndex]);

        CachingProvider provider = createServerCachingProvider(instances[currentVersionIndex]);
        HazelcastCacheManager cacheManager = (HazelcastCacheManager) provider.getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, createCacheConfig());
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(cacheName);

        putRecords(cache, 20);

        assertNoRecordEventually(instances[currentVersionIndex], cacheNameWithPrefix, true);

        for (int i = 0; i < currentVersionIndex; i++) {
            instances[i].shutdown();
        }

        waitClusterForSafeState(instances[currentVersionIndex]);
        getClusterService(instances[currentVersionIndex]).changeClusterVersion(Version.of(CURRENT_VERSION));

        for (int i = 0; i < currentVersionIndex; i++) {
            instances[i] = factory.newHazelcastInstance(getConfig());
        }

        putRecords(cache, 30);

        for (int i = 0; i < factory.getCount(); i++) {
            assertNoRecordEventually(instances[i], cacheNameWithPrefix, false);
        }
    }

    private void assertNoRecordEventually(final HazelcastInstance instance, final String cacheNameWithPrefix, final boolean ownerOnly) {
        final CacheService service = getNodeEngineImpl(instance).getService(CacheService.SERVICE_NAME);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int ownedEntryCount = 0;
                for (int i = 0; i < PARTITION_COUNT; i++) {
                    NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
                    InternalPartition partition = nodeEngine.getPartitionService().getPartition(i);
                    if (ownerOnly && !partition.getReplicaAddress(0).equals(nodeEngine.getThisAddress())) {
                        // we check only owned records because old members do not send expiration operation for backups
                        continue;
                    }
                    final ICacheRecordStore recordStore = service.getRecordStore(cacheNameWithPrefix, i);
                    ownedEntryCount += runOnPartitionThread(instance, new Callable<Integer>() {
                        @Override
                        public Integer call() {
                            return recordStore == null ? 0 : recordStore.size();
                        }
                    }, i);
                }
                assertEquals(0, ownedEntryCount);
            }
        });
    }

    private void putRecords(final Cache<Integer, Integer> cache, int seconds) throws InterruptedException {
        final Random random = new Random();
        final AtomicBoolean done = new AtomicBoolean();
        Thread putter = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!done.get()) {
                    cache.put(random.nextInt(KEY_RANGE), random.nextInt(KEY_RANGE));
                }
            }
        });
        putter.start();
        sleepAtLeastSeconds(seconds);
        done.set(true);
        putter.join();
    }

    private MutableConfiguration<Integer, Integer> createCacheConfig() {
        Factory<RandomExpiryPolicy> expiryPolicyFactory = FactoryBuilder.factoryOf(new RandomExpiryPolicy(10));
        return new MutableConfiguration<Integer, Integer>().setExpiryPolicyFactory(expiryPolicyFactory);
    }

    static class RandomExpiryPolicy implements ExpiryPolicy, Serializable {

        private Random random = new Random();
        private final int maxExpirySeconds;

        RandomExpiryPolicy(int maxExpirySeconds) {
            this.maxExpirySeconds = maxExpirySeconds;
        }

        @Override
        public Duration getExpiryForCreation() {
            return new Duration(TimeUnit.SECONDS, random.nextInt(maxExpirySeconds));
        }

        @Override
        public Duration getExpiryForAccess() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Duration getExpiryForUpdate() {
            return new Duration(TimeUnit.SECONDS, random.nextInt(maxExpirySeconds));
        }
    }
}
