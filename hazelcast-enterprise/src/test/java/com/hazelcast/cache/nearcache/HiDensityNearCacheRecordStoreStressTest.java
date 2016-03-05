package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.impl.nativememory.HiDensityNativeMemoryNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HiDensityNearCacheRecordStoreStressTest extends NearCacheRecordStoreTestSupport {

    private static final int DEFAULT_MEMORY_SIZE_IN_MEGABYTES = 128;
    private static final MemorySize DEFAULT_MEMORY_SIZE =
            new MemorySize(DEFAULT_MEMORY_SIZE_IN_MEGABYTES, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE,
                                                 NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE,
                                                 NativeMemoryConfig.DEFAULT_PAGE_SIZE,
                                                 2 * NativeMemoryConfig.DEFAULT_METADATA_SPACE_PERCENTAGE);
        memoryManager.registerThread(Thread.currentThread());
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (memoryManager != null) {
            memoryManager.dispose();
            memoryManager = null;
        }
    }

    @Override
    protected NearCacheContext createNearCacheContext() {
        return new NearCacheContext(
                new EnterpriseSerializationServiceBuilder()
                        .setMemoryManager(memoryManager)
                        .build(), null);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(name, inMemoryFormat);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);
        nearCacheConfig.setEvictionConfig(evictionConfig);
        return nearCacheConfig;
    }

    @Override
    protected <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                           NearCacheContext nearCacheContext,
                                                                           InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
            case NATIVE:
                return new HiDensityNativeMemoryNearCacheRecordStore<K, V>(nearCacheConfig, nearCacheContext);
            default:
                return super.createNearCacheRecordStore(nearCacheConfig, nearCacheContext, inMemoryFormat);
        }
    }

    @Test
    public void evictWithUsedNativeMemorySizeMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU,
                                             EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithFreeNativeMemorySizeMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU,
                                             EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithUsedNativeMemoryPercentageMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU,
                                             EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithFreeNativeMemoryPercentageMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU,
                                             EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithUsedNativeMemorySizeMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithFreeNativeMemorySizeMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithUsedNativeMemoryPercentageMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithFreeNativeMemoryPercentageMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithIllegalPercentageArgumentFreeNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE,
                                             150);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithNegativePercentageArgumentFreeNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE,
                                             -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithIllegalPercentageArgumentUsedNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE,
                                             150);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithNegativePercentageArgumentUsedNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU,
                                             EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE,
                                             -1);
    }

    private void doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy evictionPolicy,
                                                      EvictionConfig.MaxSizePolicy maxSizePolicy) {
        doEvictionWithHiDensityMaxSizePolicy(evictionPolicy, maxSizePolicy, 99);
    }

    private void doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy evictionPolicy,
                                                      EvictionConfig.MaxSizePolicy maxSizePolicy,
                                                      int percentage) {
        AtomicBoolean shutdown = new AtomicBoolean(false);
        Future future = spawn(new NearCacheTestWorker(shutdown, evictionPolicy, maxSizePolicy, percentage));
        try {
            future.get(2, TimeUnit.MINUTES);
        } catch (Exception e1) {
            // Worker couldn't finish its job in time so terminate it forcefully
            shutdown.set(true);
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception e2) {
                ExceptionUtil.rethrow(e2);
            }
        }
    }

    private class NearCacheTestWorker implements Runnable {

        private AtomicBoolean shutdown;
        private EvictionPolicy evictionPolicy;
        private EvictionConfig.MaxSizePolicy maxSizePolicy;
        private int percentage;

        private NearCacheTestWorker(AtomicBoolean shutdown,
                                    EvictionPolicy evictionPolicy,
                                    EvictionConfig.MaxSizePolicy maxSizePolicy,
                                    int percentage) {
            this.shutdown = shutdown;
            this.evictionPolicy = evictionPolicy;
            this.maxSizePolicy = maxSizePolicy;
            this.percentage = percentage;
        }

        @Override
        public void run() {
            final int HUGE_RECORD_COUNT = 1000000;
            final int DEFAULT_SIZE = DEFAULT_MEMORY_SIZE_IN_MEGABYTES / 2;
            final int DEFAULT_PERCENTAGE = percentage;

            NearCacheConfig nearCacheConfig =
                    createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);

            if (evictionPolicy == null) {
                evictionPolicy = EvictionConfig.DEFAULT_EVICTION_POLICY;
            }
            EvictionConfig evictionConfig = new EvictionConfig();
            evictionConfig.setMaximumSizePolicy(maxSizePolicy);
            if (maxSizePolicy == EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE) {
                evictionConfig.setSize(DEFAULT_SIZE);
            } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE) {
                evictionConfig.setSize(DEFAULT_PERCENTAGE);
            } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE) {
                evictionConfig.setSize(DEFAULT_SIZE);
            } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE) {
                evictionConfig.setSize(DEFAULT_PERCENTAGE);
            }
            evictionConfig.setEvictionPolicy(evictionPolicy);
            nearCacheConfig.setEvictionConfig(evictionConfig);

            NearCacheContext nearCacheContext = createNearCacheContext();
            EnterpriseSerializationService serializationService =
                    (EnterpriseSerializationService) nearCacheContext.getSerializationService();
            HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();

            NearCacheRecordStore<Integer, String> nearCacheRecordStore =
                    createNearCacheRecordStore(
                            nearCacheConfig,
                            nearCacheContext,
                            InMemoryFormat.NATIVE);

            Random random = new Random();
            byte[] bytes = new byte[128];
            random.nextBytes(bytes);
            String value = new String(bytes);

            for (int i = 0; i < HUGE_RECORD_COUNT; i++) {
                if (shutdown.get()) {
                    break;
                }
                nearCacheRecordStore.put(i, value);

                nearCacheRecordStore.doEvictionIfRequired();

                MemoryStats memoryStats = memoryManager.getMemoryStats();
                if (maxSizePolicy == EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE) {
                    long usedNativeMemory = memoryStats.getUsedNative();
                    long maxAllowedNativeMemory = MemoryUnit.MEGABYTES.toBytes(DEFAULT_SIZE);
                    assertTrue(maxAllowedNativeMemory >= usedNativeMemory);
                } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE) {
                    long usedNativeMemory = memoryStats.getUsedNative();
                    long maxAllowedNativeMemory = (memoryStats.getMaxNative() * DEFAULT_PERCENTAGE) / 100;
                    assertTrue(maxAllowedNativeMemory >= usedNativeMemory);
                } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE) {
                    long freeNativeMemory = memoryStats.getFreeNative();
                    long minAllowedFreeMemory = MemoryUnit.MEGABYTES.toBytes(DEFAULT_SIZE);
                    assertTrue(freeNativeMemory >= minAllowedFreeMemory);
                } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE) {
                    long freeNativeMemory = memoryStats.getFreeNative();
                    long minAllowedFreeMemory = (memoryStats.getMaxNative() * DEFAULT_PERCENTAGE) / 100;
                    assertTrue(freeNativeMemory >= minAllowedFreeMemory);
                }
            }
        }

    }

}
