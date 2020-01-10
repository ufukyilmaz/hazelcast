package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.nativememory.HDNearCacheRecordStoreImpl;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.internal.hidensity.HiDensityRecordStore.DEFAULT_FORCED_EVICTION_PERCENTAGE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDNearCacheRecordStoreStressTest extends NearCacheRecordStoreTestSupport {

    private static final int DEFAULT_MEMORY_SIZE_IN_MEGABYTES = 128;
    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(DEFAULT_MEMORY_SIZE_IN_MEGABYTES, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService ess;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE,
                NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE,
                NativeMemoryConfig.DEFAULT_PAGE_SIZE,
                2 * NativeMemoryConfig.DEFAULT_METADATA_SPACE_PERCENTAGE);
        memoryManager.registerThread(Thread.currentThread());
        ess = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();
    }

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.dispose();
            memoryManager = null;
        }
    }

    @Override
    NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createNearCacheConfig(name, inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig, InMemoryFormat inMemoryFormat) {
        if (inMemoryFormat != NATIVE) {
            return super.createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
        }
        NearCacheRecordStore<K, V> recordStore = new HDNearCacheRecordStoreImpl<K, V>(nearCacheConfig, ess,
                null, DEFAULT_FORCED_EVICTION_PERCENTAGE);
        recordStore.initialize();
        return recordStore;
    }

    @Test
    public void evictWithUsedNativeMemorySizeMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithFreeNativeMemorySizeMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithUsedNativeMemoryPercentageMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithFreeNativeMemoryPercentageMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithUsedNativeMemorySizeMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithFreeNativeMemorySizeMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithUsedNativeMemoryPercentageMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithFreeNativeMemoryPercentageMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithIllegalPercentageArgumentFreeNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE, 150);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithNegativePercentageArgumentFreeNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithIllegalPercentageArgumentUsedNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, 150);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictWithNegativePercentageArgumentUsedNativeMemory() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, -1);
    }

    private void doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy evictionPolicy, MaxSizePolicy maxSizePolicy) {
        doEvictionWithHiDensityMaxSizePolicy(evictionPolicy, maxSizePolicy, 99);
    }

    private void doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy evictionPolicy, MaxSizePolicy maxSizePolicy,
                                                      int percentage) {
        AtomicBoolean shutdown = new AtomicBoolean(false);
        Future future = spawn(new NearCacheTestWorker(shutdown, evictionPolicy, maxSizePolicy, percentage));
        try {
            future.get(2, TimeUnit.MINUTES);
        } catch (Exception e1) {
            // worker couldn't finish its job in time so terminate it forcefully
            shutdown.set(true);
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (Exception e2) {
                throw rethrow(e2);
            }
        }
    }

    private class NearCacheTestWorker implements Runnable {

        private final AtomicBoolean shutdown;
        private final EvictionPolicy evictionPolicy;
        private final MaxSizePolicy maxSizePolicy;
        private final int percentage;

        private NearCacheTestWorker(AtomicBoolean shutdown, EvictionPolicy evictionPolicy, MaxSizePolicy maxSizePolicy,
                                    int percentage) {
            this.shutdown = shutdown;
            this.evictionPolicy = evictionPolicy == null ? EvictionConfig.DEFAULT_EVICTION_POLICY : evictionPolicy;
            this.maxSizePolicy = maxSizePolicy;
            this.percentage = percentage;
        }

        @Override
        public void run() {
            int hugeRecordCount = 1000000;
            int defaultSize = DEFAULT_MEMORY_SIZE_IN_MEGABYTES / 2;
            int defaultPercentage = percentage;

            EvictionConfig evictionConfig = new EvictionConfig()
                    .setEvictionPolicy(evictionPolicy)
                    .setMaxSizePolicy(maxSizePolicy);
            if (maxSizePolicy == MaxSizePolicy.USED_NATIVE_MEMORY_SIZE) {
                evictionConfig.setSize(defaultSize);
            } else if (maxSizePolicy == MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE) {
                evictionConfig.setSize(defaultPercentage);
            } else if (maxSizePolicy == MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE) {
                evictionConfig.setSize(defaultSize);
            } else if (maxSizePolicy == MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE) {
                evictionConfig.setSize(defaultPercentage);
            }

            NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, NATIVE)
                    .setEvictionConfig(evictionConfig);

            HazelcastMemoryManager memoryManager = ess.getMemoryManager();

            NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(nearCacheConfig, NATIVE);

            Random random = new Random();
            byte[] bytes = new byte[128];
            random.nextBytes(bytes);
            String value = new String(bytes);

            for (int i = 0; i < hugeRecordCount; i++) {
                if (shutdown.get()) {
                    break;
                }
                nearCacheRecordStore.put(i, ess.toData(i), value, ess.toData(value));

                nearCacheRecordStore.doEviction(false);

                MemoryStats memoryStats = memoryManager.getMemoryStats();
                if (maxSizePolicy == MaxSizePolicy.USED_NATIVE_MEMORY_SIZE) {
                    long usedNativeMemory = memoryStats.getUsedNative();
                    long maxAllowedNativeMemory = MemoryUnit.MEGABYTES.toBytes(defaultSize);
                    assertTrue(maxAllowedNativeMemory >= usedNativeMemory);
                } else if (maxSizePolicy == MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE) {
                    long usedNativeMemory = memoryStats.getUsedNative();
                    long maxAllowedNativeMemory = (memoryStats.getMaxNative() * defaultPercentage) / 100;
                    assertTrue(maxAllowedNativeMemory >= usedNativeMemory);
                } else if (maxSizePolicy == MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE) {
                    long freeNativeMemory = memoryStats.getFreeNative();
                    long minAllowedFreeMemory = MemoryUnit.MEGABYTES.toBytes(defaultSize);
                    assertTrue(freeNativeMemory >= minAllowedFreeMemory);
                } else if (maxSizePolicy == MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE) {
                    long freeNativeMemory = memoryStats.getFreeNative();
                    long minAllowedFreeMemory = (memoryStats.getMaxNative() * defaultPercentage) / 100;
                    assertTrue(freeNativeMemory >= minAllowedFreeMemory);
                }
            }
        }
    }
}
