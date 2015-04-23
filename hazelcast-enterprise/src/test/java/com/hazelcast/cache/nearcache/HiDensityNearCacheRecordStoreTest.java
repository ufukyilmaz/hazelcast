package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.impl.nativememory.HiDensityNativeMemoryNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    private static final int DEFAULT_MEMORY_SIZE_IN_MEGABYTES = 256;
    private static final MemorySize DEFAULT_MEMORY_SIZE =
            new MemorySize(DEFAULT_MEMORY_SIZE_IN_MEGABYTES, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;

    @Override
    protected NearCacheContext createNearCacheContext() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
        return new NearCacheContext(
                new EnterpriseSerializationServiceBuilder()
                        .setMemoryManager(memoryManager)
                        .build(), null);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(name, inMemoryFormat);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaxSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);
        nearCacheConfig.setEvictionConfig(evictionConfig);
        return nearCacheConfig;
    }

    @After
    public void tearDown() {
        if (memoryManager != null) {
            memoryManager.destroy();
            memoryManager = null;
        }
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
    public void putAndGetRecordFromHiDensityNativeMemoryNearCacheRecordStore() {
        putAndGetRecord(InMemoryFormat.NATIVE);
    }

    @Test
    public void putAndGetRecordSuccessfullyFromHiDensityNativeMemoryNearCacheRecordStore() {
        putAndGetRecord(InMemoryFormat.NATIVE);
    }

    @Test
    public void putAndRemoveRecordSuccessfullyFromHiDensityNativeMemoryNearCacheRecordStore() {
        putAndRemoveRecord(InMemoryFormat.NATIVE);
    }

    @Test
    public void clearRecordsSuccessfullyFromHiDensityNativeMemoryNearCacheRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.NATIVE, false);
    }

    @Test(expected = IllegalStateException.class)
    public void destroyStoreFromHiDensityNativeMemoryNearCacheRecordStore() {
        clearRecordsOrDestroyStoreFromNearCacheDataRecordStore(InMemoryFormat.NATIVE, true);
    }

    @Test
    public void statsCalculatedOnHiDensityNativeMemoryNearCacheRecordStore() {
        statsCalculated(InMemoryFormat.NATIVE);
    }

    @Test
    public void ttlEvaluatedSuccessfullyOnHiDensityNativeMemoryNearCacheRecordStore() {
        ttlEvaluated(InMemoryFormat.NATIVE);
    }

    @Test
    public void maxIdleTimeEvaluatedSuccessfullyOnHiDensityNativeMemoryNearCacheRecordStore() {
        maxIdleTimeEvaluatedSuccessfully(InMemoryFormat.NATIVE);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfTTLOnHiDensityNativeMemoryNearCacheRecordStore() {
        expiredRecordsCleanedUpSuccessfully(InMemoryFormat.NATIVE, false);
    }

    @Test
    public void expiredRecordsCleanedUpSuccessfullyBecauseOfIdleTimeOnHiDensityNativeMemoryNearCacheRecordStore() {
        expiredRecordsCleanedUpSuccessfully(InMemoryFormat.NATIVE, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateHiDensityNativeMemoryNearCacheRecordStoreWithEntryCountMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE,
                                         EvictionConfig.MaxSizePolicy.ENTRY_COUNT,
                                         1000);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithUsedNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE,
                                         EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE,
                                         1000000);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithFreeNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE,
                                         EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE,
                                         1000000);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithUsedNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE,
                                         EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE,
                                         99);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithFreeNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE,
                                         EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE,
                                         1);
    }

    @Test
    public void evictWithUsedNativeMemorySizeMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithFreeNativeMemorySizeMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithUsedNativeMemoryPercentageMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithFreeNativeMemoryPercentageMaxSizePolicyAndLRUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LRU, EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithUsedNativeMemorySizeMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithFreeNativeMemorySizeMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE);
    }

    @Test
    public void evictWithUsedNativeMemoryPercentageMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
    }

    @Test
    public void evictWithFreeNativeMemoryPercentageMaxSizePolicyAndLFUEvictionPolicyOnHiDensityNativeMemoryNearCacheRecordStore() {
        doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy.LFU, EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE);
    }

    private void doEvictionWithHiDensityMaxSizePolicy(EvictionPolicy evictionPolicy,
                                                      EvictionConfig.MaxSizePolicy maxSizePolicy) {
        final int HUGE_RECORD_COUNT = 1000000;
        final int DEFAULT_SIZE = DEFAULT_MEMORY_SIZE_IN_MEGABYTES / 2;
        final int DEFAULT_PERCENTAGE = 50;

        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);

        if (evictionPolicy == null) {
            evictionPolicy = EvictionConfig.DEFAULT_EVICTION_POLICY;
        }
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaxSizePolicy(maxSizePolicy);
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
        MemoryManager memoryManager = serializationService.getMemoryManager();

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
            nearCacheRecordStore.put(i, value);

            nearCacheRecordStore.doEvictionIfRequired();

            MemoryStats memoryStats = memoryManager.getMemoryStats();
            if (maxSizePolicy == EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_SIZE) {
                long usedNativeMemory = memoryStats.getUsedNativeMemory();
                long maxAllowedNativeMemory = MemoryUnit.MEGABYTES.toBytes(DEFAULT_SIZE);
                assertTrue(maxAllowedNativeMemory >= usedNativeMemory);
            } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE) {
                long usedNativeMemory = memoryStats.getUsedNativeMemory();
                long maxAllowedNativeMemory = (memoryStats.getMaxNativeMemory() * DEFAULT_PERCENTAGE) / 100;
                assertTrue(maxAllowedNativeMemory >= usedNativeMemory);
            } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE) {
                long freeNativeMemory = memoryStats.getFreeNativeMemory();
                long minAllowedFreeMemory = MemoryUnit.MEGABYTES.toBytes(DEFAULT_SIZE);
                assertTrue(freeNativeMemory >= minAllowedFreeMemory);
            } else if (maxSizePolicy == EvictionConfig.MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE) {
                long freeNativeMemory = memoryStats.getFreeNativeMemory();
                long minAllowedFreeMemory = (memoryStats.getMaxNativeMemory() * DEFAULT_PERCENTAGE) / 100;
                assertTrue(freeNativeMemory >= minAllowedFreeMemory);
            }
        }
    }

}
