package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCacheManager;
import com.hazelcast.cache.hidensity.nearcache.impl.nativememory.HiDensityNativeMemoryNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    private static final int DEFAULT_MEMORY_SIZE_IN_MEGABYTES = 256;
    private static final MemorySize DEFAULT_MEMORY_SIZE =
            new MemorySize(DEFAULT_MEMORY_SIZE_IN_MEGABYTES, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (memoryManager != null) {
            memoryManager.destroy();
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

}
