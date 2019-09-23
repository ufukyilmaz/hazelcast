package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.nearcache.impl.nativememory.NativeMemoryNearCacheRecordStore;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    private static final int DEFAULT_MEMORY_SIZE_IN_MEGABYTES = 256;
    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(DEFAULT_MEMORY_SIZE_IN_MEGABYTES, MemoryUnit.MEGABYTES);

    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService ess;

    @Before
    public void setup() {
        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
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
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createNearCacheConfig(name, inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                           InMemoryFormat inMemoryFormat) {
        NearCacheRecordStore<K, V> recordStore;
        switch (inMemoryFormat) {
            case NATIVE:
                recordStore = new NativeMemoryNearCacheRecordStore<K, V>(nearCacheConfig, ess, null);
                break;
            default:
                recordStore = super.createNearCacheRecordStore(nearCacheConfig, inMemoryFormat);
        }
        recordStore.initialize();

        return recordStore;
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
        clearRecordsOrDestroyStore(InMemoryFormat.NATIVE, false);
    }

    @Ignore
    @Test(expected = IllegalStateException.class)
    public void destroyStoreFromHiDensityNativeMemoryNearCacheRecordStore() {
        clearRecordsOrDestroyStore(InMemoryFormat.NATIVE, true);
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

    protected void expiredRecordsCleanedUpSuccessfully(InMemoryFormat inMemoryFormat, boolean useIdleTime) {
        int cleanUpThresholdSeconds = 3;

        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, inMemoryFormat);
        if (useIdleTime) {
            nearCacheConfig.setMaxIdleSeconds(cleanUpThresholdSeconds);
        } else {
            nearCacheConfig.setTimeToLiveSeconds(cleanUpThresholdSeconds);
        }

        final NearCacheRecordStore<Integer, String> nearCacheRecordStore = createNearCacheRecordStore(
                nearCacheConfig, inMemoryFormat);

        for (int i = 0; i < DEFAULT_RECORD_COUNT; i++) {
            nearCacheRecordStore.put(i, null, "Record-" + i, null);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                nearCacheRecordStore.doExpiration();

                assertEquals(0, nearCacheRecordStore.size());

                NearCacheStats nearCacheStats = nearCacheRecordStore.getNearCacheStats();
                assertEquals(0, nearCacheStats.getOwnedEntryCount());
                assertEquals(0, nearCacheStats.getOwnedEntryMemoryCost());
            }
        });
    }


    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithEntryCountMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE, MaxSizePolicy.ENTRY_COUNT, 1000);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithUsedNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE, MaxSizePolicy.USED_NATIVE_MEMORY_SIZE, 1000000);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithFreeNativeMemorySizeMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE, MaxSizePolicy.FREE_NATIVE_MEMORY_SIZE, 1000000);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithUsedNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE, USED_NATIVE_MEMORY_PERCENTAGE, 99);
    }

    @Test
    public void canCreateHiDensityNativeMemoryNearCacheRecordStoreWithFreeNativeMemoryPercentageMaxSizePolicy() {
        createNearCacheWithMaxSizePolicy(InMemoryFormat.NATIVE, MaxSizePolicy.FREE_NATIVE_MEMORY_PERCENTAGE, 1);
    }
}
