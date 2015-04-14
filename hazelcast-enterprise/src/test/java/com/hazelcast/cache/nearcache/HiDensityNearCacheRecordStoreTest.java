package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.impl.nativememory.HiDensityNativeMemoryNearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationServiceBuilder;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheRecordStoreTest extends NearCacheRecordStoreTestSupport {

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

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

}
