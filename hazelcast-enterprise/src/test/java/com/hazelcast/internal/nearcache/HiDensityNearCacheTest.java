package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static java.lang.Runtime.getRuntime;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheTest extends NearCacheTestSupport {

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

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

    private NearCacheManager newNearCacheManager() {
        return new HiDensityNearCacheManager(ess, executionService.getGlobalTaskScheduler(), null);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);

        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(name, inMemoryFormat);
        nearCacheConfig.setEvictionConfig(evictionConfig);

        return nearCacheConfig;
    }

    @Override
    protected NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return new HiDensityNearCache<Integer, String>(name, nearCacheConfig, newNearCacheManager(),
                nearCacheRecordStore, ss, executionService.getGlobalTaskScheduler(), null);
    }

    @Test
    public void getNearCacheName() {
        doGetNearCacheName();
    }

    @Test
    public void getFromNearCache() {
        doGetFromNearCache();
    }

    @Test
    public void putToNearCache() {
        doPutToNearCache();
    }

    @Test
    public void removeFromNearCache() {
        doRemoveFromNearCache();
    }

    @Test
    public void invalidateFromNearCache() {
        doInvalidateFromNearCache();
    }

    @Test
    public void configureInvalidateOnChangeForNearCache() {
        doConfigureInvalidateOnChangeForNearCache();
    }

    @Test
    public void clearNearCache() {
        doClearNearCache();
    }

    @Test
    public void destroyNearCache() {
        doDestroyNearCache();
    }

    @Test
    public void configureInMemoryFormatForNearCache() {
        doConfigureInMemoryFormatForNearCache();
    }

    @Test
    public void getNearCacheStatsFromNearCache() {
        doGetNearCacheStatsFromNearCache();
    }

    @Test
    public void selectToSaveFromNearCache() {
        doSelectToSaveFromNearCache();
    }

    @Test
    public void createNearCacheAndWaitForExpirationCalledWithTTL() {
        doCreateNearCacheAndWaitForExpirationCalled(true);
    }

    @Test
    public void createNearCacheAndWaitForExpirationCalledWithMaxIdleTime() {
        doCreateNearCacheAndWaitForExpirationCalled(false);
    }

    @Test
    public void putToNearCacheStatsAndSeeEvictionCheckIsDone() {
        doPutToNearCacheStatsAndSeeEvictionCheckIsDone();
    }

    @Test
    public void createEntryBiggerThanNativeMemory() {
        // given
        int estimatedNearCacheConcurrencyLevel = nextPowerOfTwo(8 * getRuntime().availableProcessors());
        int metaKbPerEmptyNearCacheSegment = 4;
        int metadataSizeToTotalNativeSizeFactor = 8;
        int size = estimatedNearCacheConcurrencyLevel * metaKbPerEmptyNearCacheSegment * metadataSizeToTotalNativeSizeFactor;
        MemorySize memorySize = new MemorySize(2 * size, MemoryUnit.KILOBYTES);
        NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        PoolingMemoryManager mm = new PoolingMemoryManager(memorySize);
        try {
            NearCache<Integer, byte[]> nearCache = new HiDensityNearCache<Integer, byte[]>(
                    DEFAULT_NEAR_CACHE_NAME,
                    nearCacheConfig, newNearCacheManager(),
                    ess, executionService.getGlobalTaskScheduler(), null);
            nearCache.initialize();

            byte[] value = new byte[(int) (2 * memorySize.bytes())];

            // when - then (just don't fail)
            nearCache.put(1, value);
        } finally {
            mm.dispose();
        }
    }
}
