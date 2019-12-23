package com.hazelcast.internal.nearcache;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDNearCacheTest extends NearCacheTestSupport {

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
        return new EnterpriseNearCacheManager(ess,
                executionService.getGlobalTaskScheduler(), null, properties);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String name, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createNearCacheConfig(name, inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return new HDNearCache<Integer, String>(name, nearCacheConfig, newNearCacheManager(),
                nearCacheRecordStore, ss, executionService.getGlobalTaskScheduler(), null, properties);
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
        ILogger logger = Logger.getLogger(com.hazelcast.map.impl.nearcache.HDNearCacheTest.class);

        // given
        int estimatedNearCacheConcurrencyLevel = nextPowerOfTwo(8 * RuntimeAvailableProcessors.get());
        logger.info("Using estimatedNearCacheConcurrencyLevel " + estimatedNearCacheConcurrencyLevel);
        int metaKbPerEmptyNearCacheSegment = 4;
        int metadataSizeToTotalNativeSizeFactor = 8;
        int size = estimatedNearCacheConcurrencyLevel * metaKbPerEmptyNearCacheSegment * metadataSizeToTotalNativeSizeFactor;
        MemorySize memorySize = new MemorySize(2 * size, MemoryUnit.KILOBYTES);

        PoolingMemoryManager mm = new PoolingMemoryManager(memorySize);
        EnterpriseSerializationService ess = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(mm)
                .build();

        try {
            NearCacheConfig nearCacheConfig = createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
            NearCache<Integer, byte[]> nearCache
                    = createHDNearCache(ess, executionService.getGlobalTaskScheduler(),
                    null, nearCacheConfig, properties);
            String valueSize = formatMegaBytes(2 * memorySize.bytes());
            logger.info("Allocating value with size: " + valueSize);
            byte[] value = new byte[(int) (2 * memorySize.bytes())];
            // when - then (just don't fail)
            logger.info("Serializing value with size " + valueSize + " free memory before: " + getFreeMemoryStr());
            // bypassing serialization service for creating HeapData to prevent
            // allocating another byte[] by SS that in edge cases led to OOME
            // if there were not big enough consecutive heap space for the array
            Data valueData = new HeapData(value);
            logger.info("Serialized size: " + formatMegaBytes(valueData.totalSize())
                    + " free memory after: " + getFreeMemoryStr());
            nearCache.put(1, ess.toData(1), value, valueData);
        } finally {
            logger.info("Free memory in finally: " + getFreeMemoryStr());
            mm.dispose();
        }
    }

    private String getFreeMemoryStr() {
        return formatMegaBytes(Runtime.getRuntime().freeMemory());
    }

    private String formatMegaBytes(long size) {
        return size / 1024 / 1024 + "MB";
    }

    public static <K, V> NearCache<K, V> createHDNearCache(EnterpriseSerializationService ess,
                                                           TaskScheduler taskScheduler,
                                                           NearCacheRecordStore recordStore,
                                                           NearCacheConfig nearCacheConfig,
                                                           HazelcastProperties properties) {
        NearCache<K, V> nearCache = new HDNearCache<K, V>(DEFAULT_NEAR_CACHE_NAME,
                nearCacheConfig,
                new EnterpriseNearCacheManager(ess, taskScheduler, null, properties),
                recordStore,
                ess,
                taskScheduler,
                null,
                properties);

        nearCache.initialize();

        return nearCache;
    }
}
