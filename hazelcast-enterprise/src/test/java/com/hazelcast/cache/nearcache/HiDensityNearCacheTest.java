package com.hazelcast.cache.nearcache;

import com.hazelcast.cache.hidensity.nearcache.HiDensityNearCache;
import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
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
public class HiDensityNearCacheTest extends NearCacheTestSupport {

    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

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
                        .build(),
                createNearCacheExecutor());
    }

    private NearCacheContext createNearCacheContext(MemoryManager memoryManager) {
        return new NearCacheContext(
                new EnterpriseSerializationServiceBuilder()
                        .setMemoryManager(memoryManager)
                        .build(),
                createNearCacheExecutor());
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
    protected NearCache<Integer, String> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                         ManagedNearCacheRecordStore nearCacheRecordStore) {
        return new HiDensityNearCache<Integer, String>(name,
                nearCacheConfig,
                createNearCacheContext(),
                nearCacheRecordStore);
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

    @Test(expected = NativeOutOfMemoryError.class)
    public void createEntryBiggerThanNativeMemory() {
        MemorySize memorySize = new MemorySize(32, MemoryUnit.MEGABYTES);
        NearCacheConfig nearCacheConfig =
                createNearCacheConfig(DEFAULT_NEAR_CACHE_NAME, InMemoryFormat.NATIVE);
        PoolingMemoryManager mm = new PoolingMemoryManager(memorySize);
        try {
            NearCacheContext nearCacheContext = createNearCacheContext(mm);
            NearCache<Integer, byte[]> nearCache =
                    new HiDensityNearCache<Integer, byte[]>(
                            DEFAULT_NEAR_CACHE_NAME,
                            nearCacheConfig,
                            nearCacheContext);
            byte[] value = new byte[(int) (2 * memorySize.bytes())];
            nearCache.put(1, value);
        } finally {
            mm.destroy();
        }
    }

}
