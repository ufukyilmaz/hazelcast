package com.hazelcast.cache.recordstore;

import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cache.impl.hidensity.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNativeMemoryCacheRecordStoreTest extends CacheRecordStoreTestSupport {

    private static final MemorySize NATIVE_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Override
    protected Config createConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setSize(NATIVE_MEMORY_SIZE)
                .setEnabled(true);

        return super.createConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected CacheConfig createCacheConfig(String cacheName, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createCacheConfig(cacheName, inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected ICacheRecordStore createCacheRecordStore(HazelcastInstance instance, String cacheName,
                                                       int partitionId, InMemoryFormat inMemoryFormat) {
        ICacheService cacheService = getCacheService(instance);
        CacheConfig cacheConfig = createCacheConfig(cacheName, inMemoryFormat);
        cacheService.putCacheConfigIfAbsent(cacheConfig);

        return new HiDensityNativeMemoryCacheRecordStore(partitionId, CACHE_NAME_PREFIX + cacheName,
                (EnterpriseCacheService) cacheService, getNodeEngineImpl(instance));
    }

    @Test
    public void putObjectAndGetNativeDataFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.NATIVE);
        putAndGetFromCacheRecordStore(cacheRecordStore, InMemoryFormat.NATIVE);
    }

    @Test
    public void putObjectAndGetNativeDataExpiryPolicyFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.NATIVE);
        putAndSetExpiryPolicyFromRecordStore(cacheRecordStore, InMemoryFormat.NATIVE);
    }
}
