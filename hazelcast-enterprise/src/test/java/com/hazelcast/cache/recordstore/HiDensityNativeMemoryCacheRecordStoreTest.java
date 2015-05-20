package com.hazelcast.cache.recordstore;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.hidensity.impl.nativememory.HiDensityNativeMemoryCacheRecordStore;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheEvictionConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNativeMemoryCacheRecordStoreTest
        extends CacheRecordStoreTestSupport {

    private static final MemorySize NATIVE_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        NativeMemoryConfig nativeMemoryConfig =
                new NativeMemoryConfig()
                        .setSize(NATIVE_MEMORY_SIZE)
                        .setEnabled(true);
        config.setNativeMemoryConfig(nativeMemoryConfig);
        return config;
    }

    @Override
    protected CacheConfig createCacheConfig(String cacheName, InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = super.createCacheConfig(cacheName, inMemoryFormat);
        CacheEvictionConfig evictionConfig = new CacheEvictionConfig();
        evictionConfig.setMaxSizePolicy(CacheEvictionConfig.CacheMaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    @Override
    protected ICacheRecordStore createCacheRecordStore(HazelcastInstance instance, String cacheName,
                                                       int partitionId, InMemoryFormat inMemoryFormat) {
        NodeEngine nodeEngine = getNodeEngine(instance);
        ICacheService cacheService = getCacheService(instance);
        CacheConfig cacheConfig = createCacheConfig(cacheName, inMemoryFormat);
        cacheService.createCacheConfigIfAbsent(cacheConfig);
        return new HiDensityNativeMemoryCacheRecordStore(partitionId, CACHE_NAME_PREFIX + cacheName,
                (EnterpriseCacheService) cacheService, nodeEngine);
    }

    @Test
    public void putObjectAndGetNativeDataFromCacheRecordStore() {
        ICacheRecordStore cacheRecordStore = createCacheRecordStore(InMemoryFormat.NATIVE);
        putAndGetFromCacheRecordStore(cacheRecordStore, InMemoryFormat.NATIVE);
    }

}
