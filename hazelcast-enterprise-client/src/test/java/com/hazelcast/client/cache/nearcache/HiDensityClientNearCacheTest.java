package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityClientNearCacheTest extends ClientNearCacheTestSupport {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();

        config.getNativeMemoryConfig()
                .setSize(SERVER_NATIVE_MEMORY_SIZE)
                .setEnabled(true);

        return config;
    }

    @Override
    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = super.createClientConfig();

        clientConfig.getNativeMemoryConfig()
                .setSize(CLIENT_NATIVE_MEMORY_SIZE)
                .setEnabled(true);

        return clientConfig;
    }

    @Override
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);

        CacheConfig cacheConfig = super.createCacheConfig(inMemoryFormat);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        evictionConfig.setSize(99);

        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(inMemoryFormat);
        nearCacheConfig.setEvictionConfig(evictionConfig);
        return nearCacheConfig;
    }

    @Test
    public void putAndGetFromCacheAndThenGetFromClientHiDensityNearCache() {
        putAndGetFromCacheAndThenGetFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndThenGetFromClientHiDensityNearCache() {
        putToCacheAndThenGetFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putIfAbsentToCacheAndThenGetFromClientNearCache() {
        putIfAbsentToCacheAndThenGetFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putAsyncToCacheAndThenGetFromClientNearCacheImmediately() throws Exception {
        putAsyncToCacheAndThenGetFromClientNearCacheImmediately(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientHiDensityNearCache() {
        putToCacheAndUpdateFromOtherNodeThenGetUpdatedFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndGetInvalidationEventWhenNodeShutdown() {
        putToCacheAndGetInvalidationEventWhenNodeShutdown(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientHiDensityNearCache() {
        putToCacheAndRemoveFromOtherNodeThenCantGetUpdatedFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void testLoadAllNearCacheInvalidation() throws Exception {
        testLoadAllNearCacheInvalidation(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientHiDensityNearCache() {
        putToCacheAndClearOrDestroyThenCantGetAnyRecordFromClientNearCache(InMemoryFormat.NATIVE);
    }

    @Test
    public void putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled() {
        putToCacheAndDontInvalidateFromClientNearCacheWhenPerEntryInvalidationIsDisabled(InMemoryFormat.NATIVE);
    }

    @Test
    public void testNearCacheExpiration_withTTL() {
        testNearCacheExpiration_withTTL(InMemoryFormat.NATIVE);
    }

    @Test
    public void testNearCacheExpiration_withMaxIdle() {
        testNearCacheExpiration_withMaxIdle(InMemoryFormat.NATIVE);
    }
}
