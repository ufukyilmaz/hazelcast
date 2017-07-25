package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheTestSupport;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
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
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(SERVER_NATIVE_MEMORY_SIZE);

        return super.createConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected ClientConfig createClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(CLIENT_NATIVE_MEMORY_SIZE);

        return super.createClientConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected CacheConfig createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createCacheConfig(inMemoryFormat)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.createNearCacheConfig(inMemoryFormat)
                .setEvictionConfig(evictionConfig);
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
