package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

/**
 * Test publishing of Near Cache invalidation events, when the cache is configured with NATIVE in-memory format and
 * with hot restart enabled.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class HotRestartClientHDNearCacheInvalidationTest extends ClientCacheNearCacheInvalidationTest {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);

    @Parameters(name = "format:{0}, fromMember:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},

                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false},

                {InMemoryFormat.NATIVE, true},
                {InMemoryFormat.NATIVE, false},
        });
    }

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(4);

    @Override
    protected Config getConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(SERVER_NATIVE_MEMORY_SIZE);

        HotRestartPersistenceConfig hotRestartPersistenceConfig = new HotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(hotRestartFolderRule.getBaseDir());

        return super.getConfig()
                .setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
                .setNativeMemoryConfig(nativeMemoryConfig)
                .setHotRestartPersistenceConfig(hotRestartPersistenceConfig);
    }

    @Override
    protected ClientConfig getClientConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(CLIENT_NATIVE_MEMORY_SIZE);

        return super.getClientConfig()
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected <K, V> CacheConfig<K, V> getCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        HotRestartConfig hotRestartConfig = new HotRestartConfig()
                .setEnabled(true);

        CacheConfig<K, V> cacheConfig = super.getCacheConfig(InMemoryFormat.NATIVE);
        cacheConfig.setEvictionConfig(evictionConfig);
        cacheConfig.setHotRestartConfig(hotRestartConfig);
        return cacheConfig;
    }
}
