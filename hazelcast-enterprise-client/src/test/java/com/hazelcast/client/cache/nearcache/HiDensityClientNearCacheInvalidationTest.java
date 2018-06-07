package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.cache.impl.nearcache.ClientNearCacheInvalidationTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static java.util.Arrays.asList;

/**
 * Test publishing of Near Cache invalidation events, when the cache is configured with NATIVE in-memory format.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(SlowTest.class)
public class HiDensityClientNearCacheInvalidationTest extends ClientNearCacheInvalidationTest {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);

    @Parameters(name = "fromMember:{0}, format:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {false, InMemoryFormat.NATIVE},
                {false, InMemoryFormat.BINARY},
                {false, InMemoryFormat.OBJECT},

                {true, InMemoryFormat.NATIVE},
                {true, InMemoryFormat.BINARY},
                {true, InMemoryFormat.OBJECT},
        });
    }

    @Override
    protected Config getConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(SERVER_NATIVE_MEMORY_SIZE)
                .setAllocatorType(MemoryAllocatorType.STANDARD);

        return super.getConfig()
                .setLicenseKey(UNLIMITED_LICENSE)
                .setNativeMemoryConfig(nativeMemoryConfig)
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");
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
    protected NearCacheConfig createNearCacheConfig(InMemoryFormat inMemoryFormat) {
        return super.createNearCacheConfig(inMemoryFormat);
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig(InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(99);

        return super.<K, V>createCacheConfig(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }

    @Override
    public void setup() {
        RuntimeAvailableProcessors.override(4);
        super.setup();
    }

    @Override
    public void tearDown() {
        RuntimeAvailableProcessors.resetOverride();
        super.tearDown();
    }
}
