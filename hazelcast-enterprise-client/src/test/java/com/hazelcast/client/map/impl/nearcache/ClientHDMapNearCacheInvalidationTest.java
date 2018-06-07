package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDMapNearCacheInvalidationTest extends ClientMapNearCacheInvalidationTest {

    private static final MemorySize SERVER_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);
    private static final MemorySize CLIENT_NATIVE_MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);

    @Before
    public void setUpRuntimeAvailableProcessors() {
        RuntimeAvailableProcessors.override(4);
    }

    @After
    public void tearDownRuntimeAvailableProcessors() {
        RuntimeAvailableProcessors.resetOverride();
    }

    @Override
    protected Config getConfig() {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(SERVER_NATIVE_MEMORY_SIZE)
                .setAllocatorType(MemoryAllocatorType.STANDARD);

        return super.getConfig()
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
                .setLicenseKey(UNLIMITED_LICENSE)
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected ClientConfig getClientConfig(String mapName) {
        NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(CLIENT_NATIVE_MEMORY_SIZE);

        return super.getClientConfig(mapName)
                .setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    protected NearCacheConfig getNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return super.getNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
