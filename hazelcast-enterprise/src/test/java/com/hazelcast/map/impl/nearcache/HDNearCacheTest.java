package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.config.EvictionPolicy.LRU;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDNearCacheTest extends NearCacheTest {

    @Override
    protected Config getConfig() {
        MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        mapConfig.getEvictionConfig().setEvictionPolicy(LRU);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(32, MemoryUnit.MEGABYTES))
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        return new Config()
                .setProperty(ClusterProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE)
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    @Override
    protected NearCacheConfig newNearCacheConfig() {
        NearCacheConfig nearCacheConfig = super.newNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(false);

        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return nearCacheConfig;
    }

    @Override
    protected NearCacheConfig newNearCacheConfigWithEntryCountEviction(EvictionPolicy evictionPolicy, int size) {
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setCacheLocalEntries(true);

        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(evictionPolicy)
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(size);

        return nearCacheConfig;
    }

    @Test
    @Override
    public void testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException() {
        // this test expects an IllegalArgumentException in OS, but should not throw any exception in EE
        super.testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException();
    }
}
