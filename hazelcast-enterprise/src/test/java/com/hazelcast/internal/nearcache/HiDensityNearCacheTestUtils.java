package com.hazelcast.internal.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * Provides utility methods for unified HiDensity Near Cache tests.
 */
public final class HiDensityNearCacheTestUtils {

    private HiDensityNearCacheTestUtils() {
    }

    public static Config getNearCacheHDConfig() {
        MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setInMemoryFormat(NATIVE);

        return new Config()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE)
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }

    public static NativeMemoryConfig createNativeMemoryConfig() {
        return new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(32, MemoryUnit.MEGABYTES))
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
    }
}
