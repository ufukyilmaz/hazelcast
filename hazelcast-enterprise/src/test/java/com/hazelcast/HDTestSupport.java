package com.hazelcast;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;

import javax.cache.CacheManager;

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

/**
 * Support class to provide hd specific configuration for map tests
 */
public final class HDTestSupport {

    public static final MemorySize NATIVE_MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    public static Config getHDConfig() {
        return getHDConfig(new Config(), STANDARD);
    }

    public static Config getHDConfig(String persistentMemoryDirectory) {
        MemorySize memorySize = persistentMemoryDirectory == null ? NATIVE_MEMORY_SIZE
                : new MemorySize(128, MemoryUnit.MEGABYTES);
        return getHDConfig(new Config(), STANDARD, memorySize, persistentMemoryDirectory);
    }

    public static Config getSmallInstanceHDConfig() {
        return getHDConfig(smallInstanceConfig(), STANDARD);
    }

    public static Config getHDConfig(Config config) {
        return getHDConfig(config, STANDARD);
    }

    public static Config getHDConfig(MemoryAllocatorType allocatorType) {
        return getHDConfig(new Config(), allocatorType);
    }

    private static Config getHDConfig(Config config, MemoryAllocatorType allocatorType) {
        return getHDConfig(config, allocatorType, NATIVE_MEMORY_SIZE);
    }

    public static Config getHDConfig(Config config, MemoryAllocatorType allocatorType, MemorySize size) {
        return getHDConfig(config, allocatorType, size, null);
    }

    public static Config getHDConfig(Config config, MemoryAllocatorType allocatorType, MemorySize size,
                                     String persistentMemoryDirectory) {
        MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(size)
                .setAllocatorType(allocatorType)
                .setPersistentMemoryDirectory(persistentMemoryDirectory);

        return config
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig)
                .setLicenseKey(UNLIMITED_LICENSE);
    }

    public static <K, V> ICache<K, V> getICache(CacheManager manager, CacheConfiguration<K, V> config, String cacheName) {
        return (ICache<K, V>) manager.createCache(cacheName, config);
    }
}
