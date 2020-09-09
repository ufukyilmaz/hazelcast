package com.hazelcast;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.util.ThreadAffinity;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.cache.CacheManager;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_PAGE_SIZE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.internal.util.ThreadAffinity.newSystemThreadAffinity;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;

/**
 * Support class to provide hd specific configuration for map tests
 */
public final class HDTestSupport {

    public static Config getHDConfig() {
        return getHDConfig(new Config(), STANDARD);
    }

    public static Config getHDIndexConfig() {
        return getHDConfig(new Config(), POOLED);
    }

    public static Config getHDConfig(String persistentMemoryDirectory) {
        Config config = new Config();
        MemorySize memorySize = getNativeMemorySize(config);
        return getHDConfig(config, STANDARD, memorySize, persistentMemoryDirectory);
    }

    public static Config getSmallInstanceHDConfig() {
        return getHDConfig(smallInstanceConfig(), STANDARD);
    }

    public static Config getSmallInstanceHDIndexConfig() {
        Config config = getHDConfig(smallInstanceConfig(), STANDARD);
        config.getNativeMemoryConfig().setAllocatorType(POOLED);
        return config;
    }

    public static MemorySize getNativeMemorySize(Config config) {
        int partitionOpsThreadCount = new HazelcastProperties(config).getInteger(PARTITION_OPERATION_THREAD_COUNT);
        ThreadAffinity threadAffinity = newSystemThreadAffinity("hazelcast.operation.thread.affinity");
        if (threadAffinity.isEnabled()) {
            partitionOpsThreadCount = threadAffinity.getThreadCount();
        }

        // The 1.2 coefficient is given for the global memory manager
        long size = Math.max(128,
                (long) (1.2 * (DEFAULT_PAGE_SIZE / (1024 * 1024)) * partitionOpsThreadCount));

        return new MemorySize(size, MemoryUnit.MEGABYTES);
    }

    public static Config getHDConfig(Config config) {
        return getHDConfig(config, STANDARD);
    }

    public static Config getHDConfig(MemoryAllocatorType allocatorType) {
        return getHDConfig(new Config(), allocatorType);
    }

    private static Config getHDConfig(Config config, MemoryAllocatorType allocatorType) {
        MemorySize nativeMemorySize = getNativeMemorySize(config);
        return getHDConfig(config, allocatorType, nativeMemorySize);
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
