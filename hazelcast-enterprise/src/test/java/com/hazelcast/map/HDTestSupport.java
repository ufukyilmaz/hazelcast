package com.hazelcast.map;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IEnterpriseMap;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;

import javax.cache.CacheManager;

/**
 * Support class to provide hd specific configuration for map tests
 */
public final class HDTestSupport {

    public static final MemorySize NATIVE_MEMORY_SIZE = new MemorySize(32, MemoryUnit.MEGABYTES);

    public static Config getHDConfig() {
        return getHDConfig(new Config());
    }

    public static Config getHDConfig(Config config) {
        MapConfig mapConfig = new MapConfig()
                .setName("default")
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(NATIVE_MEMORY_SIZE)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        return config
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    public static <K, V> IEnterpriseMap<K, V> getEnterpriseMap(HazelcastInstance instance, String mapName) {
        return (IEnterpriseMap<K, V>) instance.<K, V>getMap(mapName);
    }

    public static <K, V> ICache<K, V> getICache(CacheManager manager, CacheConfiguration<K, V> config, String cacheName) {
        return (ICache<K, V>) manager.createCache(cacheName, config);
    }
}
