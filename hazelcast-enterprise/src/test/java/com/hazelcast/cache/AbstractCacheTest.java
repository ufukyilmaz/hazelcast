package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.HealthMonitorLevel;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.HDTestSupport.getICache;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

public abstract class AbstractCacheTest extends HazelcastTestSupport {

    protected static final String CACHE_NAME_PROPERTY = "cacheName";
    protected static final String IN_MEMORY_FORMAT_PROPERTY = "inMemoryFormat";

    protected static final String DEFAULT_CACHE_NAME = "CACHE";
    protected static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.NATIVE;

    protected static final String CACHE_NAME;
    protected static final InMemoryFormat IN_MEMORY_FORMAT;

    protected CachingProvider cachingProvider;
    protected CacheManager cacheManager;

    protected abstract HazelcastInstance getHazelcastInstance();

    protected abstract void onSetup();

    protected abstract void onTearDown();

    static {
        String cacheNamePropertyValue = System.getProperty(CACHE_NAME_PROPERTY);
        if (isNullOrEmpty(cacheNamePropertyValue)) {
            CACHE_NAME = DEFAULT_CACHE_NAME;
        } else {
            CACHE_NAME = cacheNamePropertyValue;
        }

        String cacheStorageTypePropertyValue = System.getProperty(IN_MEMORY_FORMAT_PROPERTY);
        if (isNullOrEmpty(cacheStorageTypePropertyValue)) {
            IN_MEMORY_FORMAT = DEFAULT_IN_MEMORY_FORMAT;
        } else {
            IN_MEMORY_FORMAT = InMemoryFormat.valueOf(cacheStorageTypePropertyValue);
        }
    }

    protected Config createConfig() {
        Config config = new Config();
        setProperties(config);
        config.setNativeMemoryConfig(getMemoryConfig());
        config.setSerializationConfig(getSerializationConfig());
        return config;
    }

    protected <K, V> CacheConfig<K, V> createCacheConfig(String cacheName) {
        return createCacheConfig(cacheName, IN_MEMORY_FORMAT);
    }

    protected <K, V> CacheConfig<K, V> createCacheConfig(String cacheName, InMemoryFormat inMemoryFormat) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(90)
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setName(cacheName)
                .setInMemoryFormat(inMemoryFormat)
                .setEvictionConfig(evictionConfig);

        cacheConfig.setStatisticsEnabled(true);

        return cacheConfig;
    }

    protected NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(256, MemoryUnit.MEGABYTES);
        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                .setSize(memorySize).setEnabled(true);
    }

    protected SerializationConfig getSerializationConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setAllowUnsafe(true).setUseNativeByteOrder(true);
        return serializationConfig;
    }

    protected void setProperties(Config config) {
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "111");
        config.setProperty(GroupProperty.SOCKET_BIND_ANY.getName(), "false");
        config.setProperty(GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.setProperty(GroupProperty.GENERIC_OPERATION_THREAD_COUNT.getName(), "2");
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");
        config.setProperty(GroupProperty.HEALTH_MONITORING_LEVEL.getName(), HealthMonitorLevel.OFF.name());
    }

    protected <K, V> ICache<K, V> createCache() {
        CacheConfig<K, V> cacheConfig = createCacheConfig(CACHE_NAME);
        return getICache(cacheManager, cacheConfig, CACHE_NAME);
    }

    protected <K, V> ICache<K, V> createCache(CacheConfig<K, V> config) {
        return getICache(cacheManager, config, CACHE_NAME);
    }

    @Before
    public void setup() {
        onSetup();
        cachingProvider = HazelcastServerCachingProvider.createCachingProvider(getHazelcastInstance());
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public void tearDown() {
        if (cacheManager != null && !cacheManager.isClosed()) {
            Iterable<String> cacheNames = cacheManager.getCacheNames();
            for (String name : cacheNames) {
                cacheManager.destroyCache(name);
            }
            cacheManager.close();
        }
        if (cachingProvider != null) {
            cachingProvider.close();
        }
        onTearDown();
    }
}
