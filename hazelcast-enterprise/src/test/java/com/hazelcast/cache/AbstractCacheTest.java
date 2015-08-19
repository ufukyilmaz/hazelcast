package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.internal.monitors.HealthMonitorLevel;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.StringUtil;

import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

/**
 * @author mdogan 02/06/14
 */
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
        if (StringUtil.isNullOrEmpty(cacheNamePropertyValue)) {
            CACHE_NAME = DEFAULT_CACHE_NAME;
        } else {
            CACHE_NAME = cacheNamePropertyValue;
        }

        String cacheStorageTypePropertyValue = System.getProperty(IN_MEMORY_FORMAT_PROPERTY);
        if (StringUtil.isNullOrEmpty(cacheStorageTypePropertyValue)) {
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

    protected CacheConfig createCacheConfig(String cacheName) {
        return createCacheConfig(cacheName, IN_MEMORY_FORMAT);
    }

    protected CacheConfig createCacheConfig(String cacheName,
                                            InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        cacheConfig.setName(cacheName);
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setStatisticsEnabled(true);
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setSize(90);
        evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    protected NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(256, MemoryUnit.MEGABYTES);
        return
                new NativeMemoryConfig()
                        .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
                        .setSize(memorySize).setEnabled(true);
    }

    protected SerializationConfig getSerializationConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setAllowUnsafe(true).setUseNativeByteOrder(true);
        return serializationConfig;
    }

    protected void setProperties(Config config) {
        config.setProperty(GroupProperty.PARTITION_COUNT, "111");
        config.setProperty(GroupProperty.SOCKET_BIND_ANY, "false");
        config.setProperty(GroupProperty.MAX_WAIT_SECONDS_BEFORE_JOIN, "0");
        config.setProperty(GroupProperty.GENERIC_OPERATION_THREAD_COUNT, "2");
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT, "4");
        config.setProperty(GroupProperty.LOGGING_TYPE, "log4j");
        config.setProperty(GroupProperty.HEALTH_MONITORING_LEVEL, HealthMonitorLevel.OFF.name());
    }

    protected ICache createCache() {
        Cache<Object, Object> cache = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        return cache.unwrap(ICache.class);
    }

    @Before
    public void setup() {
        onSetup();
        cachingProvider =
                HazelcastServerCachingProvider
                        .createCachingProvider(getHazelcastInstance());
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public void tearDown() {
        if (cacheManager != null) {
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
