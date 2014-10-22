package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.OffHeapMemoryConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.StringUtil;
import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Properties;

/**
 * @author mdogan 02/06/14
 */
public abstract class AbstractCacheTest extends HazelcastTestSupport {

    protected static final String CACHE_NAME_PROPERTY = "cacheName";
    protected static final String IN_MEMORY_FORMAT_PROPERTY = "inMemoryFormat";

    protected static final String DEFAULT_CACHE_NAME = "CACHE";
    protected static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT =
            InMemoryFormat.OFFHEAP;

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

    public static Config createConfig() {
        Config config = new Config();
        config.setProperties(getDefaultProperties());
        config.setOffHeapMemoryConfig(getDefaultMemoryConfig());
        config.setSerializationConfig(getDefaultSerializationConfig());
        return config;
    }

    public static CacheConfig createCacheConfig(String cacheName) {
        return createCacheConfig(cacheName, IN_MEMORY_FORMAT);
    }

    public static CacheConfig createCacheConfig(String cacheName,
                                                InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setStatisticsEnabled(true);
        return cacheConfig;
    }

    public static OffHeapMemoryConfig getDefaultMemoryConfig() {
        MemorySize memorySize = new MemorySize(256, MemoryUnit.MEGABYTES);
        return
                new OffHeapMemoryConfig()
                        .setAllocatorType(OffHeapMemoryConfig.MemoryAllocatorType.STANDARD)
                        .setSize(memorySize).setEnabled(true)
                        .setMinBlockSize(16).setPageSize(1 << 20);
    }

    public static SerializationConfig getDefaultSerializationConfig() {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setAllowUnsafe(true).setUseNativeByteOrder(true);
        return serializationConfig;
    }

    public static Properties getDefaultProperties() {
        Properties props = new Properties();
        props.setProperty(GroupProperties.PROP_PARTITION_COUNT, "111");
        props.setProperty(GroupProperties.PROP_SOCKET_BIND_ANY, "false");
        props.setProperty(GroupProperties.PROP_MAX_WAIT_SECONDS_BEFORE_JOIN, "0");
        props.setProperty(GroupProperties.PROP_GENERIC_OPERATION_THREAD_COUNT, "2");
        props.setProperty(GroupProperties.PROP_PARTITION_OPERATION_THREAD_COUNT, "4");
        props.setProperty(GroupProperties.PROP_LOGGING_TYPE, "log4j");
        return props;
    }

    protected ICache createCache() {
        Cache<Object, Object> cache = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return cache.unwrap(ICache.class);
    }

    @Before
    public void setup() {
        onSetup();
        cachingProvider = HazelcastServerCachingProvider.createCachingProvider(getHazelcastInstance());
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public void tearDown() {
        if (cacheManager != null) {
            Iterable<String> cacheNames = cacheManager.getCacheNames();
            for (String name : cacheNames) {
                cacheManager.destroyCache(name);
            }
        }
        onTearDown();
    }

}
