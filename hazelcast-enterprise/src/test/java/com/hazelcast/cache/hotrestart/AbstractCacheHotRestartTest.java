package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.hotrestart.HotRestartTestSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.runners.Parameterized.Parameter;

import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import java.util.function.Supplier;

import static com.hazelcast.HDTestSupport.getICache;
import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractCacheHotRestartTest extends HotRestartTestSupport {

    protected static final int KEY_COUNT = 1000;

    @Parameter
    public InMemoryFormat memoryFormat;

    @Parameter(1)
    public int keyRange;

    @Parameter(2)
    public boolean fsyncEnabled;

    @Parameter(3)
    public boolean evictionEnabled;

    String cacheName;

    protected void setupCacheInternal() {

    }

    @Override
    public final void setupInternal() {
        cacheName = randomName();
        setupCacheInternal();
    }

    HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(new Supplier<Config>() {
            @Override
            public Config get() {
                return makeConfig();
            }
        });
    }

    HazelcastInstance newHazelcastInstance(final Config config) {
        return newHazelcastInstance(new Supplier<Config>() {
            @Override
            public Config get() {
                return withHotRestart(config);
            }
        });
    }

    HazelcastInstance[] restartInstances(int clusterSize) {
        return restartCluster(clusterSize, new Supplier<Config>() {
            @Override
            public Config get() {
                return makeConfig();
            }
        });
    }

    HazelcastInstance[] newInstances(int clusterSize) {
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = newHazelcastInstance();
        }
        return instances;
    }

    Config makeConfig() {
        Config config = createConfig();
        return withHotRestart(config);
    }

    private Config withHotRestart(Config config) {
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig
                .setEnabled(true)
                .setBaseDir(baseDir);
        return config;
    }

    private Config createConfig() {
        Config config = new Config()
                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                .setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                // to reduce used native memory size
                .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        if (memoryFormat == InMemoryFormat.NATIVE) {
            config.getNativeMemoryConfig()
                    .setEnabled(true)
                    .setSize(getNativeMemorySize())
                    .setMetadataSpacePercentage(20);
        }
        return config;
    }

    MemorySize getNativeMemorySize() {
        return new MemorySize(64, MemoryUnit.MEGABYTES);
    }

    <V> ICache<Integer, V> createCache() {
        HazelcastInstance hz = getFirstInstance();
        assertNotNull(hz);
        return createCache(hz, 1);
    }

    <K, V> ICache<K, V> createCache(HazelcastInstance hz) {
        return createCache(hz, 1);
    }

    <K, V> ICache<K, V> createCache(HazelcastInstance hz, int backupCount) {
        EvictionConfig evictionConfig;
        if (memoryFormat == InMemoryFormat.NATIVE) {
            int size = evictionEnabled ? 90 : 100;
            evictionConfig = new EvictionConfig(size, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, EvictionPolicy.LRU);
        } else {
            int size = evictionEnabled ? keyRange / 2 : Integer.MAX_VALUE;
            evictionConfig = new EvictionConfig(size, MaxSizePolicy.ENTRY_COUNT, EvictionPolicy.LRU);
        }
        return createCache(hz, backupCount, evictionConfig);
    }

    <K, V> ICache<K, V> createCache(HazelcastInstance hz, EvictionConfig evictionConfig) {
        return createCache(hz, 1, evictionConfig);
    }

    <K, V> ICache<K, V> createCache(HazelcastInstance hz, int backupCount, EvictionConfig evictionConfig) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setBackupCount(backupCount)
                .setEvictionConfig(evictionConfig);
        cacheConfig.setStatisticsEnabled(true);
        cacheConfig.getHotRestartConfig()
                .setEnabled(true)
                .setFsync(fsyncEnabled);
        if (memoryFormat == InMemoryFormat.NATIVE) {
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        }

        CacheManager cacheManager = createCachingProvider(hz).getCacheManager();
        return getICache(cacheManager, cacheConfig, cacheName);
    }

    EnterpriseCacheService getCacheService(HazelcastInstance hz) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        return nodeEngine.getService("hz:impl:cacheService");
    }

    @SuppressWarnings("unchecked")
    static <K, V, C extends Configuration<K, V>> C getConfiguration(ICache<K, V> cache) {
        return (C) cache.getConfiguration(CacheConfig.class);
    }
}
