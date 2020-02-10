package com.hazelcast.cache.impl.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.internal.hotrestart.HotRestartTestSupport;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import org.junit.runners.Parameterized.Parameter;

import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import java.util.function.Supplier;

import static com.hazelcast.HDTestSupport.getICache;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.internal.hotrestart.encryption.TestHotRestartEncryptionUtils.withBasicEncryptionAtRestConfig;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
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

    @Parameter(4)
    public boolean encrypted;

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
                .setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100")
                // to reduce used native memory size
                .setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        if (memoryFormat == NATIVE) {
            config.getNativeMemoryConfig()
                    .setEnabled(true)
                    .setSize(getNativeMemorySize())
                    .setMetadataSpacePercentage(20);
        }

        if (encrypted) {
            config = withBasicEncryptionAtRestConfig(config);
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
        int maxSize;
        MaxSizePolicy maxSizePolicy;
        if (memoryFormat == NATIVE) {
            maxSize = evictionEnabled ? 90 : 100;
            maxSizePolicy = USED_NATIVE_MEMORY_PERCENTAGE;
        } else {
            maxSize = evictionEnabled ? keyRange / 2 : Integer.MAX_VALUE;
            maxSizePolicy = ENTRY_COUNT;
        }

        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(maxSize)
                .setMaxSizePolicy(maxSizePolicy)
                .setEvictionPolicy(EvictionPolicy.LRU);

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
        if (memoryFormat == NATIVE) {
            cacheConfig.setInMemoryFormat(NATIVE);
        }

        CacheManager cacheManager = createServerCachingProvider(hz).getCacheManager();
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
