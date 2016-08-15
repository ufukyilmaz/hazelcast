package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.ICache;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractCacheHotRestartTest extends HazelcastTestSupport {

    private static InetAddress localAddress;

    @BeforeClass
    public static void setupClass() throws UnknownHostException {
        localAddress = InetAddress.getLocalHost();
    }

    @Rule
    public TestName testName = new TestName();

    private File folder;
    private TestHazelcastInstanceFactory factory;
    String cacheName;

    @Parameterized.Parameter(0)
    public InMemoryFormat memoryFormat;

    @Parameterized.Parameter(1)
    public int keyRange;

    @Parameterized.Parameter(2)
    public boolean evictionEnabled;

    @Before
    public final void setup() throws UnknownHostException {
        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }

        cacheName = randomName();

        factory = createFactory();

        setupInternal();
    }

    private TestHazelcastInstanceFactory createFactory() {
        String[] addresses = new String[10];
        Arrays.fill(addresses, "127.0.0.1");
        return new TestHazelcastInstanceFactory(5000, addresses);
    }

    void setupInternal() {
    }

    @After
    public final void tearDown() {
        tearDownInternal();

        if (factory != null) {
            factory.terminateAll();
        }

        if (folder != null) {
            delete(folder);
        }
    }

    void tearDownInternal() {
    }

    HazelcastInstance newHazelcastInstance() {
        return factory.newHazelcastInstance(makeConfig());
    }

    HazelcastInstance newHazelcastInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    HazelcastInstance[] newInstances(int clusterSize) {
        return factory.newInstances(makeConfig(), clusterSize);
    }

    void restartInstances(int clusterSize) {
        ClusterState state = ClusterState.ACTIVE;
        if (factory != null) {
            Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
            if (!instances.isEmpty()) {
                HazelcastInstance instance = instances.iterator().next();
                Cluster cluster = instance.getCluster();
                state = cluster.getClusterState();
                cluster.changeClusterState(ClusterState.PASSIVE);
            }
            factory.terminateAll();
        }

        factory = createFactory();

        final CountDownLatch latch = new CountDownLatch(clusterSize);
        final Config config = makeConfig();

        for (int i = 0; i < clusterSize; i++) {
            final Address address = new Address("127.0.0.1", localAddress, 5000 + i);
            new Thread() {
                @Override
                public void run() {
                    factory.newHazelcastInstance(address, config);
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        if (!instances.isEmpty()) {
            HazelcastInstance instance = instances.iterator().next();
            instance.getCluster().changeClusterState(state);
        }
    }

    HazelcastInstance restartHazelcastInstance(HazelcastInstance hz) {
        Address address = getNode(hz).getThisAddress();
        Config config = hz.getConfig();
        hz.shutdown();
        hz = factory.newHazelcastInstance(address, config);
        return hz;
    }

    Config makeConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100");

        // to reduce used native memory size
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig.setEnabled(true);
        hotRestartPersistenceConfig.setBaseDir(folder);

        config.getNativeMemoryConfig().setEnabled(true)
                .setSize(getNativeMemorySize())
                .setMetadataSpacePercentage(20);
        return config;
    }

    MemorySize getNativeMemorySize() {
        return new MemorySize(64, MemoryUnit.MEGABYTES);
    }

    <V> ICache<Integer, V> createCache() {
        HazelcastInstance hz = factory.getAllHazelcastInstances().iterator().next();
        assertNotNull(hz);
        return createCache(hz, 1);
    }

    <V> ICache<Integer, V> createCache(HazelcastInstance hz) {
        return createCache(hz, 1);
    }

    <V> ICache<Integer, V> createCache(HazelcastInstance hz, int backupCount) {
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

    <V> ICache<Integer, V> createCache(HazelcastInstance hz, EvictionConfig evictionConfig) {
        return createCache(hz, 1, evictionConfig);
    }

    <V> ICache<Integer, V> createCache(HazelcastInstance hz, int backupCount, EvictionConfig evictionConfig) {
        CacheConfig<Integer, V> cacheConfig = new CacheConfig<Integer, V>();
        cacheConfig.getHotRestartConfig().setEnabled(true);
        cacheConfig.setBackupCount(backupCount);
        cacheConfig.setStatisticsEnabled(true);
        if (memoryFormat == InMemoryFormat.NATIVE) {
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        }
        cacheConfig.setEvictionConfig(evictionConfig);

        CacheManager cacheManager = createCachingProvider(hz).getCacheManager();
        Cache<Integer, V> cache = cacheManager.createCache(cacheName, cacheConfig);
        return cache.unwrap(ICache.class);
    }

    EnterpriseCacheService getCacheService(HazelcastInstance hz) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        return nodeEngine.getService("hz:impl:cacheService");
    }
}
