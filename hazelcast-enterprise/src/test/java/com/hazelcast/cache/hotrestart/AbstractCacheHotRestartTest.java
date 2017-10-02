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
import org.junit.runners.Parameterized.Parameter;

import javax.cache.CacheManager;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.HDTestSupport.getICache;
import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractCacheHotRestartTest extends HazelcastTestSupport {

    protected static final int KEY_COUNT = 1000;

    private static InetAddress localAddress;

    @Rule
    public TestName testName = new TestName();

    @Parameter(0)
    public InMemoryFormat memoryFormat;

    @Parameter(1)
    public int keyRange;

    @Parameter(2)
    public boolean evictionEnabled;

    TestHazelcastInstanceFactory factory;
    String cacheName;

    private File baseDir;

    @BeforeClass
    public static void setupClass() throws UnknownHostException {
        localAddress = InetAddress.getLocalHost();
    }

    @Before
    public final void setup() {
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(baseDir);
        if (!baseDir.mkdir() && !baseDir.exists()) {
            throw new AssertionError("Unable to create test folder: " + baseDir.getAbsolutePath());
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

        if (baseDir != null) {
            delete(baseDir);
        }
    }

    void tearDownInternal() {
    }

    HazelcastInstance newHazelcastInstance() {
        Address address = factory.nextAddress();
        return factory.newHazelcastInstance(address, makeConfig(address));
    }

    HazelcastInstance newHazelcastInstance(Config config) {
        Address address = factory.nextAddress();
        return factory.newHazelcastInstance(address, withHotRestart(address, config));
    }

    HazelcastInstance[] newInstances(int clusterSize) {
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        for (int i = 0; i < clusterSize; i++) {
            instances[i] = newHazelcastInstance();
        }
        return instances;
    }

    HazelcastInstance[] restartInstances(int clusterSize) {
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

        for (int i = 0; i < clusterSize; i++) {
            final Address address = new Address("127.0.0.1", localAddress, 5000 + i);
            new Thread() {
                @Override
                public void run() {
                    Config config = makeConfig(address);
                    factory.newHazelcastInstance(address, config);
                    latch.countDown();
                }
            }.start();
        }

        assertOpenEventually(latch);

        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        if (instances.length > 0) {
            assertClusterSizeEventually(clusterSize, instances);
            HazelcastInstance instance = instances[0];
            instance.getCluster().changeClusterState(state);
        }
        return instances;
    }

    HazelcastInstance restartHazelcastInstance(HazelcastInstance hz, Config config) {
        Address address = getNode(hz).getThisAddress();
        hz.shutdown();
        hz = factory.newHazelcastInstance(address, config);
        return hz;
    }

    Config makeConfig(Address address) {
        Config config = makeConfig();
        return withHotRestart(address, config);
    }

    private Config withHotRestart(Address address, Config config) {
        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        hotRestartPersistenceConfig
                .setEnabled(true)
                .setBaseDir(new File(baseDir, toFileName(address.getHost() + ":" + address.getPort())));
        return config;
    }

    Config makeConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), SampleLicense.UNLIMITED_LICENSE);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "100");

        // to reduce used native memory size
        config.setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4");

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
        HazelcastInstance hz = factory.getAllHazelcastInstances().iterator().next();
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
                .setEnabled(true);
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
}
