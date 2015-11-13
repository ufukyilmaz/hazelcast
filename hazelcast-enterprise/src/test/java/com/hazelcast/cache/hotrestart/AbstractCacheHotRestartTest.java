package com.hazelcast.cache.hotrestart;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.Address;
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
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static com.hazelcast.nio.IOUtil.delete;
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

    void setupInternal() {}

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

    void tearDownInternal() {}

    HazelcastInstance newHazelcastInstance() {
        return factory.newHazelcastInstance(makeConfig());
    }

    HazelcastInstance[] newInstances(int clusterSize) {
        return factory.newInstances(makeConfig(), clusterSize);
    }

    void restartInstances(int clusterSize) {
        if (factory != null) {
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
    }

    HazelcastInstance restartHazelcastInstance(HazelcastInstance hz) {
        Address address = getNode(hz).getThisAddress();
        hz.shutdown();
        hz = factory.newHazelcastInstance(address, makeConfig());
        return hz;
    }

    Config makeConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY, SampleLicense.UNLIMITED_LICENSE);
        config.setProperty(GroupProperty.PARTITION_MAX_PARALLEL_REPLICATIONS, "100");

        HotRestartConfig hotRestartConfig = config.getHotRestartConfig();
        hotRestartConfig.setEnabled(true);
        hotRestartConfig.setHomeDir(folder);

        config.getNativeMemoryConfig().setEnabled(true)
                .setSize(getNativeMemorySize())
                .setMetadataSpacePercentage(50);
        return config;
    }

    MemorySize getNativeMemorySize() {
        return new MemorySize(128, MemoryUnit.MEGABYTES);
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
        CacheConfig<Integer, V> cacheConfig = new CacheConfig<Integer, V>();
        cacheConfig.setHotRestartEnabled(true);
        cacheConfig.setBackupCount(backupCount);
        cacheConfig.setStatisticsEnabled(true);

        EvictionConfig evictionConfig;

        if (memoryFormat == InMemoryFormat.NATIVE) {
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
            int size = evictionEnabled ? 90 : 100;
            evictionConfig = new EvictionConfig(size, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, EvictionPolicy.LRU);
        } else {
            int size = evictionEnabled ? keyRange / 2 : Integer.MAX_VALUE;
            evictionConfig = new EvictionConfig(size, MaxSizePolicy.ENTRY_COUNT, EvictionPolicy.LRU);
        }

        cacheConfig.setEvictionConfig(evictionConfig);

        CacheManager cacheManager = createCachingProvider(hz).getCacheManager();
        Cache<Integer, V> cache = cacheManager.createCache(cacheName, cacheConfig);
        return cache.unwrap(ICache.class);
    }

    private static String toFileName(String name) {
        return name.replaceAll("[:\\\\/*\"?|<>',]", "_");
    }
}
