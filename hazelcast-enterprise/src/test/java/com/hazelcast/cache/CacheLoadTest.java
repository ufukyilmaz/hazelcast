package com.hazelcast.cache;

import com.hazelcast.cache.impl.HazelcastCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.LocalMemoryStats;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 02/06/14
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class CacheLoadTest {

    private static final String CACHE_NAME = "CACHE";

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(3);

    private HazelcastInstance server;
    private ICache<Object, Object> cache;
    private MemorySize totalSize = new MemorySize(512, MemoryUnit.MEGABYTES);

    @Before
    public void setup() {
        Config config = getConfig();
        server = Hazelcast.newHazelcastInstance(config);
        cache = createCache(CACHE_NAME);
    }

    protected final Config getConfig() {
        Config config = new Config();
        config.setProperties(AbstractCacheTest.getDefaultProperties());

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);

        config.setNativeMemoryConfig(new NativeMemoryConfig().setEnabled(true)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED).setSize(totalSize)
                .setPageSize(1 << 20));

        config.setSerializationConfig(AbstractCacheTest.getDefaultSerializationConfig());

        /*
        config.addCacheConfig(new CacheConfig()
                                .setName(CACHE_NAME)
                                .setEvictionPolicy(EvictionPolicy.LRU)
                                //.setEvictionPercentage(10)
                                //.setEvictionThresholdPercentage(90));
        );
        */
        return config;
    }

    protected ICache createCache(String name) {
        System.setProperty(GroupProperties.PROP_JCACHE_PROVIDER_TYPE, "server");
        CacheManager cacheManager = new HazelcastCachingProvider().getCacheManager();
        CacheConfig cc = new CacheConfig()
                                .setName(CACHE_NAME)
                                .setEvictionPolicy(EvictionPolicy.LRU);
        return cacheManager.createCache(name,
                                        (Configuration<Object, Object>)cc)
                                .unwrap(ICache.class);
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testContinuousPut() throws InterruptedException {
        Random random = new Random();
        int minSize = 10;
        int maxSize = (int) MemoryUnit.KILOBYTES.toBytes(99);
        final String[] values = new String[5000];
        for (int i = 0; i < values.length; i++) {
            byte[] bb = new byte[random.nextInt(maxSize) + minSize];
            random.nextBytes(bb);
            values[i] = new String(bb);
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        int nThreads = 8;
        ExecutorService ex = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            ex.execute(new Runnable() {
                @Override
                public void run() {
                    Random rand = new Random();
                    while (running.get()) {
                        String value = values[rand.nextInt(values.length)];
                        try {
                            cache.put(rand.nextInt(), value);
                        } catch (Throwable e) {
                            e.printStackTrace();
                            error.set(e);
                            break;
                        }
                    }
                }
            });
        }

        Node node = TestUtil.getNode(server);
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) node.getSerializationService();
        MemoryManager memoryManager = serializationService.getMemoryManager();
        LocalMemoryStats memoryStats = memoryManager.getMemoryStats();

        long now = System.currentTimeMillis();
        long end = now + TIMEOUT;
        while (end >= now && error.get() == null) {
            Thread.sleep(10000);
            System.out.println(memoryStats);
            now = System.currentTimeMillis();
        }
        running.set(false);
        ex.shutdown();
        assertTrue(ex.awaitTermination(30, TimeUnit.SECONDS));
        assertNull(error.get());
    }

    @Test
    public void testIncrementalDataLoad() throws InterruptedException {
        int min = 500;
        int max = (int) MemoryUnit.KILOBYTES.toBytes(10);
        int size = min;

        Node node = TestUtil.getNode(server);
        EnterpriseSerializationService serializationService = (EnterpriseSerializationService) node.getSerializationService();
        MemoryManager memoryManager = serializationService.getMemoryManager();
        LocalMemoryStats memoryStats = memoryManager.getMemoryStats();

        Random rand = new Random();
        while (size < max) {
            int k = (int) (totalSize.bytes() / size);
            byte[] value = new byte[size];
            rand.nextBytes(value);

            for (int i = 0; i < k; i++) {
                cache.put(rand.nextInt(), value);
            }
            size += 500;
            System.out.println(memoryStats);
        }
    }
}
