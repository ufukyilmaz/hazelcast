package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
@Ignore
public class CacheLoadTest extends AbstractCacheTest {

    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(3);

    private HazelcastInstance instance;
    private MemorySize totalSize = new MemorySize(512, MemoryUnit.MEGABYTES);

    @Override
    protected void onSetup() {
        Config config = createConfig();
        instance = Hazelcast.newHazelcastInstance(config);
    }

    @Override
    protected void onTearDown() {
        Hazelcast.shutdownAll();
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    @Override
    protected final Config createConfig() {
        Config config = new Config();
        setProperties(config);

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);

        NativeMemoryConfig memoryConfig = super.getMemoryConfig();
        memoryConfig.setSize(totalSize);

        config.setNativeMemoryConfig(memoryConfig);

        config.setSerializationConfig(getSerializationConfig());

        return config;
    }

    @Override
    protected CacheConfig createCacheConfig(String cacheName,
                                            InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = super.createCacheConfig(cacheName, inMemoryFormat);
        cacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.LRU);
        return cacheConfig;
    }

    @Test
    public void testContinuousPut() throws InterruptedException {
        final ICache cache = createCache();

        Random random = new Random();
        int minSize = 10;
        int maxSize = (int) MemoryUnit.KILOBYTES.toBytes(99);
        final String[] values = new String[3000];
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

        Node node = TestUtil.getNode(instance);
        EnterpriseSerializationService serializationService =
                (EnterpriseSerializationService) node.getSerializationService();
        MemoryManager memoryManager = serializationService.getMemoryManager();
        MemoryStats memoryStats = memoryManager.getMemoryStats();

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
        final ICache cache = createCache();

        int min = 500;
        int max = (int) MemoryUnit.KILOBYTES.toBytes(10);
        int size = min;

        Node node = TestUtil.getNode(instance);
        EnterpriseSerializationService serializationService =
                (EnterpriseSerializationService) node.getSerializationService();
        MemoryManager memoryManager = serializationService.getMemoryManager();
        MemoryStats memoryStats = memoryManager.getMemoryStats();

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
