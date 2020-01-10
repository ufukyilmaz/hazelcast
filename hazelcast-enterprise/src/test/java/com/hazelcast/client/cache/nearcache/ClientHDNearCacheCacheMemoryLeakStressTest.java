package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask;
import com.hazelcast.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheConfiguration;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.nearcache.impl.HDNearCache;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.AssertEnabledFilterRule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.CacheManager;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.HDTestSupport.getICache;
import static com.hazelcast.HDTestSupport.getSmallInstanceHDConfig;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.dumpNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class ClientHDNearCacheCacheMemoryLeakStressTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "policy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE},
                {NearCacheConfig.LocalUpdatePolicy.INVALIDATE}});
    }

    @Parameterized.Parameter
    public NearCacheConfig.LocalUpdatePolicy localUpdatePolicy;

    @Rule
    public final TestRule assertEnabledRule = new AssertEnabledFilterRule();

    private static final String CACHE_NAME = ClientHDNearCacheCacheMemoryLeakStressTest.class.getName();
    private static final int KEY_RANGE = 1000;
    private static final int MAX_VALUE_SIZE = 1 << 12; // Up to 4K
    private static final int OPERATION_COUNT = 8;
    private static final long TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    private static final MemoryAllocatorType ALLOCATOR_TYPE = MemoryAllocatorType.STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(16, MemoryUnit.MEGABYTES);

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @Test
    @RequireAssertEnabled
    public void near_cache_does_not_leak_hd_memory() {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(ALLOCATOR_TYPE)
                .setSize(MEMORY_SIZE);

        Config config = getSmallInstanceHDConfig()
                .setNativeMemoryConfig(memoryConfig)
                // Set Max Parallel Replications to max value, so that the initial partitions can sync as soon possible.
                // Due to a race condition in object destruction, it can happen that the sync operation takes place
                // while a cache is being destroyed which can result in a memory leak.
                .setProperty(PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), String.valueOf(Integer.MAX_VALUE))
                // the automatic cache expiration task may cleanup some entries during validation and fail assertions
                // --> we don't want it to kick in during test execution
                .setProperty(CacheClearExpiredRecordsTask.PROP_CLEANUP_ENABLED, "false");


        HazelcastInstance server = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(getClientConfig());

        CacheManager cacheManager = createClientCachingProvider(client).getCacheManager();
        CacheConfiguration<Integer, byte[]> cacheConfig = createCacheConfiguration();
        ICache<Integer, byte[]> cache = getICache(cacheManager, cacheConfig, CACHE_NAME);

        assertInstanceOf(NearCachedClientCacheProxy.class, cache);
        assertInstanceOf(HDNearCache.class, ((NearCachedClientCacheProxy) cache).getNearCache());

        // warm-up
        cache.size();

        final AtomicBoolean stop = new AtomicBoolean(false);

        final int threads = 8;
        final CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            new WorkerThread(cache, latch, stop).start();
        }

        sleepAtLeastMillis(TIMEOUT);
        stop.set(true);
        assertOpenEventually(latch, TIMEOUT * 2);

        cache.destroy();

        // ensure no hd memory leaks
        HazelcastClientInstanceImpl clientInstance = ((HazelcastClientProxy) client).client;
        try {
            assertTrueEventually(() -> {
                MemoryStats memoryStats = clientInstance.getClientExtension().getMemoryStats();

                long usedNative = memoryStats.getUsedNative();
                long usedMetadata = memoryStats.getUsedMetadata();

                assertEquals("usedNative: " + usedNative, 0, usedNative);
                assertEquals("usedMetadata: " + usedMetadata, 0, usedMetadata);
            }, 10);
        } catch (AssertionError e) {
            dumpNativeMemory(clientInstance.getSerializationService());
            throw e;
        }
    }

    private ClientConfig getClientConfig() {
        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(MemoryAllocatorType.STANDARD)
                .setSize(MEMORY_SIZE);

        NearCacheConfig nearCacheConfig = createNearCacheConfig(CACHE_NAME, InMemoryFormat.NATIVE, true)
                .setInvalidateOnChange(true)
                .setLocalUpdatePolicy(localUpdatePolicy)
                .setMaxIdleSeconds(1)
                .setTimeToLiveSeconds(1);
        nearCacheConfig.getEvictionConfig()
                .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(95)
                .setEvictionPolicy(EvictionPolicy.LRU);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    private static CacheConfiguration<Integer, byte[]> createCacheConfiguration() {
        CacheConfiguration<Integer, byte[]> cacheConfig = new CacheConfig<Integer, byte[]>()
                .setBackupCount(1)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setEvictionConfig(getEvictionConfig());
        return cacheConfig;
    }

    private static EvictionConfig getEvictionConfig() {
        return new EvictionConfig()
                .setSize(10000)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
    }

    private static class WorkerThread extends Thread {

        private final ICache<Integer, byte[]> cache;
        private final CountDownLatch latch;
        private final Random rand = new Random();
        private final AtomicBoolean stop;

        WorkerThread(ICache<Integer, byte[]> cache, CountDownLatch latch, AtomicBoolean stop) {
            this.cache = cache;
            this.latch = latch;
            this.stop = stop;
        }

        @Override
        public void run() {
            try {
                while (!stop.get()) {
                    int key = rand.nextInt(KEY_RANGE);
                    int op = rand.nextInt(OPERATION_COUNT);
                    doOp(op, key);
                }
            } finally {
                latch.countDown();
            }
        }

        private void doOp(int op, int key) {
            switch (op) {
                case 0:
                    cache.put(key, newValue(key));
                    break;

                case 1:
                    cache.get(key);
                    break;

                case 2:
                    cache.remove(key);
                    break;

                case 3:
                    cache.replace(key, newValue(key));
                    break;

                case 4:
                    cache.putIfAbsent(key, newValue(key));
                    break;

                case 5:
                    Set<Integer> keysToAdd = new HashSet<>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToAdd.add(k);
                    }
                    cache.getAll(keysToAdd, null);
                    break;

                case 6:
                    Map<Integer, byte[]> entries = new HashMap<>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        entries.put(k, newValue(k));
                    }
                    cache.putAll(entries, null);
                    break;
                case 7:
                    Set<Integer> keysToRemove = new HashSet<>(32);
                    for (int k = key, i = 0; i < 32 && k < KEY_RANGE; k++, i++) {
                        keysToRemove.add(k);
                    }
                    cache.removeAll(keysToRemove);
                    break;

                default:
                    cache.put(key, newValue(key));
            }
        }

        private byte[] newValue(int k) {
            return ClientHDNearCacheCacheMemoryLeakStressTest.newValue(rand, k);
        }
    }

    private static byte[] newValue(Random rand, int k) {
        int len = 16 + rand.nextInt(MAX_VALUE_SIZE); // up to 4k
        byte[] value = new byte[len];
        rand.nextBytes(value);

        Bits.writeIntB(value, 0, k);
        Bits.writeIntB(value, len - 4, k);

        return value;
    }
}
