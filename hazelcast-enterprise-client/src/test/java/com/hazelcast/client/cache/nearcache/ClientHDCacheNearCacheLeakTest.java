package com.hazelcast.client.cache.nearcache;

import com.hazelcast.cache.ICache;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsNotZero;
import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsZero;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.dumpNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static com.hazelcast.config.EvictionPolicy.LRU;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientHDCacheNearCacheLeakTest extends HazelcastTestSupport {

    private static final MemoryAllocatorType ALLOCATOR_TYPE = MemoryAllocatorType.STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);
    private static final int PARTITION_COUNT = 271;
    private static final String CACHE_NAME = randomMapName("HDNearCache");

    @Parameters(name = "MemoryAllocatorType:{0} NearCachePreloader:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {MemoryAllocatorType.STANDARD, true},
                {MemoryAllocatorType.STANDARD, false},
                {MemoryAllocatorType.POOLED, true},
                {MemoryAllocatorType.POOLED, false},
        });
    }

    @Parameter
    public MemoryAllocatorType memoryAllocatorType;

    @Parameter(value = 1)
    public boolean enableNearCachePreloader;

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testDestroyShutdown() {
        Config config = createConfig();
        config.getNativeMemoryConfig().setAllocatorType(memoryAllocatorType);

        ClientConfig clientConfig = createClientConfig(enableNearCachePreloader);
        clientConfig.getNativeMemoryConfig().setAllocatorType(memoryAllocatorType);

        CacheConfig<Integer, Integer> cacheConfig = createCacheConfig();

        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        HazelcastClientProxy client = (HazelcastClientProxy) hazelcastFactory.newHazelcastClient(clientConfig);

        CachingProvider provider = HazelcastClientCachingProvider.createCachingProvider(client);
        HazelcastClientCacheManager cacheManager = (HazelcastClientCacheManager) provider.getCacheManager();
        ICache<Integer, Integer> clientCache = cacheManager.createCache(CACHE_NAME, cacheConfig);

        NearCacheStats nearCacheStats = clientCache.getLocalCacheStatistics().getNearCacheStatistics();

        for (int i = 0; i < 1000; i++) {
            clientCache.put(i, i);
        }
        for (int i = 0; i < 1000; i++) {
            clientCache.get(i);
            clientCache.get(i);
        }
        assertTrue("The Near Cache should be filled (" + nearCacheStats + ")", nearCacheStats.getOwnedEntryCount() > 0);

        MemoryStats memberMemoryStats = getNode(member).hazelcastInstance.getMemoryStats();
        MemoryStats clientMemoryStats = client.client.getClientExtension().getMemoryStats();

        assertMemoryStatsNotZero("member", memberMemoryStats);
        assertMemoryStatsNotZero("client", clientMemoryStats);

        if (memoryAllocatorType == MemoryAllocatorType.STANDARD) {
            clientCache.destroy();
            if (clientMemoryStats.getUsedNative() > 0 || clientMemoryStats.getUsedMetadata() > 0) {
                dumpNativeMemory(client.getSerializationService());
            }
        }

        client.shutdown();
        member.shutdown();

        assertMemoryStatsZero("member", memberMemoryStats);
        assertMemoryStatsZero("client", clientMemoryStats);
    }

    private static Config createConfig() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(99)
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        MapConfig mapConfig = new MapConfig(CACHE_NAME)
                .setBackupCount(1)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setStatisticsEnabled(true)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setMaxSizeConfig(maxSizeConfig);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(ALLOCATOR_TYPE)
                .setSize(MEMORY_SIZE);

        return new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT))
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    private static ClientConfig createClientConfig(boolean enableNearCachePreloader) {
        NearCachePreloaderConfig nearCachePreloaderConfig = new NearCachePreloaderConfig()
                .setEnabled(enableNearCachePreloader);

        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(CACHE_NAME)
                .setInvalidateOnChange(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setPreloaderConfig(nearCachePreloaderConfig);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(ALLOCATOR_TYPE)
                .setSize(MEMORY_SIZE);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

    private static <K, V> CacheConfig<K, V> createCacheConfig() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(LRU)
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return new CacheConfig<K, V>()
                .setName(CACHE_NAME)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
