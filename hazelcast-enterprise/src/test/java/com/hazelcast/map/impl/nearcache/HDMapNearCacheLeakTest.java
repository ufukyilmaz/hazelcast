package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsNotZero;
import static com.hazelcast.NativeMemoryTestUtil.assertMemoryStatsZero;
import static com.hazelcast.NativeMemoryTestUtil.disableNativeMemoryDebugging;
import static com.hazelcast.NativeMemoryTestUtil.dumpNativeMemory;
import static com.hazelcast.NativeMemoryTestUtil.enableNativeMemoryDebugging;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class HDMapNearCacheLeakTest extends HazelcastTestSupport {

    private static final MemoryAllocatorType ALLOCATOR_TYPE = MemoryAllocatorType.STANDARD;
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);
    private static final int PARTITION_COUNT = 271;
    private static final String MAP_NAME = randomMapName("HDNearCache");

    @Parameters(name = "MemoryAllocatorType:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {MemoryAllocatorType.STANDARD},
                {MemoryAllocatorType.POOLED},
        });
    }

    @Parameter
    public MemoryAllocatorType memoryAllocatorType;

    private TestHazelcastInstanceFactory factory;

    @BeforeClass
    public static void setupClass() {
        enableNativeMemoryDebugging();
    }

    @AfterClass
    public static void tearDownClass() {
        disableNativeMemoryDebugging();
    }

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
    }

    @Test
    public void testDestroyShutdown() {
        Config config = createConfig();
        config.getNativeMemoryConfig().setAllocatorType(memoryAllocatorType);

        Config configWithNearCache = createConfig();
        configWithNearCache.getNativeMemoryConfig().setAllocatorType(memoryAllocatorType);
        addNearCacheConfig(configWithNearCache);

        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(configWithNearCache);

        IMap<Integer, Integer> map = hz2.getMap(MAP_NAME);
        NearCacheStats nearCacheStats = map.getLocalMapStats().getNearCacheStats();

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 1000; i++) {
            map.get(i);
            map.get(i);
        }
        assertTrue("The Near Cache should be filled (" + nearCacheStats + ")", nearCacheStats.getOwnedEntryCount() > 0);

        MemoryStats hz1MemoryStats = getNode(hz1).hazelcastInstance.getMemoryStats();
        MemoryStats hz2MemoryStats = getNode(hz2).hazelcastInstance.getMemoryStats();

        assertMemoryStatsNotZero("hz1", hz1MemoryStats);
        assertMemoryStatsNotZero("hz2", hz2MemoryStats);

        if (memoryAllocatorType == MemoryAllocatorType.STANDARD) {
            map.destroy();
            if (hz2MemoryStats.getUsedNative() > 0 || hz2MemoryStats.getUsedMetadata() > 0) {
                dumpNativeMemory(hz2);
            }
        }

        hz2.shutdown();
        hz1.shutdown();

        assertMemoryStatsZero("hz1", hz1MemoryStats);
        assertMemoryStatsZero("hz2", hz2MemoryStats);
    }

    private static Config createConfig() {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(99)
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        MapConfig mapConfig = new MapConfig(MAP_NAME)
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

    private static void addNearCacheConfig(Config config) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setName(MAP_NAME)
                .setInvalidateOnChange(true)
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        config.getMapConfig(MAP_NAME).setNearCacheConfig(nearCacheConfig);
    }
}
