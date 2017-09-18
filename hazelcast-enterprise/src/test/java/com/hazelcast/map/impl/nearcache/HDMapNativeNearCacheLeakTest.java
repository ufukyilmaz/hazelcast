package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.adapter.IMapDataStructureAdapter;
import com.hazelcast.internal.nearcache.AbstractEnterpriseNearCacheLeakTest;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.NearCacheTestContext;
import com.hazelcast.internal.nearcache.NearCacheTestContextBuilder;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getMapNearCacheManager;
import static com.hazelcast.map.impl.nearcache.MapInvalidationListener.createInvalidationEventHandler;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class HDMapNativeNearCacheLeakTest extends AbstractEnterpriseNearCacheLeakTest<Data, String> {

    @Parameters(name = "MemoryAllocatorType:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {MemoryAllocatorType.STANDARD},
                {MemoryAllocatorType.POOLED},
        });
    }

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(InMemoryFormat.NATIVE)
                .setInvalidateOnChange(true);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(int size) {
        Config config = createConfig(false);

        HazelcastInstance dataInstance = factory.newHazelcastInstance(config);
        IMap<K, V> dataMap = dataInstance.getMap(DEFAULT_NEAR_CACHE_NAME);
        IMapDataStructureAdapter<K, V> dataAdapter = new IMapDataStructureAdapter<K, V>(dataMap);

        dataInstanceMemoryStats = getNode(dataInstance).hazelcastInstance.getMemoryStats();

        populateDataAdapter(dataAdapter, size);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder();
        return builder
                .setDataInstance(dataInstance)
                .setDataAdapter(dataAdapter)
                .build();
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder() {
        Config configWithNearCache = createConfig(true);

        HazelcastInstance nearCacheInstance = factory.newHazelcastInstance(configWithNearCache);
        IMap<K, V> nearCacheMap = nearCacheInstance.getMap(DEFAULT_NEAR_CACHE_NAME);

        NearCacheManager nearCacheManager = getMapNearCacheManager(nearCacheInstance);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(DEFAULT_NEAR_CACHE_NAME);

        RepairingTask repairingTask = ((MapNearCacheManager) nearCacheManager).getRepairingTask();

        nearCacheInstanceMemoryStats = getNode(nearCacheInstance).hazelcastInstance.getMemoryStats();

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, getSerializationService(nearCacheInstance))
                .setNearCacheInstance(nearCacheInstance)
                .setNearCacheAdapter(new IMapDataStructureAdapter<K, V>(nearCacheMap))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setHasLocalData(true)
                .setInvalidationListener(createInvalidationEventHandler(nearCacheMap))
                .setRepairingTask(repairingTask);
    }

    private Config createConfig(boolean withNearCache) {
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig()
                .setSize(99)
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);

        MapConfig mapConfig = new MapConfig(DEFAULT_NEAR_CACHE_NAME)
                .setBackupCount(1)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setStatisticsEnabled(true)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .setMaxSizeConfig(maxSizeConfig);

        if (withNearCache) {
            mapConfig.setNearCacheConfig(nearCacheConfig);
        }

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setAllocatorType(memoryAllocatorType)
                .setSize(MEMORY_SIZE);

        return new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT))
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }
}
