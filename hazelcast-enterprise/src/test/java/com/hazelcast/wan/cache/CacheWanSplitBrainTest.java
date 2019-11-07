package com.hazelcast.wan.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.CustomWanPublisherConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.CountingWanPublisher;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.impl.DelegatingWanReplicationScheme;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Asserts that the cache merge operation will publish WAN events.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheWanSplitBrainTest extends SplitBrainTestSupport {

    private static final String WAN_REPLICATION_NAME = "wanRef";
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameters(name = "inMemoryFormat:{0} cacheMergePolicy:{1} wanMergePolicy:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {OBJECT, PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},
                {BINARY, PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},
                {NATIVE, com.hazelcast.spi.merge.PassThroughMergePolicy.class, com.hazelcast.spi.merge.PutIfAbsentMergePolicy.class},
                {NATIVE, com.hazelcast.spi.merge.PassThroughMergePolicy.class, com.hazelcast.spi.merge.PutIfAbsentMergePolicy.class},
        });
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(value = 1)
    public Class cacheMergePolicy;

    @Parameter(value = 2)
    public Class wanMergePolicy;

    private String cacheName = randomString();
    private Cache<String, String> cache1;
    private Cache<String, String> cache2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        CustomWanPublisherConfig pc = new CustomWanPublisherConfig()
                .setPublisherId("customPublisherId")
                .setClassName(CountingWanPublisher.class.getName());

        WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName(WAN_REPLICATION_NAME)
                .addCustomPublisherConfig(pc);

        Config config = super.config();
        config.addWanReplicationConfig(wanConfig);
        if (inMemoryFormat == NATIVE) {
            config = getHDConfig(config, POOLED, MEMORY_SIZE);
        }
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        warmUpPartitions(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        CacheConfig<String, String> cacheConfig = newCacheConfig(cacheName, inMemoryFormat, cacheMergePolicy, wanMergePolicy);
        cache1 = createCache(firstBrain[0], cacheConfig);
        cache2 = createCache(secondBrain[0], cacheConfig);

        cache1.put("key", "value");
        cache2.put("key", "passThroughValue");
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        assertEquals("passThroughValue", cache1.get("key"));
        assertEquals("passThroughValue", cache2.get("key"));

        int totalPublishedEvents = 0;
        int totalPublishedBackupEvents = 0;
        for (HazelcastInstance instance : instances) {
            EnterpriseWanReplicationService wanReplicationService
                    = (EnterpriseWanReplicationService) getNodeEngineImpl(instance).getWanReplicationService();
            DelegatingWanReplicationScheme delegate = wanReplicationService.getWanReplicationPublishers(WAN_REPLICATION_NAME);
            for (WanReplicationPublisher publisher : delegate.getPublishers()) {
                CountingWanPublisher countingPublisher = (CountingWanPublisher) publisher;
                totalPublishedEvents += countingPublisher.getCount();
                totalPublishedBackupEvents += countingPublisher.getBackupCount();
            }
        }
        assertEquals("Expected 3 published WAN events", 3, totalPublishedEvents);
        assertEquals("Expected 3 published WAN backup events", 3, totalPublishedBackupEvents);
    }

    private static <K, V> Cache<K, V> createCache(HazelcastInstance hazelcastInstance, CacheConfig<K, V> cacheConfig) {
        CachingProvider cachingProvider = createServerCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        return cacheManager.createCache(cacheConfig.getName(), cacheConfig);
    }

    private static <K, V> CacheConfig<K, V> newCacheConfig(String cacheName, InMemoryFormat inMemoryFormat,
                                                           Class cacheMergePolicy, Class wanMergePolicy) {
        EvictionConfig evictionConfig = new EvictionConfig();
        if (inMemoryFormat == NATIVE) {
            evictionConfig.setSize(90);
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        }

        CacheConfig<K, V> cacheConfig = new CacheConfig<>();
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setName(cacheName);
        cacheConfig.getMergePolicyConfig().setPolicy(cacheMergePolicy.getName());
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setWanReplicationRef(new WanReplicationRef()
                .setName(WAN_REPLICATION_NAME)
                .setMergePolicy(wanMergePolicy.getName()));
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }
}
