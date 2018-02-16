package com.hazelcast.wan.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.CountingWanEndpoint;
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
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheWanSplitBrainTest extends SplitBrainTestSupport {

    private static final String WAN_REPLICATION_NAME = "wanRef";
    private static final MemorySize MEMORY_SIZE = new MemorySize(128, MemoryUnit.MEGABYTES);

    @Parameters(name = "inMemoryFormat:{0} cacheMergePolicy:{1} wanMergePolicy:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {OBJECT, PassThroughCacheMergePolicy.class, PutIfAbsentCacheMergePolicy.class},
                {OBJECT, PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},
                {BINARY, PassThroughCacheMergePolicy.class, PutIfAbsentCacheMergePolicy.class},
                {BINARY, PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},
                {NATIVE, PassThroughCacheMergePolicy.class, PutIfAbsentCacheMergePolicy.class},
                {NATIVE, PassThroughMergePolicy.class, PutIfAbsentMergePolicy.class},
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
        WanPublisherConfig wanPublisherConfig = new WanPublisherConfig()
                .setClassName(CountingWanEndpoint.class.getName());

        WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName(WAN_REPLICATION_NAME)
                .addWanPublisherConfig(wanPublisherConfig);

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
            WanReplicationPublisherDelegate delegate
                    = (WanReplicationPublisherDelegate) wanReplicationService.getWanReplicationPublisher(WAN_REPLICATION_NAME);
            for (WanReplicationEndpoint endpoint : delegate.getEndpoints()) {
                CountingWanEndpoint countingEndpoint = (CountingWanEndpoint) endpoint;
                totalPublishedEvents += countingEndpoint.getCount();
                totalPublishedBackupEvents += countingEndpoint.getBackupCount();
            }
        }
        assertEquals("Expected 3 published WAN events", 3, totalPublishedEvents);
        assertEquals("Expected 3 published WAN backup events", 3, totalPublishedBackupEvents);
    }

    private static <K, V> Cache<K, V> createCache(HazelcastInstance hazelcastInstance, CacheConfig<K, V> cacheConfig) {
        CachingProvider cachingProvider = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        return cacheManager.createCache(cacheConfig.getName(), cacheConfig);
    }

    private static <K, V> CacheConfig<K, V> newCacheConfig(String cacheName, InMemoryFormat inMemoryFormat,
                                                           Class cacheMergePolicy, Class wanMergePolicy) {
        EvictionConfig evictionConfig = new EvictionConfig();
        if (inMemoryFormat == NATIVE) {
            evictionConfig.setSize(90);
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        }

        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>();
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setName(cacheName);
        cacheConfig.setMergePolicy(cacheMergePolicy.getName());
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setWanReplicationRef(new WanReplicationRef()
                .setName(WAN_REPLICATION_NAME)
                .setMergePolicy(wanMergePolicy.getName()));
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }
}
