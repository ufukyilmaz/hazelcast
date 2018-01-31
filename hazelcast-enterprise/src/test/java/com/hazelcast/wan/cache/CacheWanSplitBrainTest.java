package com.hazelcast.wan.cache;

import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.cache.merge.PutIfAbsentCacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.enterprise.wan.EnterpriseWanReplicationService;
import com.hazelcast.enterprise.wan.WanReplicationEndpoint;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * Asserts that the cache merge operation will publish WAN events
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheWanSplitBrainTest extends SplitBrainTestSupport {

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{
                InMemoryFormat.OBJECT,
                InMemoryFormat.BINARY,
                // InMemoryFormat.NATIVE // uncomment when HD cache merge is implemented
        });
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;


    private static final String WAN_REPLICATION_NAME = "wanRef";
    private String cacheName = randomString();
    private Cache<String, String> cache1;
    private Cache<String, String> cache2;
    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected int[] brains() {
        // second half should merge to first 
        return new int[]{2, 1};
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

        CacheConfig cacheConfig = newCacheConfig(cacheName, inMemoryFormat);
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
        for (HazelcastInstance instance : instances) {
            final EnterpriseWanReplicationService wanReplicationService =
                    (EnterpriseWanReplicationService) getNodeEngineImpl(instance).getWanReplicationService();
            final WanReplicationPublisherDelegate delegate =
                    (WanReplicationPublisherDelegate) wanReplicationService.getWanReplicationPublisher(WAN_REPLICATION_NAME);
            for (WanReplicationEndpoint endpoint : delegate.getEndpoints()) {
                final CountingWanEndpoint countingEndpoint = (CountingWanEndpoint) endpoint;
                totalPublishedEvents += countingEndpoint.counter.get();
            }
        }

        assertEquals(3, totalPublishedEvents);
    }

    @Override
    protected Config config() {
        Config config = super.config();
        final WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName(WAN_REPLICATION_NAME)
                .addWanPublisherConfig(new WanPublisherConfig().setClassName(CountingWanEndpoint.class.getName()));
        config.addWanReplicationConfig(wanConfig);
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            MemorySize memorySize = new MemorySize(128, MemoryUnit.MEGABYTES);
            final NativeMemoryConfig nativeMemoryConfig = new NativeMemoryConfig()
                    .setAllocatorType(MemoryAllocatorType.POOLED)
                    .setSize(memorySize).setEnabled(true);
            config.setNativeMemoryConfig(nativeMemoryConfig);
        }
        return config;
    }

    private static Cache createCache(HazelcastInstance hazelcastInstance, CacheConfig cacheConfig) {
        CachingProvider cachingProvider1 = HazelcastServerCachingProvider.createCachingProvider(hazelcastInstance);
        CacheManager cacheManager1 = cachingProvider1.getCacheManager();
        return cacheManager1.createCache(cacheConfig.getName(), cacheConfig);
    }

    private static CacheConfig newCacheConfig(String cacheName, InMemoryFormat inMemoryFormat) {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setMergePolicy(PassThroughCacheMergePolicy.class.getName());
        cacheConfig.setInMemoryFormat(inMemoryFormat);
        cacheConfig.setWanReplicationRef(
                new WanReplicationRef()
                        .setName(WAN_REPLICATION_NAME)
                        .setMergePolicy(PutIfAbsentCacheMergePolicy.class.getName()));
        EvictionConfig evictionConfig = new EvictionConfig();
        if (inMemoryFormat == InMemoryFormat.NATIVE) {
            cacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
            evictionConfig.setSize(90);
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaximumSizePolicy(EvictionConfig.MaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }


    private static class MergeLifecycleListener implements LifecycleListener {
        private final CountDownLatch latch;

        MergeLifecycleListener(int mergingClusterSize) {
            latch = new CountDownLatch(mergingClusterSize);
        }

        @Override
        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleEvent.LifecycleState.MERGED) {
                latch.countDown();
            }
        }

        void await() {
            assertOpenEventually(latch);
        }
    }
}
