package com.hazelcast.wan.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.CountingWanPublisher;
import com.hazelcast.wan.WanPublisher;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public class WanCachePutAllTest extends WanCacheTestSupport {

    @Parameterized.Parameters(name = "memoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NATIVE},
                {BINARY},
        });
    }

    @Parameterized.Parameter(0)
    public InMemoryFormat memoryFormat;

    @BeforeClass
    public static void initJCache() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrTestUtil.cleanup();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return memoryFormat;
    }

    @Test
    public void testPutAllPublishesExpectedNumberOfWanEvents() {
        int countItemsToPutAll = 10;
        String cacheName = randomMapName();
        String wanSetupName = "atob";
        setupClustersWithCacheName(cacheName, wanSetupName);

        ICache<Integer, Integer> cache = clusterA[0].getCacheManager().getCache(cacheName);

        Map<Integer, Integer> mapForPutAll = new HashMap<>(countItemsToPutAll);
        for (int i = 0; i < countItemsToPutAll; i++) {
            mapForPutAll.put(i, i);
        }

        cache.putAll(mapForPutAll);

        verifyNumberOfPublishedWanEvents(countItemsToPutAll, wanSetupName);
    }

    private void verifyNumberOfPublishedWanEvents(int expectedWanEvents, String wanSetupName) {
        int totalPublishedEvents = 0;
        int totalPublishedBackupEvents = 0;
        for (HazelcastInstance instance : clusterA) {
            final EnterpriseWanReplicationService wanReplicationService =
                    (EnterpriseWanReplicationService) getNodeEngineImpl(instance).getWanReplicationService();
            final DelegatingWanScheme delegate = wanReplicationService.getWanReplicationPublishers(wanSetupName);
            for (WanPublisher publisher : delegate.getPublishers()) {
                final CountingWanPublisher countingPublisher = (CountingWanPublisher) publisher;
                totalPublishedEvents += countingPublisher.getCount();
                totalPublishedBackupEvents += countingPublisher.getBackupCount();
            }
        }

        assertEquals(expectedWanEvents, totalPublishedEvents);
        assertEquals(expectedWanEvents, totalPublishedBackupEvents);
    }

    private void setupClustersWithCacheName(String cacheName, String wanSetupName) {
        initConfigA();
        initConfigB();

        setupReplicateFrom(configA,
                "customPublisherId", CountingWanPublisher.class.getName(),
                wanSetupName, PassThroughMergePolicy.class.getName(),
                "default", null);
        // disable WAN replication for the default cache config (it's auto-enabled by the setupReplicateFrom())
        configA.getCacheConfig("default")
                .setWanReplicationRef(null);

        startClusterA();
        startClusterB();

        configureCacheWithWanReplication(cacheName, wanSetupName);
    }

    private void configureCacheWithWanReplication(String cacheName, String wanSetupName) {
        WanReplicationRef wanRef = new WanReplicationRef()
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName())
                .setName(wanSetupName);

        CacheSimpleConfig cacheConfig = new CacheSimpleConfig()
                .setName(cacheName)
                .setInMemoryFormat(memoryFormat);
        cacheConfig.setWanReplicationRef(wanRef);

        EvictionConfig evictionConfig = new EvictionConfig();
        if (isNativeMemoryEnabled()) {
            evictionConfig.setSize(90)
                    .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE);
        } else {
            evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        }
        cacheConfig.setEvictionConfig(evictionConfig);

        clusterA[0].getConfig().addCacheConfig(cacheConfig);

        cacheConfig = new CacheSimpleConfig().setName(cacheName);
        clusterB[0].getConfig().addCacheConfig(cacheConfig);
    }
}
