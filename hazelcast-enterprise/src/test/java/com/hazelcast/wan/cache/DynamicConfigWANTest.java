package com.hazelcast.wan.cache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class DynamicConfigWANTest extends WanCacheTestSupport {

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
        return InMemoryFormat.BINARY;
    }

    @Test
    public void testDynamicMapConfigWithWAN_smoke() {
        String cacheName = randomMapName();
        String wanSetupName = "atob";

        initConfigA();
        initConfigB();

        setupReplicateFrom(configA, configB, clusterB.length, wanSetupName, PassThroughMergePolicy.class.getName(),
                "default");
        // disable WAN replication for the default cache config (it's auto-enabled by the setupReplicateFrom())
        configA.getCacheConfig("default")
                .setWanReplicationRef(null);

        startClusterA();
        startClusterB();

        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setMergePolicyClassName(PassThroughMergePolicy.class.getName());
        wanRef.setName(wanSetupName);

        CacheSimpleConfig cacheConfig = new CacheSimpleConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setWanReplicationRef(wanRef);
        clusterA[0].getConfig().addCacheConfig(cacheConfig);

        cacheConfig = new CacheSimpleConfig().setName(cacheName);
        clusterB[0].getConfig().addCacheConfig(cacheConfig);

        ICache<Integer, Integer> cache = clusterA[0].getCacheManager().getCache(cacheName);
        for (int i = 0; i < 10; i++) {
            cache.put(i, i);
        }

        assertKeysIn(clusterB, cacheName, 0, 10);
    }

    private void assertKeysIn(HazelcastInstance[] instances, String cacheName, int from, int to) {
        for (final HazelcastInstance instance : instances) {
            final ICache<Integer, Integer> cache = instance.getCacheManager().getCache(cacheName);
            for (int i = from; i < to; i++) {
                final int currentKey = i;
                assertTrueEventually(new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertTrue("Key " + currentKey + " missing in the cache obtained from a member "
                                + instance, cache.containsKey(currentKey));
                    }
                });
            }
        }
    }
}
