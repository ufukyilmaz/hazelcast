package com.hazelcast.test.modulepath;

import static com.hazelcast.test.modulepath.EnterpriseTestUtils.assertClusterSize;
import static com.hazelcast.test.modulepath.EnterpriseTestUtils.createConfigWithTcpJoin;
import static com.hazelcast.test.modulepath.EnterpriseTestUtils.createServerCachingProvider;
import static com.hazelcast.test.modulepath.EnterpriseTestUtils.waitClusterForSafeState;
import static org.junit.Assert.assertEquals;

import java.io.File;

import javax.cache.Cache;
import javax.cache.spi.CachingProvider;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

/**
 * Hot restart tests on modulepath. They copy behavior from
 * <a href="https://github.com/hazelcast/hazelcast-code-samples/tree/v3.10/enterprise/hot-restart/src/main/java">hot-restart
 * code samples</a>.
 */
public class JCacheHotRestartTest {

    @Rule
    public TemporaryFolder ruleTempFolder = new TemporaryFolder();

    @BeforeClass
    public static void cleanUp() {
        HazelcastInstanceFactory.terminateAll();
    }

    @After
    public void after() {
        cleanUp();
    }

    /**
     * <pre>
     * Given: A single member is running on modulepath.
     * When: A JCache instance with hot-restart enabled is used and member is restarted.
     * Then: The cache values are available after the restart.
     * </pre>
     */
    @Test
    public void testSingleNodeRestart() {
        HazelcastInstance instance = newHazelcastInstance(5701);
        assertClusterSize(1, instance);
        initCache(instance);
        instance.shutdown();
        instance = newHazelcastInstance(5701);
        assertCacheValues(createCache(instance));
    }

    /**
     * <pre>
     * Given: Two members cluster is running on modulepath and a JCache with hot-restart enabled is created.
     * When: The cluster status is set PASSIVE and one member is replaced by a new one with the same config.
     * Then: The cache values are available after the cluster re-activation.
     * </pre>
     */
    @Test
    public void testRollingUpgrade() {
        HazelcastInstance instance1 = newHazelcastInstance(5701);
        HazelcastInstance instance2 = newHazelcastInstance(5702);

        Cache<Integer, String> cache = initCache(instance2);

        instance2.getCluster().changeClusterState(ClusterState.PASSIVE);

        instance1.shutdown();
        waitClusterForSafeState(instance2);

        instance1 = newHazelcastInstance(5701);

        instance1.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertClusterSize(2, instance1, instance2);
        assertCacheValues(cache);
    }

    /**
     * <pre>
     * Given: Two members cluster is running on modulepath and a JCache with hot-restart enabled is created.
     * When: The cluster is shut down and both members are newly started.
     * Then: The cache values are available after the cluster re-activation.
     * </pre>
     */
    @Test
    public void testMultiNodeRestart() {
        HazelcastInstance instance1 = newHazelcastInstance(5701);
        HazelcastInstance instance2 = newHazelcastInstance(5702);

        initCache(instance1);

        instance2.getCluster().shutdown();

        // Offload to a new thread - instances should start in parallel to be able to do hot-restart verification
        new Thread() {
            public void run() {
                newHazelcastInstance(5701);
            }
        }.start();

        instance2 = newHazelcastInstance(5702);
        instance2.getCluster().changeClusterState(ClusterState.ACTIVE);

        assertCacheValues(createCache(instance2));
    }

    private HazelcastInstance newHazelcastInstance(int port) {
        Config config = createConfigWithTcpJoin(port);

        HotRestartPersistenceConfig hotRestartConfig = config.getHotRestartPersistenceConfig();
        hotRestartConfig.setEnabled(true).setBaseDir(new File(ruleTempFolder.getRoot(), Integer.toString(port)));

        return Hazelcast.newHazelcastInstance(config);
    }

    private static Cache<Integer, String> createCache(HazelcastInstance instance) {
        CachingProvider cachingProvider = createServerCachingProvider(instance);

        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>("cache");
        cacheConfig.setBackupCount(0).getHotRestartConfig().setEnabled(true);

        return cachingProvider.getCacheManager().createCache("cache", cacheConfig);
    }

    protected void assertCacheValues(Cache<Integer, String> cache) {
        for (int i = 0; i < 50; i++) {
            assertEquals("value" + i, cache.get(i));
        }
    }

    protected Cache<Integer, String> initCache(HazelcastInstance instance) {
        Cache<Integer, String> cache = createCache(instance);
        for (int i = 0; i < 50; i++) {
            cache.put(i, "value" + i);
        }
        return cache;
    }
}
