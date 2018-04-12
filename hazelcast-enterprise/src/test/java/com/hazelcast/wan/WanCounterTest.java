package com.hazelcast.wan;

import com.hazelcast.cache.HazelcastCachingProvider;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.AbstractHazelcastCacheManager;
import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.wan.WanReplicationPublisherDelegate;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({NightlyTest.class, SlowTest.class})
public class WanCounterTest extends WanReplicationTestSupport {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String TARGET_GROUP_B = "B";
    private static final String CACHE_MANAGER = "cache-manager";
    private int ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE = 3 * 60;

    private ClassLoader classLoaderA;

    @After
    public void cleanup() {
        super.cleanup();
        JsrTestUtil.cleanup();
    }

    @Before
    public void setup() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT);

        configA = getConfig();
        configA.getGroupConfig().setName("A");
        configA.setInstanceName("confA");
        configA.getNetworkConfig().setPort(5701);
        classLoaderA = createCacheManagerClassLoader();
        configA.setClassLoader(classLoaderA);
        CacheSimpleConfig cacheConfigA = configA.getCacheConfig(CACHE_NAME);
        cacheConfigA.setEvictionConfig(evictionConfig)
                    .setMergePolicy(PassThroughCacheMergePolicy.class.getName());

        configB = getConfig();
        configB.getGroupConfig().setName(TARGET_GROUP_B);
        configB.setInstanceName("confB");
        configB.getNetworkConfig().setPort(5801);
        configB.setClassLoader(createCacheManagerClassLoader());
        CacheSimpleConfig cacheConfigB = configB.getCacheConfig(CACHE_NAME);
        cacheConfigB.setEvictionConfig(evictionConfig)
                    .setMergePolicy(PassThroughCacheMergePolicy.class.getName());
    }

    @Test
    public void testCountersReachZeroAfterMigrationCommit() {
        final String replicaName = "testCountersReachZeroAfterMigrationCommit";
        final HazelcastInstance[] clusterA = new HazelcastInstance[4];
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        configA.getCacheConfig(CACHE_NAME)
               .setWanReplicationRef(createWanReplicationRef(replicaName, PassThroughCacheMergePolicy.class.getName(), null));

        // create one instance in cluster A
        configA.setInstanceName(configA.getInstanceName() + 1);
        clusterA[0] = HazelcastInstanceFactory.newHazelcastInstance(configA);

        startClusterB();

        // pause the wan replication to ensure that we have something to migrate
        wanService(clusterA[0]).pause(replicaName, TARGET_GROUP_B);

        createDataInMap(clusterA[0], MAP_NAME, 0, 1000);
        createCacheDataIn(clusterA[0], classLoaderA, CACHE_MANAGER, CACHE_NAME, 0, 1000);
        assertEquals(2000, getPrimaryOutboundQueueCounter(clusterA[0], replicaName, TARGET_GROUP_B));

        // create the second instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 2);
        clusterA[1] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[1]).pause(replicaName, TARGET_GROUP_B);

        // create the third instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 3);
        clusterA[2] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[2]).pause(replicaName, TARGET_GROUP_B);

        // create the fourth instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 4);
        clusterA[3] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[3]).pause(replicaName, TARGET_GROUP_B);

        createDataInMap(clusterA[0], MAP_NAME, 1000, 2000);

        // resume the wan replication on instances
        wanService(clusterA[0]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[1]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[2]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[3]).resume(replicaName, TARGET_GROUP_B);

        verifyOutboundQueueSizesAreEventuallyZero(replicaName, clusterA);

        assertKeysInMap(clusterB, MAP_NAME, 0, 2000);
    }

    @Test
    public void testCountersReachZeroAfterMigrateToNewAndBack() {
        final String replicaName = "testCountersReachZeroAfterMigrateToNewAndBack";
        final HazelcastInstance[] clusterA = new HazelcastInstance[2];
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        configA.getCacheConfig(CACHE_NAME)
               .setWanReplicationRef(createWanReplicationRef(replicaName, PassThroughCacheMergePolicy.class.getName(), null));

        givenSizeOfClusterB(1);

        // create one instance in cluster A
        configA.setInstanceName(configA.getInstanceName() + 1);
        clusterA[0] = HazelcastInstanceFactory.newHazelcastInstance(configA);

        startClusterB();

        // pause the wan replication to ensure that we have something to migrate
        wanService(clusterA[0]).pause(replicaName, TARGET_GROUP_B);

        createDataInMap(clusterA[0], MAP_NAME, 0, 1000);
        createCacheDataIn(clusterA[0], classLoaderA, CACHE_MANAGER, CACHE_NAME, 0, 1000);
        assertEquals(2000, getPrimaryOutboundQueueCounter(clusterA[0], replicaName, TARGET_GROUP_B));

        // create the second instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 2);
        clusterA[1] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[1]).pause(replicaName, TARGET_GROUP_B);

        // terminate the second instance in cluster A -> migrate back
        clusterA[1].getLifecycleService().terminate();

        createDataInMap(clusterA[0], MAP_NAME, 1000, 2000);

        // resume the wan replication on instance
        wanService(clusterA[0]).resume(replicaName, TARGET_GROUP_B);

        verifyOutboundQueueSizesAreEventuallyZero(replicaName, clusterA);

        assertKeysInMap(clusterB, MAP_NAME, 0, 2000);
    }

    @Test
    public void testCountersReachZeroAfterMigrationTargetDisappears() {
        final String replicaName = "testCountersReachZeroAfterTargetDisappears";
        final HazelcastInstance[] clusterA = new HazelcastInstance[4];
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        configA.getCacheConfig(CACHE_NAME)
               .setWanReplicationRef(createWanReplicationRef(replicaName, PassThroughCacheMergePolicy.class.getName(), null));

        // create one instance in cluster A
        configA.setInstanceName(configA.getInstanceName() + 1);
        clusterA[0] = HazelcastInstanceFactory.newHazelcastInstance(configA);

        startClusterB();

        // pause the wan replication to ensure that we have something to migrate
        wanService(clusterA[0]).pause(replicaName, TARGET_GROUP_B);

        createDataInMap(clusterA[0], MAP_NAME, 0, 1000);
        createCacheDataIn(clusterA[0], classLoaderA, CACHE_MANAGER, CACHE_NAME, 0, 1000);
        assertEquals(2000, getPrimaryOutboundQueueCounter(clusterA[0], replicaName, TARGET_GROUP_B));

        // create the second instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 2);
        clusterA[1] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[1]).pause(replicaName, TARGET_GROUP_B);

        // create the third instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 3);
        clusterA[2] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[2]).pause(replicaName, TARGET_GROUP_B);

        // create the fourth instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 4);
        clusterA[3] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        clusterA[3].getLifecycleService().terminate();

        createDataInMap(clusterA[0], MAP_NAME, 1000, 2000);

        // resume the wan replication on the instances
        wanService(clusterA[0]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[1]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[2]).resume(replicaName, TARGET_GROUP_B);

        verifyOutboundQueueSizesAreEventuallyZero(replicaName, clusterA);

        assertKeysInMap(clusterB, MAP_NAME, 0, 2000);
    }

    @Test
    public void testCountersReachZeroAfterMigrationRollback() {
        final String replicaName = "testCountersReachZeroAfterTargetDisappears";
        final AtomicReferenceArray<HazelcastInstance> clusterA = new AtomicReferenceArray<HazelcastInstance>(4);
        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        configA.getCacheConfig(CACHE_NAME)
               .setWanReplicationRef(createWanReplicationRef(replicaName, PassThroughCacheMergePolicy.class.getName(), null));
        WanPublisherConfig wanPublisherConfig = configA.getWanReplicationConfig(replicaName)
                                                       .getWanPublisherConfigs().get(0);
        wanPublisherConfig.setClassName(MigrationBreakerWanPublisher.class.getName());

        // create one instance in cluster A
        configA.setInstanceName(configA.getInstanceName() + 1);
        clusterA.set(0, HazelcastInstanceFactory.newHazelcastInstance(configA));

        startClusterB();

        // pause the wan replication to ensure that we have something to migrate
        wanService(clusterA.get(0)).pause(replicaName, TARGET_GROUP_B);

        createDataInMap(clusterA.get(0), MAP_NAME, 0, 1000);
        createCacheDataIn(clusterA.get(0), classLoaderA, CACHE_MANAGER, CACHE_NAME, 0, 1000);
        assertEquals(2000, getPrimaryOutboundQueueCounter(clusterA.get(0), replicaName, TARGET_GROUP_B));

        // create the second instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 2);
        clusterA.set(1, HazelcastInstanceFactory.newHazelcastInstance(configA));
        wanService(clusterA.get(1)).pause(replicaName, TARGET_GROUP_B);

        // create the third instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 3);
        clusterA.set(2, HazelcastInstanceFactory.newHazelcastInstance(configA));
        wanService(clusterA.get(2)).pause(replicaName, TARGET_GROUP_B);

        // create the fourth instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 4);
        // kill instance 3 during migration to fail the migration
        ((MigrationBreakerWanPublisher) wanReplicationEndpoint(clusterA.get(0), replicaName)).failMigration();
        clusterA.set(3, HazelcastInstanceFactory.newHazelcastInstance(configA));

        createDataInMap(clusterA.get(0), MAP_NAME, 1000, 2000);

        // resume the wan replication on the instances
        wanService(clusterA.get(0)).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA.get(1)).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA.get(2)).resume(replicaName, TARGET_GROUP_B);

        HazelcastInstance[] clusterAInstances = new HazelcastInstance[4];
        for (int i = 0; i < 4; i++) {
            clusterAInstances[i] = clusterA.get(i);
        }
        verifyOutboundQueueSizesAreEventuallyZero(replicaName, clusterAInstances);

        assertKeysInMap(clusterB, MAP_NAME, 0, 2000);
    }

    @Test
    public void testCountersReachZeroAfterClearQueuesWithParallelLoad() {
        final String replicaName = "testCountersReachZeroAfterClearQueuesWithParallelLoad";
        final HazelcastInstance[] clusterA = new HazelcastInstance[1];
        givenSizeOfClusterB(1);

        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        initCluster(clusterA, configA);
        startClusterB();

        final int entriesToPut = 1000;
        final CountDownLatch inTheMiddleOfLoadLatch = new CountDownLatch(entriesToPut / 2);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                createDataInMap(clusterA[0], "map", 0, entriesToPut, inTheMiddleOfLoadLatch);
            }
        });

        assertOpenEventually(inTheMiddleOfLoadLatch);
        wanService(clusterA[0]).clearQueues(replicaName, TARGET_GROUP_B);

        verifyOutboundQueueSizesAreEventuallyZero(replicaName, clusterA);
        executorService.shutdown();
    }

    @Test
    public void testCountersReachZeroAfterClearQueuesDuringMigration() {
        final String replicaName = "testCountersReachZeroAfterClearQueuesDuringMigration";
        final HazelcastInstance[] clusterA = new HazelcastInstance[4];
        givenSizeOfClusterB(1);

        setupReplicateFrom(configA, configB, clusterB.length, replicaName, PassThroughMergePolicy.class.getName());
        WanPublisherConfig wanPublisherConfig = configA.getWanReplicationConfig(replicaName)
                                                       .getWanPublisherConfigs().get(0);
        wanPublisherConfig.setClassName(QueueClearerWanPublisher.class.getName());

        // create one instance in cluster A
        configA.setInstanceName(configA.getInstanceName() + 1);
        clusterA[0] = HazelcastInstanceFactory.newHazelcastInstance(configA);

        startClusterB();

        // pause the wan replication to ensure that we have something to migrate
        wanService(clusterA[0]).pause(replicaName, TARGET_GROUP_B);

        createDataInMap(clusterA[0], MAP_NAME, 0, 1000);
        assertEquals(1000, getPrimaryOutboundQueueCounter(clusterA[0], replicaName, TARGET_GROUP_B));

        // create the second instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 2);
        clusterA[1] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[1]).pause(replicaName, TARGET_GROUP_B);

        // create the third instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 3);
        clusterA[2] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[2]).pause(replicaName, TARGET_GROUP_B);

        // create the fourth instance in cluster A -> migration
        configA.setInstanceName(configA.getInstanceName() + 4);
        ((QueueClearerWanPublisher) wanReplicationEndpoint(clusterA[0], replicaName))
                .clearQueuesOnNextMigration(clusterA[0].getName());

        clusterA[3] = HazelcastInstanceFactory.newHazelcastInstance(configA);
        wanService(clusterA[3]).pause(replicaName, TARGET_GROUP_B);

        // resume the wan replication on the instances
        wanService(clusterA[0]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[1]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[2]).resume(replicaName, TARGET_GROUP_B);
        wanService(clusterA[3]).resume(replicaName, TARGET_GROUP_B);

        // node to clear queues on nodes 1&2&3, since some primary entries were removed by clearQueues on node 1 during migration
        wanService(clusterA[1]).clearQueues(replicaName, TARGET_GROUP_B);
        wanService(clusterA[2]).clearQueues(replicaName, TARGET_GROUP_B);
        wanService(clusterA[3]).clearQueues(replicaName, TARGET_GROUP_B);

        verifyOutboundQueueSizesAreEventuallyZero(replicaName, clusterA);
    }

    private void verifyOutboundQueueSizesAreEventuallyZero(final String replicaName, final HazelcastInstance[] clusterInstances) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : clusterInstances) {
                    if (instance != null && instance.getLifecycleService().isRunning()) {
                        int outboundQueueSize = getPrimaryOutboundQueueCounter(instance, replicaName, TARGET_GROUP_B);

                        WanBatchReplication endpoint = wanReplicationEndpoint(instance, replicaName);
                        int outboundBackupQueueSize = endpoint.getCurrentBackupElementCount();

                        assertEquals(0, outboundQueueSize);
                        assertEquals(0, outboundBackupQueueSize);
                    }
                }
            }
        });
    }

    private int getPrimaryOutboundQueueCounter(HazelcastInstance hz, String replicaName, String targetName) {
        return wanService(hz).getStats()
                             .get(replicaName).getLocalWanPublisherStats()
                             .get(targetName).getOutboundQueueSize();
    }

    private WanReplicationService wanService(HazelcastInstance hz) {
        return getNode(hz).nodeEngine.getWanReplicationService();
    }

    private void createDataInMap(HazelcastInstance node, String mapName, int start, int end) {
        createDataInMap(node, mapName, start, end, null);
    }

    private void createDataInMap(HazelcastInstance node, String mapName, int start, int end, CountDownLatch loadLatch) {
        IMap<Integer, String> m = node.getMap(mapName);
        for (; start < end; start++) {
            m.put(start, node.getConfig().getGroupConfig().getName() + start);
            if (loadLatch != null) {
                loadLatch.countDown();
            }
        }
    }

    private int createCacheDataIn(HazelcastInstance instance, ClassLoader classLoader,
                                  String cacheManager, String cacheName,
                                  int start, int end) {
        ICache<Integer, String> myCache = getOrCreateCache(instance, cacheManager, cacheName, classLoader);

        for (; start < end; start++) {
            final String value = instance.getConfig().getGroupConfig().getName() + start;
            myCache.put(start, value);
        }
        return myCache.size();
    }

    private ICache<Integer, String> getOrCreateCache(HazelcastInstance instance, String cacheManager, String cacheName,
                                                     ClassLoader classLoader) {
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = HazelcastCachingProvider.propertiesByInstanceName(instance.getConfig().getInstanceName());
        try {
            URI cacheManagerName = new URI(cacheManager);
            AbstractHazelcastCacheManager manager = (AbstractHazelcastCacheManager) provider.getCacheManager(cacheManagerName,
                    classLoader, properties);

            CacheConfig<Integer, String> cacheConfig = createCacheConfig(cacheName, instance);
            return manager.getOrCreateCache(cacheName, cacheConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CacheConfig<Integer, String> createCacheConfig(String cacheName, HazelcastInstance node) throws Exception {
        CacheConfig<Integer, String> cacheConfig = new CacheConfig<Integer, String>(node.getConfig().getCacheConfig(cacheName));
        cacheConfig.setInMemoryFormat(getMemoryFormat());
        cacheConfig.setStatisticsEnabled(true);
        EvictionConfig evictionConfig = new EvictionConfig()
                .setSize(90)
                .setMaximumSizePolicy(ENTRY_COUNT);
        cacheConfig.setEvictionConfig(evictionConfig);
        return cacheConfig;
    }

    private WanBatchReplication wanReplicationEndpoint(HazelcastInstance instance, String replicaName) {
        WanReplicationPublisherDelegate atob = (WanReplicationPublisherDelegate) wanService(instance)
                .getWanReplicationPublisher(replicaName);
        return (WanBatchReplication) atob.getEndpoint(TARGET_GROUP_B);
    }

    private void assertKeysInMap(final HazelcastInstance[] cluster, final String mapName, final int start, final int end) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(checkKeysInMap(cluster, mapName, start, end));
            }
        }, ASSERT_TRUE_EVENTUALLY_TIMEOUT_VALUE);
    }

    private boolean checkKeysInMap(HazelcastInstance[] cluster, String mapName, int start, int end) {
        IMap m = getMap(cluster, mapName);
        for (; start < end; start++) {
            if (!m.containsKey(start)) {
                return false;
            }
        }
        return true;
    }

    private IMap<Integer, Integer> getMap(HazelcastInstance[] cluster, String mapName) {
        HazelcastInstance node = getNode(cluster);
        return node.getMap(mapName);
    }

    private void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz,
                                    String setupName, String policy) {
        setupReplicateFrom(fromConfig, toConfig, clusterSz, setupName, policy, null);
    }

    private void setupReplicateFrom(Config fromConfig, Config toConfig, int clusterSz,
                                    String setupName, String policy, String filter) {
        setupWanReplicationConfig(fromConfig, toConfig, clusterSz, setupName);

        WanReplicationRef wanRef = createWanReplicationRef(setupName, policy, filter);
        fromConfig.getMapConfig("default").setWanReplicationRef(wanRef);
    }

    private WanReplicationRef createWanReplicationRef(String setupName, String policy, String filter) {
        WanReplicationRef wanRef = new WanReplicationRef();
        wanRef.setName(setupName);
        wanRef.setMergePolicy(policy);
        if (filter != null) {
            wanRef.addFilter(filter);
        }
        return wanRef;
    }

    private void setupWanReplicationConfig(Config fromConfig, Config toConfig, int clusterSz, String setupName) {
        WanReplicationConfig wanConfig = fromConfig.getWanReplicationConfig(setupName);
        if (wanConfig == null) {
            wanConfig = new WanReplicationConfig();
            wanConfig.setName(setupName);
        }
        wanConfig.addWanPublisherConfig(targetCluster(toConfig, clusterSz));
        fromConfig.addWanReplicationConfig(wanConfig);
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    private static class MigrationBreakerWanPublisher extends WanBatchReplication {
        private final AtomicBoolean failMigration = new AtomicBoolean();

        @Override
        public void onMigrationStart(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
            super.onMigrationStart(partitionId, currentReplicaIndex, newReplicaIndex);
            if (failMigration.compareAndSet(true, false)) {
                throw new RuntimeException("Intentionally failing migration");
            }
        }

        private void failMigration() {
            failMigration.set(true);
        }
    }

    private static class QueueClearerWanPublisher extends WanBatchReplication {
        private final AtomicBoolean clearQueuesFlag = new AtomicBoolean();
        private final AtomicInteger hitCount = new AtomicInteger();
        private final AtomicReference<String> nodeName = new AtomicReference<String>();

        @Override
        public void onMigrationCommit(int partitionId, int currentReplicaIndex, int newReplicaIndex) {
            super.onMigrationCommit(partitionId, currentReplicaIndex, newReplicaIndex);

            if (node.getNodeEngine().getHazelcastInstance().getName().equals(nodeName.get())
                    && clearQueuesFlag.get()
                    && hitCount.incrementAndGet() == 10) {
                clearQueues();
            }
        }

        private void clearQueuesOnNextMigration(String name) {
            nodeName.set(name);
            clearQueuesFlag.set(true);
        }
    }

    private CacheManagerClassLoader createCacheManagerClassLoader() {
        ClassLoader currentClassLoader = getClass().getClassLoader();
        return new CacheManagerClassLoader(new URL[0], currentClassLoader);
    }

    private static class CacheManagerClassLoader extends URLClassLoader {

        CacheManagerClassLoader(URL[] urls, ClassLoader classLoader) {
            super(urls, classLoader);
        }

        @Override
        public String toString() {
            return "test";
        }
    }
}
