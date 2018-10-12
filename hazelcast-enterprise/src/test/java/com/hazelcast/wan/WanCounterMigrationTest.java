package com.hazelcast.wan;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.ClusterMemberStartAction;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanCacheTestSupport.verifyCacheReplicated;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationEndpoint;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanCounterMigrationTest {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @BeforeClass
    public static void setupClass() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupClass() {
        JsrTestUtil.cleanup();
    }

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 4).setup();
        targetCluster = clusterB(factory, 1).setup();

        configureCache(sourceCluster);
        configureCache(targetCluster);

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withWanPublisher(MigrationBreakerWanPublisher.class)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughCacheMergePolicy.class)
                     .setup();

        // uncomment to dump the counters when debugging locally
        // dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT);

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig)
                   .setMergePolicy(PassThroughCacheMergePolicy.class.getName());
    }

    @Test
    public void testCountersReachZeroAfterMigrationCommit() {
        sourceCluster.startAClusterMember();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        sourceCluster.startClusterMembers(new PausingClusterMemberStartAction());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterMigrateToNewAndBack() {
        sourceCluster.startAClusterMember();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        HazelcastInstance secondClusterMember = sourceCluster.startAClusterMember();
        secondClusterMember.getLifecycleService().terminate();
        waitAllForSafeState(sourceCluster.getMembers());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterMigrationTargetDisappears() {
        sourceCluster.startAClusterMember();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        sourceCluster.startClusterMembers(2, new PausingClusterMemberStartAction());
        HazelcastInstance startedMember = sourceCluster.startAClusterMember();
        startedMember.getLifecycleService().terminate();
        waitAllForSafeState(sourceCluster.getMembers());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterMigrationRollback() {
        HazelcastInstance master = sourceCluster.startAClusterMember();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        sourceCluster.startClusterMembers(2, new PausingClusterMemberStartAction());
        ((MigrationBreakerWanPublisher) wanReplicationEndpoint(master, wanReplication)).failMigration();
        HazelcastInstance startedMember = sourceCluster.startAClusterMember();
        startedMember.getLifecycleService().terminate();
        waitAllForSafeState(sourceCluster.getMembers());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterBouncingSourceCluster() {
        sourceCluster.getConfig().setProperty(GroupProperty.PARTITION_COUNT.getName(), Integer.toString(4));

        sourceCluster.startCluster();
        targetCluster.startCluster();

        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        fillMap(sourceCluster, MAP_NAME, 0, 8);

        for (HazelcastInstance member : sourceCluster.getMembers()) {
            member.shutdown();
            sourceCluster.startAClusterMember();
        }

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    private class PausingClusterMemberStartAction implements ClusterMemberStartAction {

        @Override
        public void onMemberStarted(HazelcastInstance instance) {
            wanReplicationService(instance).pause(REPLICATION_NAME, targetCluster.getConfig().getGroupConfig().getName());
        }
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

}
