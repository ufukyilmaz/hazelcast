package com.hazelcast.wan;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.enterprise.wan.impl.replication.WanBatchReplication;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
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
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Caching;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanCacheTestSupport.verifyCacheReplicated;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationPublisher;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class WanCounterMigrationTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";
    private static final int SNAPSHOT_OVERWRITES = 3;

    @Parameters(name = "backups:{0} snapshot:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {0, false},
                {0, true},
                {1, false},
                {1, true},
                {2, false},
                {2, true},
                {3, false},
                {3, true}
        });
    }

    @Parameter
    public int backupCount;

    @Parameter(1)
    public boolean snapshotEnabled;

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
        Caching.getCachingProvider().close();
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
                .withSnapshotEnabled(snapshotEnabled)
                .withInitialPublisherState(WanPublisherState.PAUSED)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

        sourceCluster.getConfig().getMapConfig(MAP_NAME)
                .setBackupCount(backupCount)
                .setAsyncBackupCount(0);

        // uncomment to dump the counters when debugging locally
        // dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT);

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig).getMergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getName());
    }

    @Test
    public void testCountersReachZeroAfterMigrationCommit() {
        sourceCluster.startAClusterMember();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        sourceCluster.startClusterMembers(new PausingClusterMemberStartAction());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        overwriteIfSnapshotEnabled();

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterMigrateToNewAndBack() {
        sourceCluster.startAClusterMember();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        HazelcastInstance secondClusterMember = sourceCluster.startAClusterMember();
        secondClusterMember.getLifecycleService().terminate();
        waitAllForSafeState(sourceCluster.getMembers());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        overwriteIfSnapshotEnabled();

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterMigrationTargetDisappears() {
        sourceCluster.startAClusterMember();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        sourceCluster.startClusterMembers(2);
        HazelcastInstance startedMember = sourceCluster.startAClusterMember();
        startedMember.getLifecycleService().terminate();
        waitAllForSafeState(sourceCluster.getMembers());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        overwriteIfSnapshotEnabled();

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    @Test
    public void testCountersReachZeroAfterMigrationRollback() {
        HazelcastInstance master = sourceCluster.startAClusterMember();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        fillCache(sourceCluster, CACHE_NAME, 0, 1000);

        sourceCluster.startClusterMembers(2);
        ((MigrationBreakerWanPublisher) wanReplicationPublisher(master, wanReplication)).failMigration();
        HazelcastInstance startedMember = sourceCluster.startAClusterMember();
        startedMember.getLifecycleService().terminate();
        waitAllForSafeState(sourceCluster.getMembers());

        fillMap(sourceCluster, MAP_NAME, 1000, 2000);
        fillCache(sourceCluster, CACHE_NAME, 1000, 2000);

        overwriteIfSnapshotEnabled();

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

        fillMap(sourceCluster, MAP_NAME, 0, 8);

        for (HazelcastInstance member : sourceCluster.getMembers()) {
            member.shutdown();
            sourceCluster.startAClusterMember();
        }
        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        overwriteIfSnapshotEnabled();

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    private void overwriteIfSnapshotEnabled() {
        if (snapshotEnabled) {
            for (int i = 0; i < SNAPSHOT_OVERWRITES; i++) {
                fillMap(sourceCluster, MAP_NAME, 500, 1500);
                fillCache(sourceCluster, CACHE_NAME, 500, 1500);
            }
        }
    }

    private class PausingClusterMemberStartAction implements ClusterMemberStartAction {

        @Override
        public void onMemberStarted(HazelcastInstance instance) {
            wanReplicationService(instance).pause(REPLICATION_NAME, targetCluster.getConfig().getClusterName());
        }
    }

    private static class MigrationBreakerWanPublisher extends WanBatchReplication {
        private final AtomicBoolean failMigration = new AtomicBoolean();

        @Override
        public void onMigrationStart(PartitionMigrationEvent event) {
            super.onMigrationStart(event);
            if (failMigration.compareAndSet(true, false)) {
                throw new RuntimeException("Intentionally failing migration");
            }
        }

        private void failMigration() {
            failMigration.set(true);
        }
    }

}
