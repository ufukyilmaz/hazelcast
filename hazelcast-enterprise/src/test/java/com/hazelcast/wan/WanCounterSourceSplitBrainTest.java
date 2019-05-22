package com.hazelcast.wan;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanCacheTestSupport.verifyCacheReplicated;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class WanCounterSourceSplitBrainTest {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    @Parameters(name = "cluster:{0} isolated:{1} backup:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                // ignored case for now, known to be broken
                // see issue EE #2091
                // {2, 1, 0},
                {2, 1, 1},
                {3, 1, 1},
                {3, 1, 2},
                {4, 1, 1},
                {4, 2, 2},
                {5, 1, 1},
                {5, 2, 3}
        });
    }

    @Parameter
    public int sourceClusterSize;

    @Parameter(1)
    public int numberOfIsolatedNodes;

    @Parameter(2)
    public int backupCount;

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private List<HazelcastInstance> isolatedNodes;

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
        sourceCluster = clusterA(factory, sourceClusterSize).setup();
        targetCluster = clusterB(factory, 1).setup();

        configureCache(sourceCluster);
        configureCache(targetCluster);

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughCacheMergePolicy.class)
                     .setup();

        sourceCluster.getConfig().getMapConfig(MAP_NAME)
                     .setBackupCount(backupCount)
                     .setAsyncBackupCount(0);

        sourceCluster.getConfig().getCacheConfig(CACHE_NAME)
                     .setBackupCount(backupCount)
                     .setAsyncBackupCount(0);

        Config config = sourceCluster.getConfig();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "2");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "2");
        config.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "2");
        config.setProperty(GroupProperty.MAX_JOIN_MERGE_TARGET_SECONDS.getName(), "2");

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
    public void testCountersReachZeroAfterHealingFullSplit() {
        // using this latch temporarily to synchronize resuming WAN replication and split-brain
        // healing in the test as a workaround to the known #2987 issue
        // TODO remove this latch if the issue #2987 is fixed
        CountDownLatch isolatedNodesMergeLatch = new CountDownLatch(numberOfIsolatedNodes);

        sourceCluster.startClusterAndWaitForSafeState();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 100);
        fillCache(sourceCluster, CACHE_NAME, 0, 100);

        pickIsolatedNodes();

        fullSplit();
        waitForExpectedSplitClusterSize();

        fillMap(isolatedNodes.get(0), MAP_NAME, 100, 150);
        fillCache(isolatedNodes.get(0), CACHE_NAME, 100, 150);

        registerIsolatedNodeMergeListener(isolatedNodesMergeLatch);
        healFullSplit();
        waitForExpectedSplitHealedClusterSize();
        assertOpenEventually(isolatedNodesMergeLatch);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyCacheReplicated(sourceCluster, targetCluster, CACHE_NAME);
    }

    private void registerIsolatedNodeMergeListener(CountDownLatch mergedLatch) {
        for (HazelcastInstance instance : isolatedNodes) {
            instance.getLifecycleService().addLifecycleListener(lifecycleEvent -> {
                if (MERGED == lifecycleEvent.getState()) {
                    mergedLatch.countDown();
                }
            });
        }
    }

    private void pickIsolatedNodes() {
        isolatedNodes = new ArrayList<>(numberOfIsolatedNodes);

        HazelcastInstance[] members = sourceCluster.getMembers();
        for (int i = 0; i < numberOfIsolatedNodes; i++) {
            isolatedNodes.add(members[members.length - i - 1]);
        }
    }

    private void waitForExpectedSplitClusterSize() {
        int sourceClusterSize = sourceCluster.size();
        int remainingClusterSize = sourceClusterSize - isolatedNodes.size();
        int isolatedClusterSize = isolatedNodes.size();

        for (HazelcastInstance member : sourceCluster.getMembers()) {
            if (isolatedNodes.contains(member)) {
                assertClusterSizeEventually(isolatedClusterSize, member);
            } else {
                assertClusterSizeEventually(remainingClusterSize, member);
            }
        }
    }

    private void waitForExpectedSplitHealedClusterSize() {
        int sourceClusterSize = sourceCluster.size();

        for (HazelcastInstance member : sourceCluster.getMembers()) {
            assertClusterSizeEventually(sourceClusterSize, member);
        }
    }

    private void fullSplit() {
        HazelcastInstance[] members = sourceCluster.getMembers();
        for (HazelcastInstance isolatedMember : isolatedNodes) {
            for (HazelcastInstance member : members) {
                if (!isolatedNodes.contains(member)) {
                    blockCommunicationBetween(member, isolatedMember);
                }
            }
        }
    }

    private void healFullSplit() {
        HazelcastInstance[] members = sourceCluster.getMembers();
        for (HazelcastInstance isolatedMember : isolatedNodes) {
            for (HazelcastInstance member : members) {
                if (!isolatedNodes.contains(member)) {
                    unblockCommunicationBetween(member, isolatedMember);
                }
            }
        }
    }
}
