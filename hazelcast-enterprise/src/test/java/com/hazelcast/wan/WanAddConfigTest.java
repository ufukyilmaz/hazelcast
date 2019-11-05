package com.hazelcast.wan;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import com.hazelcast.wan.impl.AddWanConfigResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.cache.Caching;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static com.hazelcast.test.HazelcastTestSupport.spawn;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.Cluster.clusterC;
import static com.hazelcast.wan.fw.WanCacheTestSupport.fillCache;
import static com.hazelcast.wan.fw.WanCacheTestSupport.getOrCreateCache;
import static com.hazelcast.wan.fw.WanCacheTestSupport.verifyCacheReplicated;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.waitForSyncToComplete;
import static com.hazelcast.wan.map.MapWanBatchReplicationTest.isAllMembersConnected;
import static com.hazelcast.wan.map.MapWanReplicationTestSupport.assertKeysNotInEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanAddConfigTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-debug.xml");

    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster clusterA;
    private Cluster clusterB;
    private Cluster clusterC;

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    private final ILogger logger = Logger.getLogger(WanAddConfigTest.class);

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
        clusterA = clusterA(factory, 2).setup();
        clusterB = clusterB(factory, 2).setup();
        clusterC = clusterC(factory, 2).setup();

        configureCache(clusterA);
        configureCache(clusterB);
        configureCache(clusterC);

        WanReplication wanReplication = replicate()
                .from(clusterA)
                .to(null)
                .withSetupName(REPLICATION_NAME)
                .setup();


        clusterA.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

        clusterA.replicateCache(CACHE_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

//         uncomment to dump the counters when debugging locally
        //dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT);

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig)
                .getMergePolicyConfig().setPolicy(PassThroughMergePolicy.class.getName());
    }

    @Test
    public void testConcurrentMapMutation() throws ExecutionException, InterruptedException {
        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();

        clusterA.startCluster();
        clusterB.startCluster();

        final AtomicBoolean mutationActive = new AtomicBoolean();

        Future mutationFuture = spawn(() -> {
            while (mutationActive.get()) {
                fillMap(clusterA, MAP_NAME, 0, 100);
                fillCache(clusterA, CACHE_NAME, 0, 100);
            }
        });

        assertEquals(0, clusterB.getAMember().getMap(MAP_NAME).size());
        assertEquals(0, getOrCreateCache(clusterB, CACHE_NAME).size());
        clusterA.addWanReplication(toBReplication);

        mutationActive.set(false);
        mutationFuture.get();

        fillMap(clusterA, MAP_NAME, 0, 1000);
        fillCache(clusterA, CACHE_NAME, 0, 1000);
        verifyMapReplicated(clusterA, clusterB, MAP_NAME);
        verifyCacheReplicated(clusterA, clusterB, CACHE_NAME);

        verifyEventCountersAreEventuallyZero(clusterA, toBReplication);
    }

    @Test
    public void testBouncingClusterAddsWanReplication() throws InterruptedException, ExecutionException {
        final AtomicBoolean bouncingActive = new AtomicBoolean(true);
        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();

        clusterA.startCluster();

        Future bouncingFuture = spawn(() -> {
            while (bouncingActive.get()) {
                HazelcastInstance[] members = clusterA.getMembers();
                for (int i = 0; i < members.length; i++) {
                    logger.info("Source cluster WAN config counts before bouncing: "
                            + getSourceClusterConfigCountMap(toBReplication.getSetupName()).toString());
                    members[i].shutdown();
                    clusterA.startClusterMember(i);
                    logger.info("Source cluster WAN config counts after bouncing: "
                            + getSourceClusterConfigCountMap(toBReplication.getSetupName()).toString());
                }
            }
        });

        assertPublisherCountEventually(REPLICATION_NAME, 0);

        while (true) {
            try {
                NodeEngineImpl nodeEngine = getNodeEngineImpl(clusterA.getAMember());
                ClusterService clusterService = nodeEngine.getClusterService();
                logger.info("Adding WAN replication on " + clusterService.getLocalMember()
                        + ", membership list: " + clusterService.getMembers()
                        + ", partitionTable: " + nodeEngine.getPartitionService().createPartitionTableView());
                nodeEngine.getWanReplicationService()
                        .addWanReplicationConfig(toBReplication.getConfig());
                logger.info("WAN replication config added");
                break;
            } catch (HazelcastInstanceNotActiveException e) {
                // invoked on a shutdown member, pick another member
            } catch (RejectedExecutionException e) {
                // member running the operation is shutting down when executing the callback of the invocation
            }
        }

        assertPublisherCountEventually(REPLICATION_NAME, 1);
        bouncingActive.set(false);
        logger.info("Stop bouncing the members");

        bouncingFuture.get();
        logger.info("Bouncing loop finished, bouncing the cluster one more time");
        assertPublisherCountEventually(REPLICATION_NAME, 1);

        // bounce cluster one more time
        HazelcastInstance[] members = clusterA.getMembers();
        for (int i = 0; i < members.length; i++) {
            logger.info("Source cluster WAN config counts before bouncing: "
                    + getSourceClusterConfigCountMap(toBReplication.getSetupName()).toString());
            members[i].shutdown();
            clusterA.startClusterMember(i);
            logger.info("Source cluster WAN config counts after bouncing: "
                    + getSourceClusterConfigCountMap(toBReplication.getSetupName()).toString());
        }

        logger.info("Members are up and running");
        assertPublisherCountEventually(REPLICATION_NAME, 1);
    }

    @Test
    public void testTwoConfigsAddedConcurrently() throws InterruptedException, BrokenBarrierException {
        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();
        final WanReplication toCReplication = replicate()
                .to(clusterC)
                .withSetupName(REPLICATION_NAME)
                .setup();
        clusterA.startCluster();

        assertPublisherCountEventually(REPLICATION_NAME, 0);

        final CountDownLatch configAdded = new CountDownLatch(2);
        final CyclicBarrier gate = new CyclicBarrier(3);
        spawn(() -> {
            try {
                gate.await();
                clusterA.addWanReplication(toBReplication);
                configAdded.countDown();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        });
        spawn(() -> {
            try {
                gate.await();
                clusterA.addWanReplication(toCReplication);
                configAdded.countDown();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        });
        gate.await();
        configAdded.await();

        assertPublisherCountEventually(REPLICATION_NAME, 2);
    }

    @Test
    public void syncAllAfterAddingMemberToSourceCluster() {
        clusterA.startAClusterMember();
        clusterB.startCluster();

        fillMap(clusterA, "map", 0, 1000);
        fillMap(clusterA, "map2", 0, 2000);

        assertKeysNotInEventually(clusterB.getMembers(), "map", 0, 1000);

        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();

        AddWanConfigResult result = clusterA.addWanReplication(toBReplication);

        assertContains(result.getAddedPublisherIds(), clusterB.getName());
        assertEquals(0, result.getIgnoredPublisherIds().size());

        clusterA.startAClusterMember();
        assertClusterSizeEventually(2, clusterA.getAMember());

        for (HazelcastInstance clusterAMember : clusterA.getMembers()) {
            assertNotNull(clusterAMember.getConfig().getWanReplicationConfig(REPLICATION_NAME));
        }

        fillMap(clusterA, "map3", 0, 3000);

        assertKeysNotInEventually(clusterB.getMembers(), "map", 0, 1000);
        assertKeysNotInEventually(clusterB.getMembers(), "map2", 0, 2000);
        assertKeysNotInEventually(clusterB.getMembers(), "map3", 0, 3000);

        clusterA.syncAllMaps(toBReplication);

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA.getMembers(), REPLICATION_NAME, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            clusterA.syncAllMaps(toBReplication);
        }

        verifyMapReplicated(clusterA, clusterB, "map");
        assertKeysNotInEventually(clusterB.getMembers(), "map2", 0, 2000);
        assertKeysNotInEventually(clusterB.getMembers(), "map3", 0, 3000);
    }

    @Test
    public void testConfigAddedTwiceIsIgnored() {
        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();
        final WanReplication toCReplication = replicate()
                .to(clusterC)
                .withSetupName(REPLICATION_NAME)
                .setup();

        clusterA.startCluster();

        assertPublisherCountEventually(REPLICATION_NAME, 0);

        AddWanConfigResult addResult = clusterA.addWanReplication(toBReplication);
        assertEquals(1, addResult.getAddedPublisherIds().size());
        assertEquals(0, addResult.getIgnoredPublisherIds().size());
        assertPublisherCountEventually(REPLICATION_NAME, 1);

        toBReplication.getConfig()
                .getBatchPublisherConfigs()
                .addAll(toCReplication.getConfig().getBatchPublisherConfigs());

        addResult = clusterA.addWanReplication(toBReplication);
        assertEquals(1, addResult.getAddedPublisherIds().size());
        assertEquals(1, addResult.getIgnoredPublisherIds().size());
        assertPublisherCountEventually(REPLICATION_NAME, 2);
    }

    private Map<String, Integer> getSourceClusterConfigCountMap(String wanReplicationName) {
        while (true) {
            Map<String, Integer> instanceConfigCount = new HashMap<>();
            boolean encounteredException = false;
            for (HazelcastInstance instance : clusterA.getMembers()) {
                Node node;
                try {
                    node = getNode(instance);
                } catch (IllegalArgumentException ex) {
                    // sleep and restart collection
                    encounteredException = true;
                    break;
                }
                WanReplicationConfig wanConfig = node.getConfig()
                        .getWanReplicationConfig(wanReplicationName);
                int configSize = wanConfig.getBatchPublisherConfigs().size();
                instanceConfigCount.put(instance.getName(), configSize);
            }
            if (!encounteredException) {
                return instanceConfigCount;
            } else {
                sleepMillis(250);
            }
        }
    }

    private void assertPublisherCountEventually(final String wanReplicationName,
                                                final int count) {
        assertTrueEventually(() -> {
            Map<String, Integer> instanceConfigCount = getSourceClusterConfigCountMap(wanReplicationName);
            for (Map.Entry<String, Integer> entry : instanceConfigCount.entrySet()) {
                if (entry.getValue() != count) {
                    fail(instanceConfigCount.toString());
                }
            }
        });
    }
}
