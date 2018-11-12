package com.hazelcast.wan;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.AssertTask;
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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
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
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanAddConfigTest {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster clusterA;
    private Cluster clusterB;
    private Cluster clusterC;

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
        clusterA = clusterA(factory, 2).setup();
        clusterB = clusterB(factory, 2).setup();
        clusterC = clusterC(factory, 2).setup();

        configureCache(clusterA);
        configureCache(clusterB);
        configureCache(clusterC);

        wanReplication = replicate()
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
                .withMergePolicy(PassThroughCacheMergePolicy.class)
                .setup();

//         uncomment to dump the counters when debugging locally
        //dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT);

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig)
                   .setMergePolicy(PassThroughCacheMergePolicy.class.getName());
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

        Future mutationFuture = spawn(new Runnable() {
            @Override
            public void run() {
                while (mutationActive.get()) {
                    fillMap(clusterA, MAP_NAME, 0, 100);
                    fillCache(clusterA, CACHE_NAME, 0, 100);
                }
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

        Future bouncingFuture = spawn(new Runnable() {
            @Override
            public void run() {
                while (bouncingActive.get()) {
                    HazelcastInstance[] members = clusterA.getMembers();
                    for (int i = 0; i < members.length; i++) {
                        members[i].shutdown();
                        clusterA.startClusterMember(i);
                    }
                }
            }
        });

        assertPublisherCountEventually(REPLICATION_NAME, 0);

        while (true) {
            try {
                clusterA.addWanReplication(toBReplication);
                break;
            } catch (HazelcastInstanceNotActiveException e) {
                // invoked on a shutdown member, pick another member
            }
        }

        bouncingActive.set(false);
        bouncingFuture.get();

        // bounce cluster one more time
        HazelcastInstance[] members = clusterA.getMembers();
        for (int i = 0; i < members.length; i++) {
            members[i].shutdown();
            clusterA.startClusterMember(i);
        }

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
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    gate.await();
                    clusterA.addWanReplication(toBReplication);
                    configAdded.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        });
        spawn(new Runnable() {
            @Override
            public void run() {
                try {
                    gate.await();
                    clusterA.addWanReplication(toCReplication);
                    configAdded.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        });
        gate.await();
        configAdded.await();

        assertPublisherCountEventually(REPLICATION_NAME, 2);
    }

    @Test
    public void testConfigAddedTwiceIsIgnored() {
        final WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .setup();
        clusterA.startCluster();

        assertPublisherCountEventually(REPLICATION_NAME, 0);

        clusterA.addWanReplication(toBReplication);
        assertPublisherCountEventually(REPLICATION_NAME, 1);

        clusterA.addWanReplication(toBReplication);
        assertPublisherCountEventually(REPLICATION_NAME, 1);
    }

    private void assertPublisherCountEventually(final String wanReplicationName,
                                                final int count) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (HazelcastInstance instance : clusterA.getMembers()) {
                    WanReplicationConfig wanConfig = getNode(instance).getConfig()
                                                                      .getWanReplicationConfig(wanReplicationName);
                    assertEquals(count, wanConfig.getWanPublisherConfigs().size());
                }
            }
        });
    }
}
