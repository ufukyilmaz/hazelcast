package com.hazelcast.wan;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.map.merge.PassThroughMergePolicy;
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.test.HazelcastTestSupport.ASSERT_TRUE_EVENTUALLY_TIMEOUT;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationEndpoint;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanCounterClearQueuesTest {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private ScheduledExecutorService executorService;
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
        executorService.shutdown();
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
                .withWanPublisher(QueueClearerWanPublisher.class)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughCacheMergePolicy.class)
                     .setup();

        executorService = Executors.newSingleThreadScheduledExecutor();
        // uncomment to dump the counters when debugging locally
        // dumpWanCounters(wanReplication, executorService);
    }

    private void configureCache(Cluster cluster) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT);

        CacheSimpleConfig cacheConfig = cluster.getConfig().getCacheConfig(CACHE_NAME);
        cacheConfig.setEvictionConfig(evictionConfig)
                   .setMergePolicy(PassThroughCacheMergePolicy.class.getName());
    }

    @Test
    public void testCountersReachZeroAfterClearQueuesWithParallelLoad() throws Exception {
        sourceCluster.startAClusterMember();
        targetCluster.startCluster();

        final int entriesToPut = 1000;
        final CountDownLatch inTheMiddleOfLoadLatch = new CountDownLatch(entriesToPut / 2);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                fillMap(sourceCluster, MAP_NAME, 0, entriesToPut, inTheMiddleOfLoadLatch);
            }
        });

        inTheMiddleOfLoadLatch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, MILLISECONDS);
        wanReplicationService(sourceCluster.getAMember()).clearQueues(REPLICATION_NAME, targetCluster.getName());

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    @Test
    public void testCountersReachZeroAfterClearQueuesDuringMigration() {
        HazelcastInstance master = sourceCluster.startAClusterMember();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);

        sourceCluster.startClusterMembers(2, new PausingClusterMemberStartAction());
        ((QueueClearerWanPublisher) wanReplicationEndpoint(master, wanReplication))
                .clearQueuesOnNextMigration(master.getName());

        sourceCluster.startAClusterMember();
        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);
        sourceCluster.clearWanQueuesOnAllMembers(wanReplication);

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    private class PausingClusterMemberStartAction implements ClusterMemberStartAction {

        @Override
        public void onMemberStarted(HazelcastInstance instance) {
            wanReplicationService(instance).pause(REPLICATION_NAME, targetCluster.getConfig().getGroupConfig().getName());
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

}
