package com.hazelcast.wan;

import com.hazelcast.cache.merge.PassThroughCacheMergePolicy;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanCounterSyncTest {
    private static final String MAP_NAME = "map";
    private static final String CACHE_NAME = "cache";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private ScheduledExecutorService executorService;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @Parameter
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Parameterized.Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                NONE,
                MERKLE_TREES
        });
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

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.replicateCache(CACHE_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughCacheMergePolicy.class)
                     .setup();

        if (consistencyCheckStrategy == MERKLE_TREES) {
            sourceCluster.getConfig().getMapMerkleTreeConfig(MAP_NAME)
                         .setEnabled(true)
                         .setDepth(4);
            targetCluster.getConfig().getMapMerkleTreeConfig(MAP_NAME)
                         .setEnabled(true)
                         .setDepth(4);
        }

        executorService = Executors.newSingleThreadScheduledExecutor();
        // uncomment to dump the counters when debugging locally
        //        dumpWanCounters(wanReplication, executorService);
    }

    @Test
    public void testCountersReachZeroAfterSyncingInParallelWithLoad() throws Exception {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        executorService.scheduleAtFixedRate(new SyncTask(), 10, 100, MILLISECONDS);
        executorService.submit(new LoadTask())
                       .get();

        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    private class LoadTask implements Runnable {

        @Override
        public void run() {
            final IMap<Object, Object> map = sourceCluster.getAMember().getMap(MAP_NAME);
            for (int i = 0; i < 1000; i++) {
                if (i % 10 == 0) {
                    map.put(i, i);
                    try {
                        MILLISECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        EmptyStatement.ignore(e);
                    }
                }
            }
        }
    }

    private class SyncTask implements Runnable {

        @Override
        public void run() {
            try {
                sourceCluster.syncMap(wanReplication, MAP_NAME);
            } catch (Exception ex) {
                EmptyStatement.ignore(ex);
            }
        }
    }
}
