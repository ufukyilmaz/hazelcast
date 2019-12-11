package com.hazelcast.wan.map;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.ConsistencyCheckStrategy;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.ConsistencyCheckStrategy.MERKLE_TREES;
import static com.hazelcast.config.ConsistencyCheckStrategy.NONE;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanAntiEntropyTestSupport.verifyAllPartitionsAreConsistent;
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.waitForSyncToComplete;
import static com.hazelcast.wan.map.WanBatchPublisherMapTest.isAllMembersConnected;
import static com.hazelcast.wan.map.WanMapTestSupport.assertKeysNotInEventually;
import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWanSyncClusterStateTest extends HazelcastTestSupport {

    @Parameters(name = "consistencyCheckStrategy:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {NONE},
                {MERKLE_TREES}
        });
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 2).setup();
        targetCluster = clusterB(factory, 2).setup();

        configureMerkleTrees(sourceCluster);
        configureMerkleTrees(targetCluster);

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withConsistencyCheckStrategy(consistencyCheckStrategy)
                .setup();


        sourceCluster.replicateMap("default")
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

//         uncomment to dump the counters when debugging locally
        //dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    private void configureMerkleTrees(Cluster cluster) {
        if (consistencyCheckStrategy == MERKLE_TREES) {
            cluster.getConfig().getMapConfig("default")
                    .getMerkleTreeConfig()
                    .setEnabled(true)
                    .setDepth(6);
        }
    }

    @Parameter(0)
    public ConsistencyCheckStrategy consistencyCheckStrategy;

    @Test
    public void testSyncWhenSourceClusterIsPassive() {
        sourceCluster.startCluster();
        targetCluster.startCluster();
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        assertKeysNotInEventually(targetCluster.getMembers(), "map", 0, 1000);

        sourceCluster.changeClusterState(ClusterState.PASSIVE);
        sourceCluster.syncMap(wanReplication, MAP_NAME);

        waitForSyncToComplete(sourceCluster);
        if (!isAllMembersConnected(sourceCluster.getMembers(), REPLICATION_NAME, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            sourceCluster.syncMap(wanReplication, MAP_NAME);
        }

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    @Test
    public void testConsistencyCheckWhenSourceAndTargetArePassive() {
        assumeTrue(consistencyCheckStrategy != NONE);

        sourceCluster.startCluster();
        targetCluster.startCluster();

        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);

        sourceCluster.changeClusterState(ClusterState.PASSIVE);
        targetCluster.changeClusterState(ClusterState.PASSIVE);

        sourceCluster.consistencyCheck(wanReplication, MAP_NAME);

        verifyAllPartitionsAreConsistent(sourceCluster, wanReplication, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

    @Test
    public void testReplicationResumesWhenSourceIsPassive() {
        sourceCluster.startCluster();
        targetCluster.startCluster();

        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        fillMap(sourceCluster, MAP_NAME, 0, 1000);
        sourceCluster.changeClusterState(ClusterState.PASSIVE);
        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

}
