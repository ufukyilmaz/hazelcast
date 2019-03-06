package com.hazelcast.wan.map;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.waitForSyncToComplete;
import static com.hazelcast.wan.map.MapWanBatchReplicationTest.isAllMembersConnected;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanInvocationFailoverTest {

    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster clusterA;
    private Cluster clusterB;

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Before
    public void setup() {
        clusterA = clusterA(factory, 2).setup();
        clusterB = clusterB(factory, 2).setup();

        WanReplication wanReplication = replicate()
                .from(clusterA)
                .to(null)
                .withSetupName(REPLICATION_NAME)
                .setup();


        clusterA.replicateMap("default")
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

//         uncomment to dump the counters when debugging locally
        //dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }


    @Test
    public void basicSyncTest() {
        clusterA.startCluster();
        clusterB.startCluster();

        WanReplication toBReplication = replicate()
                .to(clusterB)
                .withSetupName(REPLICATION_NAME)
                .withReplicationBatchSize(1)
                .setup();
        clusterA.addWanReplication(toBReplication);

        fillMap(clusterA, MAP_NAME, 0, 100);

        clusterB.shutdownMembers();
        factory.cleanup();

        clusterB.startCluster();

        clusterA.syncMap(toBReplication, MAP_NAME);

        waitForSyncToComplete(clusterA);
        if (!isAllMembersConnected(clusterA.getMembers(), REPLICATION_NAME, "B")) {
            // we give another try to the sync if it failed because of unsuccessful connection attempt
            clusterA.syncMap(toBReplication, MAP_NAME);
        }

        verifyMapReplicated(clusterA, clusterB, MAP_NAME);
    }
}
