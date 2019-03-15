package com.hazelcast.wan;

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

import static com.hazelcast.wan.WanReplicationTestSupport.assertWanQueueSizesEventually;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanReplication.replicate;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanQueueMigrationTest {
    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 2).setup();
        targetCluster = clusterB(factory, 2).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        // uncomment to dump the counters when debugging locally
        // dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    @Test
    public void testQueueMigration() {
        sourceCluster.startCluster();
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);

        fillMap(sourceCluster, MAP_NAME, 0, 1000);

        assertWanQueueSizesEventually(
                sourceCluster.getMembers(),
                wanReplication.getSetupName(),
                wanReplication.getTargetClusterName(), 1000);

        sourceCluster.getAMember().shutdown();

        assertWanQueueSizesEventually(
                sourceCluster.getMembers(),
                wanReplication.getSetupName(),
                wanReplication.getTargetClusterName(), 1000, 0);
    }
}
