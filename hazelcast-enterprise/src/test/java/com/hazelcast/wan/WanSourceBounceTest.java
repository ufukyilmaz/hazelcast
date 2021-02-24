package com.hazelcast.wan;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
import static com.hazelcast.wan.fw.WanCounterTestSupport.verifyEventCountersAreEventuallyZero;
import static com.hazelcast.wan.fw.WanMapTestSupport.fillMap;
import static com.hazelcast.wan.fw.WanMapTestSupport.verifyMapReplicated;
import static com.hazelcast.wan.fw.WanReplication.replicate;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanSourceBounceTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 3).setup();
        targetCluster = clusterB(factory, 1).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withInitialPublisherState(WanPublisherState.REPLICATING)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();

        // uncomment to dump the counters when debugging locally
        // dumpWanCounters(wanReplication, Executors.newSingleThreadScheduledExecutor());
    }

    @Test
    public void testOneMemberRestartTolerance() {
        memberRestartToleranceTest(1);
    }

    @Test
    public void testTwoMemberRestartTolerance() {
        memberRestartToleranceTest(2);
    }

    private void memberRestartToleranceTest(int restarts) {
        sourceCluster.startClusterMembers();

        fillMap(sourceCluster, MAP_NAME, 0, 10);

        for (int i = 0; i < restarts; i++) {
            sourceCluster.getAMember().shutdown();
            sourceCluster.startAClusterMember();
            waitAllForSafeState(sourceCluster.getMembers());
        }

        targetCluster.startCluster();

        verifyMapReplicated(sourceCluster, targetCluster, MAP_NAME);
        verifyEventCountersAreEventuallyZero(sourceCluster, wanReplication);
    }

}
