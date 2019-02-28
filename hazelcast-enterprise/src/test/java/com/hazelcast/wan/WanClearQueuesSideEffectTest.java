package com.hazelcast.wan;

import com.hazelcast.config.WanPublisherState;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.WanPublisherState.PAUSED;
import static com.hazelcast.config.WanPublisherState.REPLICATING;
import static com.hazelcast.config.WanPublisherState.STOPPED;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class WanClearQueuesSideEffectTest {
    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private WanReplication wanReplication;
    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 1).setup();
        Cluster targetCluster = clusterB(factory, 1).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withInitialPublisherState(REPLICATING)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                     .withReplication(wanReplication)
                     .withMergePolicy(PassThroughMergePolicy.class)
                     .setup();

        sourceCluster.startCluster();

        // initializing WAN publisher
        sourceCluster.getAMember()
                     .getMap(MAP_NAME)
                     .put("42", "42");

        targetCluster.startCluster();
    }

    @Test
    public void testClearQueuesWhenPausedDoesNotResume() {
        sourceCluster.pauseWanReplicationOnAllMembers(wanReplication);
        assertWanPublisherStateIs(PAUSED);

        sourceCluster.clearWanQueuesOnAllMembers(wanReplication);
        assertWanPublisherStateIs(PAUSED);
    }

    @Test
    public void testClearQueuesWhenStoppedDoesNotResume() {
        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);
        assertWanPublisherStateIs(STOPPED);

        sourceCluster.clearWanQueuesOnAllMembers(wanReplication);
        assertWanPublisherStateIs(STOPPED);
    }

    @Test
    public void testClearQueuesWhenActiveDoesNotPauseOrStop() {
        assertWanPublisherStateIs(REPLICATING);

        sourceCluster.clearWanQueuesOnAllMembers(wanReplication);
        assertWanPublisherStateIs(REPLICATING);
    }

    private void assertWanPublisherStateIs(WanPublisherState expectedState) {
        WanPublisherState publisherState = wanReplicationService(sourceCluster.getAMember())
                .getStats()
                .get(wanReplication.getSetupName())
                .getLocalWanPublisherStats()
                .get(wanReplication.getTargetClusterName())
                .getPublisherState();

        assertEquals(expectedState, publisherState);
    }
}
