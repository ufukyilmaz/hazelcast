package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.impl.EnterpriseWanReplicationService;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.wan.fw.Cluster;
import com.hazelcast.wan.fw.WanReplication;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.wan.WanPublisherState.PAUSED;
import static com.hazelcast.wan.WanPublisherState.REPLICATING;
import static com.hazelcast.wan.WanPublisherState.STOPPED;
import static com.hazelcast.wan.fw.Cluster.clusterA;
import static com.hazelcast.wan.fw.Cluster.clusterB;
import static com.hazelcast.wan.fw.WanReplication.replicate;
import static com.hazelcast.wan.fw.WanTestSupport.wanReplicationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanReplicationStatsTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "map";
    private static final String REPLICATION_NAME = "wanReplication";

    private Cluster sourceCluster;
    private Cluster targetCluster;
    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
    private WanReplication wanReplication;

    @Before
    public void setup() {
        sourceCluster = clusterA(factory, 1).setup();
        targetCluster = clusterB(factory, 1).setup();

        wanReplication = replicate()
                .from(sourceCluster)
                .to(targetCluster)
                .withSetupName(REPLICATION_NAME)
                .withInitialPublisherState(PAUSED)
                .setup();

        sourceCluster.replicateMap(MAP_NAME)
                .withReplication(wanReplication)
                .withMergePolicy(PassThroughMergePolicy.class)
                .setup();
    }

    @Test
    public void testPublisherState() {
        HazelcastInstance sourceMember = sourceCluster.startAClusterMember();
        EnterpriseWanReplicationService service = wanReplicationService(sourceMember);
        assertPublisherState(service, PAUSED);

        sourceCluster.resumeWanReplicationOnAllMembers(wanReplication);
        assertPublisherState(service, REPLICATING);

        sourceCluster.stopWanReplicationOnAllMembers(wanReplication);
        assertPublisherState(service, STOPPED);
    }

    private void assertPublisherState(EnterpriseWanReplicationService service, WanPublisherState wanPublisherState) {
        LocalWanStats stats = service.getStats().get(REPLICATION_NAME);
        assertNotNull(stats);
        LocalWanPublisherStats publisherStats = stats.getLocalWanPublisherStats().get(targetCluster.getName());
        assertEquals(wanPublisherState, publisherStats.getPublisherState());
    }
}
