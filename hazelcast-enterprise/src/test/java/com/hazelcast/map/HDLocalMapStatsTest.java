package com.hazelcast.map;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.PASSIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDLocalMapStatsTest extends LocalMapStatsTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }

    @Test
    public void hdStatsCollectionAllowed_whenClusterStateIsActive() throws Exception {
        hdStatsCollectionAllowedWhenClusterState(ACTIVE);
    }

    @Test
    public void hdStatsCollectionAllowed_whenClusterStateIsPassive() throws Exception {
        hdStatsCollectionAllowedWhenClusterState(PASSIVE);
    }

    @Test
    public void hdStatsCollectionAllowed_whenClusterStateIsFrozen() throws Exception {
        hdStatsCollectionAllowedWhenClusterState(FROZEN);
    }

    protected void hdStatsCollectionAllowedWhenClusterState(ClusterState state) {
        HazelcastInstance member = createHazelcastInstance(getConfig());
        IMap map = member.getMap("test");

        member.getCluster().changeClusterState(state);
        map.getLocalMapStats();
    }
}
