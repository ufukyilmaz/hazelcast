package com.hazelcast.map;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.PASSIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDLocalMapStatsTest extends LocalMapStatsTest {

    // NOTE: all sizes below are as expected from the POOLED memory allocator

    // 60 bytes (HD record structure) + 1 byte (memory manager header)
    // next available buddy block that fits, is 64 bytes
    private static final int HD_RECORD_DEFAULT_COST = 64;

    // 4 bytes NativeMemoryData + 17 bytes actual data + 1 byte (memory manager header)
    // next available block that fits is 32 bytes
    private static final int KEY_COST = 32;

    // 4 bytes NativeMemoryData + 12 bytes actual data + 1 byte (memory manager header)
    // next available block that fits is 32 bytes
    private static final int VALUE_COST = 32;

    // key address & value address
    private static final int BEHM_SLOT_COST = 16;

    static final int SINGLE_MAP_ENTRY_COST = HD_RECORD_DEFAULT_COST + KEY_COST + VALUE_COST + BEHM_SLOT_COST;

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }

    @Test
    public void hdStatsCollectionAllowed_whenClusterStateIsActive() {
        hdStatsCollectionAllowedWhenClusterState(ACTIVE);
    }

    @Test
    public void hdStatsCollectionAllowed_whenClusterStateIsPassive() {
        hdStatsCollectionAllowedWhenClusterState(PASSIVE);
    }

    @Test
    public void hdStatsCollectionAllowed_whenClusterStateIsFrozen() {
        hdStatsCollectionAllowedWhenClusterState(FROZEN);
    }

    private void hdStatsCollectionAllowedWhenClusterState(ClusterState state) {
        IMap map = getMap();
        instance.getCluster().changeClusterState(state);
        map.getLocalMapStats();
    }
}
