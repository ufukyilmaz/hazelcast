package com.hazelcast.map;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceV1;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.map.HDTestSupport.getHDConfig;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDLocalMapStatsTest extends LocalMapStatsTest {

    // All sizes below are as expected from the POOLED memory allocator

    // 60bytes (hd record structure) + 1byte (memory manager header)
    // next available buddy block that fits, is 64bytes
    private static final int HD_RECORD_DEFAULT_COST = 64;

    // 4 bytes NativeMemoryData + 17 bytes actual data + 1 (memory manager header)
    // next available block that fits is 32 bytes;
    private static final int KEY_COST = 32;

    // 4 bytes NativeMemoryData + 12 bytes actual data + 1 (memory manager header)
    // next available block that fits is 32 bytes;
    private static final int VALUE_COST = 32;

    // Key address & Value address
    private static final int BEHM_SLOT_COST = 16;

    static final int SINGLE_MAP_ENTRY_COST = HD_RECORD_DEFAULT_COST + KEY_COST + VALUE_COST + BEHM_SLOT_COST;


    @Override
    protected Config getConfig() {
        return getHDConfig();
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

    @Test
    public void hdOwnedEntryCost() {
        HazelcastInstance instance = createHazelcastInstance(getHDConfig(POOLED));
        MemoryStats stats = ((EnterpriseSerializationServiceV1) getNodeEngineImpl(instance)
                .getSerializationService()).getMemoryManager().getMemoryStats();

        IMap<String, Integer> map = instance.getMap("FooBar");
        int numOfEntries = 5;

        map.put("Key_0", 0);

        assertEquals(1, map.getLocalMapStats().getOwnedEntryCount());
        assertEquals(SINGLE_MAP_ENTRY_COST, map.getLocalMapStats().getOwnedEntryMemoryCost());
        assertEquals(0, map.getLocalMapStats().getHeapCost());

        for (int i = 1; i < numOfEntries; i++) {
            map.put("Key_" + i, i);
        }

        assertEquals(numOfEntries * SINGLE_MAP_ENTRY_COST, map.getLocalMapStats().getOwnedEntryMemoryCost());
        assertEquals(stats.getUsedNative() + (numOfEntries * BEHM_SLOT_COST), map.getLocalMapStats().getOwnedEntryMemoryCost());

        map.clear();

        assertEquals(0, map.getLocalMapStats().getOwnedEntryMemoryCost());
    }

}
