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
import static org.junit.Assert.assertEquals;

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

    @Test
    public void hdOwnedEntryCost() {
        HazelcastInstance instance = createHazelcastInstance(HDTestSupport.getHDConfig(POOLED));
        MemoryStats stats = ((EnterpriseSerializationServiceV1) getNodeEngineImpl(instance)
                .getSerializationService()).getMemoryManager().getMemoryStats();

        IMap<String, Integer> map = instance.getMap("FooBar");
        int numOfEntries = 5;

        map.put("Key_0", 0);

        // Sizes as expected from the POOLED memory allocator
        int hdRecordDefaultCost = 128; // 64bytes (hd record structure) + 1byte (memory manager header)
                                       // can't fit in 64bytes block, so next size is 128bytes
        int keyCost = 32; // 4 bytes NativeMemoryData + 17 bytes actual data + 1 (memory manager header)
                          // next available block that fits is 32 bytes;
        int valueCost = 32; // 4 bytes NativeMemoryData + 12 bytes actual data + 1 (memory manager header)
                            // next available block that fits is 32 bytes;
        int behmSlotSize = 16; // Key address & Value address

        int expectedSizeSingleEntryMemoryCost = hdRecordDefaultCost + keyCost + valueCost + behmSlotSize;

        assertEquals(1, map.getLocalMapStats().getOwnedEntryCount());
        assertEquals(expectedSizeSingleEntryMemoryCost, map.getLocalMapStats().getOwnedEntryMemoryCost());
        assertEquals(0, map.getLocalMapStats().getHeapCost());

        for (int i = 1; i < numOfEntries; i++) {
            map.put("Key_" + i, i);
        }

        assertEquals(numOfEntries * expectedSizeSingleEntryMemoryCost, map.getLocalMapStats().getOwnedEntryMemoryCost());
        assertEquals(stats.getUsedNative() + (numOfEntries * behmSlotSize), map.getLocalMapStats().getOwnedEntryMemoryCost());

        map.clear();

        assertEquals(0, map.getLocalMapStats().getOwnedEntryMemoryCost());
    }
}
