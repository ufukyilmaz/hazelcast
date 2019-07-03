package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceV1;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDLocalMapStatsOwnedEntryCostTest extends HazelcastTestSupport {

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

    @Test
    public void hdOwnedEntryCost() {
        HazelcastInstance instance = createHazelcastInstance(getHDConfig(POOLED));
        MemoryStats stats = ((EnterpriseSerializationServiceV1) getNodeEngineImpl(instance).getSerializationService())
                .getMemoryManager().getMemoryStats();

        IMap<String, Integer> map = instance.getMap("ownedEntryCostMap");

        // single entry

        map.put("key-0", 0);

        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(1, localMapStats.getOwnedEntryCount());
        assertEquals(SINGLE_MAP_ENTRY_COST, localMapStats.getOwnedEntryMemoryCost());
        assertEquals(BEHM_SLOT_COST + stats.getUsedNative(), localMapStats.getOwnedEntryMemoryCost());
        assertEquals(0, localMapStats.getHeapCost());

        // multiple entries

        int numOfEntries = 5;
        for (int i = 1; i < numOfEntries; i++) {
            map.put("key-" + i, i);
        }

        localMapStats = map.getLocalMapStats();
        assertEquals(numOfEntries, localMapStats.getOwnedEntryCount());
        assertEquals(numOfEntries * SINGLE_MAP_ENTRY_COST, localMapStats.getOwnedEntryMemoryCost());
        assertEquals(numOfEntries * BEHM_SLOT_COST + stats.getUsedNative(), localMapStats.getOwnedEntryMemoryCost());
        assertEquals(0, localMapStats.getHeapCost());

        // empty map

        map.clear();

        localMapStats = map.getLocalMapStats();
        assertEquals(0, localMapStats.getOwnedEntryCount());
        assertEquals(0, localMapStats.getOwnedEntryMemoryCost());
        assertEquals(0, localMapStats.getHeapCost());
    }
}
