package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.map.HDLocalMapStatsTest.SINGLE_MAP_ENTRY_COST;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class HDLocalMapStatsSlowTest
        extends HazelcastTestSupport {

    @Test
    public void hdOwnedEntryCost_whenPartitionRebalanced() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(
                getHDConfig(new Config(), POOLED, new MemorySize(128, MEGABYTES))
                        .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
        );
        waitAllForSafeState(instances);

        String mapName = "FooBar";

        IMap<String, Integer> map = instances[0].getMap(mapName);
        int numOfEntries = 200;
        int numOfNodeRestarts = 4;

        // Empty
        assertEquals(0, getTotalEntryCost(instances, mapName));

        // Populate data
        for (int i = 0; i < numOfEntries; i++) {
            map.put("Key_" + i, i);
        }

        // Verify size
        assertEquals(numOfEntries * SINGLE_MAP_ENTRY_COST, getTotalEntryCost(instances, mapName));

        // Restart one node to force partition migration and re-balancing (aka. invoking HDStorageImpl.reset())
        for (int i = 0; i < numOfNodeRestarts; i++) {
            instances[i].getLifecycleService().shutdown();
            instances[i] = factory.newHazelcastInstance(
                    getHDConfig(new Config(), POOLED, new MemorySize(128, MEGABYTES))
                            .setProperty(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "4")
            );
            waitAllForSafeState(instances);
        }

        // Verify size
        assertEquals(numOfEntries * SINGLE_MAP_ENTRY_COST, getTotalEntryCost(instances, mapName));
    }

    private long getTotalEntryCost(HazelcastInstance[] instances, String mapName) {
        long totalCost = 0;
        for (HazelcastInstance instance : instances) {
            totalCost += instance.getMap(mapName).getLocalMapStats().getOwnedEntryMemoryCost();
        }

        return totalCost;
    }

}
