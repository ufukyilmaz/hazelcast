package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.EnterpriseNodeExtension;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HDMapPartitionClearOperationTest extends HazelcastTestSupport {

    /**
     * This test calls internally {@link com.hazelcast.map.impl.operation.EnterpriseMapPartitionClearOperation}.
     */
    @Test
    public void testMapShutdown_finishesSuccessfully() throws Exception {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        IMap map = node.getMap("default");

        final int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        EnterpriseNodeExtension nodeExtension = (EnterpriseNodeExtension) getNode(node).getNodeExtension();
        MemoryStats memoryStats = nodeExtension.getMemoryManager().getMemoryStats();

        long minUsedMemory = count * 8L; // int key + int value
        assertThat(memoryStats.getUsedNative(), greaterThanOrEqualTo(minUsedMemory));

        node.shutdown();

        assertEquals(0, memoryStats.getUsedNative());
    }

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
