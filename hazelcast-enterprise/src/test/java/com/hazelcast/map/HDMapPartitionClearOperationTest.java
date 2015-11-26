package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

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

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        try {
            node.shutdown();
        } catch (Exception e) {
            fail();
        }
    }

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }

}
