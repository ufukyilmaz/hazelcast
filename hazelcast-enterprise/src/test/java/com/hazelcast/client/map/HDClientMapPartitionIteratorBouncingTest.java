package com.hazelcast.client.map;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.HDMapPartitionIteratorBouncingTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({SlowTest.class})
public class HDClientMapPartitionIteratorBouncingTest extends HDMapPartitionIteratorBouncingTest {

    @Override
    protected boolean isClientDriver() {
        return true;
    }
}
