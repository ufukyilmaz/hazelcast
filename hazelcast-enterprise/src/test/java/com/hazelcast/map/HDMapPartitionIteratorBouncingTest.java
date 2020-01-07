package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({SlowTest.class})
public class HDMapPartitionIteratorBouncingTest extends MapPartitionIteratorBouncingTest {

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
