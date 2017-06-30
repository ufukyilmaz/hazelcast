package com.hazelcast.client.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

/**
 * Test for issue https://github.com/hazelcast/hazelcast/issues/2138
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(NightlyTest.class)
public class HDMapMemoryUsageStressTest extends MapMemoryUsageStressTest {

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
