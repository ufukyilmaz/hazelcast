package com.hazelcast.client.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.HDTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Test for issue https://github.com/hazelcast/hazelcast/issues/2138
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(NightlyTest.class)
@Ignore(value = "https://github.com/hazelcast/hazelcast-enterprise/issues/1510")
public class HDMapMemoryUsageStressTest extends MapMemoryUsageStressTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }
}
