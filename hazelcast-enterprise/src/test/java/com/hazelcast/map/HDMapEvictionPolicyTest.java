package com.hazelcast.map;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.eviction.MapEvictionPolicyTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapEvictionPolicyTest extends MapEvictionPolicyTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }
}
