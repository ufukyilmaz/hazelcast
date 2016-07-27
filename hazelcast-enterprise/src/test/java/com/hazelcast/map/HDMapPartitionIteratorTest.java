package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapPartitionIteratorTest extends MapPartitionIteratorTest {

    @Override
    protected Config getConfig() {
        return HDTestSupport.getHDConfig();
    }
}
