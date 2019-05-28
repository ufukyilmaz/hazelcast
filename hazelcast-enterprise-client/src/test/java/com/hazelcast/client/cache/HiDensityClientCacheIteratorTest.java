package com.hazelcast.client.cache;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HiDensityClientCacheIteratorTest extends ClientCacheIteratorTest {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        config.getNativeMemoryConfig().setEnabled(true)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES));

        return config;
    }
}
