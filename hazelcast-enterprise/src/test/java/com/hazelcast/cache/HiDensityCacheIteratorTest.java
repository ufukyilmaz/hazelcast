package com.hazelcast.cache;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class HiDensityCacheIteratorTest extends CacheIteratorTest {

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(128, MemoryUnit.MEGABYTES));
        return config;
    }
}
