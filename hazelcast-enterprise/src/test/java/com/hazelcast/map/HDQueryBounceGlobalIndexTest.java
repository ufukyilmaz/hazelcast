package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.query.impl.HDGlobalIndexProvider.PROPERTY_GLOBAL_HD_INDEX_ENABLED;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDQueryBounceGlobalIndexTest extends QueryBounceTest {

    @Override
    protected Config getConfig() {
        Config config = getHDConfig(super.getConfig(), POOLED, new MemorySize(128, MEGABYTES));
        config.setProperty(PROPERTY_GLOBAL_HD_INDEX_ENABLED.getName(), "true");
        return config;
    }
}
