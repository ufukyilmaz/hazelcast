package com.hazelcast.map.impl.operation;

import com.hazelcast.config.Config;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.HDTestSupport.getNativeMemorySize;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDPostJoinMapOperationTest extends PostJoinMapOperationTest {

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MemorySize memorySize = getNativeMemorySize(config);
        return getHDConfig(config, NativeMemoryConfig.MemoryAllocatorType.POOLED, memorySize);
    }
}
