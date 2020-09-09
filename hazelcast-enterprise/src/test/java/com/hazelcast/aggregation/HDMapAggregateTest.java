package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getNativeMemorySize;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapAggregateTest extends MapAggregateTest {

    @Override
    public void doWithConfig(Config config) {
        MapConfig mapConfig = config.getMapConfig("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        MemorySize nativeMemorySize = getNativeMemorySize(config);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(nativeMemorySize)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        config.setNativeMemoryConfig(memoryConfig);
    }
}
