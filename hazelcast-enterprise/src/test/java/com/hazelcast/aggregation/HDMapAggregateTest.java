package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.NATIVE_MEMORY_SIZE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Ignore("https://github.com/hazelcast/hazelcast-enterprise/issues/2019")
public class HDMapAggregateTest extends MapAggregateTest {

    @Override
    public void doWithConfig(Config config) {
        MapConfig mapConfig = config.getMapConfig("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(NATIVE_MEMORY_SIZE)
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);

        config.setNativeMemoryConfig(memoryConfig);
    }
}
