package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.STANDARD;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDQueryBounceTest extends QueryBounceTest {

    @Override
    protected Config getConfig() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig().setName("default").setInMemoryFormat(InMemoryFormat.NATIVE);

        MemorySize size = new MemorySize(64, MEGABYTES);

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(size)
                .setAllocatorType(STANDARD);

        return config
                .addMapConfig(mapConfig)
                .setNativeMemoryConfig(memoryConfig);
    }

}
