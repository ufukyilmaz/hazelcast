package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class HDMapNearCacheStaleReadTest extends MapNearCacheStaleReadTest {

    @Override
    protected Config getConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);

        MapConfig mapConfig = new MapConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setNearCacheConfig(nearCacheConfig);

        return getHDConfig(super.getConfig(mapName))
                .addMapConfig(mapConfig);
    }
}
