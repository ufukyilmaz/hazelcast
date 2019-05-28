package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapNearCacheInvalidationTest extends MapNearCacheInvalidationTest {

    @Override
    protected Config getConfig(String mapName) {
        return getHDConfig(super.getConfig(mapName));
    }

    @Override
    protected MapConfig getMapConfig(String mapName) {
        return super.getMapConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Override
    protected NearCacheConfig getNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                .setSize(90);

        return super.getNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }

}
