package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapNearCacheStalenessTest extends MapNearCacheStalenessTest {

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
        return super.getNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }
}
