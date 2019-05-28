package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapInvalidationMetaDataMigrationTest extends MemberMapInvalidationMetaDataMigrationTest {

    @Override
    protected Config getConfig(String mapName) {
        return getHDConfig(super.getConfig(mapName));
    }

    @Override
    protected MapConfig getMapConfig(String mapName) {
        return super.getMapConfig(mapName)
                    .setInMemoryFormat(NATIVE)
                    .setEvictionPolicy(EvictionPolicy.LRU);
    }

    @Override
    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return super.getNearCacheConfig(mapName)
                .setInMemoryFormat(NATIVE);
    }
}
