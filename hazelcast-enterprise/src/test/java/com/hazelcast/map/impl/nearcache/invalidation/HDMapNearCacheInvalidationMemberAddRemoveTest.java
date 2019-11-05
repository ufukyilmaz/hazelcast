package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.MaxSizePolicy.ENTRY_COUNT;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(NightlyTest.class)
public class HDMapNearCacheInvalidationMemberAddRemoveTest extends MemberMapInvalidationMemberAddRemoveTest {

    @Override
    protected Config createConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected MapConfig createMapConfig(String mapName) {
        return super.createMapConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE);
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(90);

        return super.createNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
