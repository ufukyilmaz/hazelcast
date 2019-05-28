package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapMetaDataGeneratorTest extends MemberMapMetaDataGeneratorTest {

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
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
