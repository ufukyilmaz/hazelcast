package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.getNearCacheHDConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDMapInvalidationMetaDataMigrationTest extends MapInvalidationMetaDataMigrationTest {

    @Override
    protected Config getConfig() {
        return getNearCacheHDConfig();
    }

    @Override
    protected InMemoryFormat getNearCacheInMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }
}
