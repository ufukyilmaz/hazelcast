package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;

/**
 * Basic HiDensity Near Cache tests for {@link com.hazelcast.core.IMap} on Hazelcast members.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapNearCacheBasicTest extends MapNearCacheBasicTest {

    @Before
    public void setUp() {
        nearCacheConfig
                = createNearCacheConfig(InMemoryFormat.NATIVE, DEFAULT_SERIALIZE_KEYS)
                .setCacheLocalEntries(true);
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }
}
