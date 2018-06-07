package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_INVALIDATE_ON_CHANGE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDMapNearCachePreloaderTest extends ClientMapNearCachePreloaderTest {

    @Before
    @Override
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(NATIVE, DEFAULT_SERIALIZE_KEYS, DEFAULT_INVALIDATE_ON_CHANGE, KEY_COUNT,
                storeFile.getParent());
    }

    @Override
    protected Config getConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected ClientConfig getClientConfig() {
        return super.getClientConfig()
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }
}
