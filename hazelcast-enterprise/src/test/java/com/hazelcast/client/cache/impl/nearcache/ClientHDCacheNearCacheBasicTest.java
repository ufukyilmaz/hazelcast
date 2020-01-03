package com.hazelcast.client.cache.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_LOCAL_UPDATE_POLICY;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.internal.nearcache.HDNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;

/**
 * Basic HiDensity Near Cache tests for {@link com.hazelcast.cache.ICache} on Hazelcast clients.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDCacheNearCacheBasicTest extends ClientCacheNearCacheBasicTest {

    @Before
    @Override
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(NATIVE, DEFAULT_SERIALIZE_KEYS)
                .setLocalUpdatePolicy(DEFAULT_LOCAL_UPDATE_POLICY);
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
