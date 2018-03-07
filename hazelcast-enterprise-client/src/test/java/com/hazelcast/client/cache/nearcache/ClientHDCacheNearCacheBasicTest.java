package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_LOCAL_UPDATE_POLICY;
import static com.hazelcast.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.getNearCacheHDConfig;
import static com.hazelcast.internal.nearcache.NearCacheTestUtils.createNearCacheConfig;

/**
 * Basic HiDensity Near Cache tests for {@link com.hazelcast.cache.ICache} on Hazelcast clients.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientHDCacheNearCacheBasicTest extends ClientCacheNearCacheBasicTest {

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(NATIVE, DEFAULT_SERIALIZE_KEYS)
                .setLocalUpdatePolicy(DEFAULT_LOCAL_UPDATE_POLICY);
    }

    @Override
    protected Config getConfig() {
        return getNearCacheHDConfig();
    }

    @Override
    protected ClientConfig createClientConfig() {
        return super.createClientConfig()
                .setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), UNLIMITED_LICENSE)
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }
}
