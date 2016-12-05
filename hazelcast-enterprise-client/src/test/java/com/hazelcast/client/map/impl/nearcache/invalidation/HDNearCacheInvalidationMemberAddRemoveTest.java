package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;
import static com.hazelcast.cache.nearcache.HiDensityNearCacheTestUtils.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class HDNearCacheInvalidationMemberAddRemoveTest extends InvalidationMemberAddRemoveTest {

    @Override
    protected NearCacheConfig createNearCacheConfig(String mapName) {
        NearCacheConfig nearCacheConfig = super.createNearCacheConfig(mapName);
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setMaximumSizePolicy(ENTRY_COUNT);
        evictionConfig.setSize(90);
         nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setName(mapName);
        return nearCacheConfig;
    }


    @Override
    protected Config createConfig() {
        return getHDConfig();
    }

    @Override
    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = super.createClientConfig();
        clientConfig.setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                .setNativeMemoryConfig(createNativeMemoryConfig());
        return clientConfig;
    }
}
