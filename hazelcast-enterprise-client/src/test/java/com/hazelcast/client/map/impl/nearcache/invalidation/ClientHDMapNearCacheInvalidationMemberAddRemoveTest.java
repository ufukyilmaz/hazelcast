package com.hazelcast.client.map.impl.nearcache.invalidation;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.internal.nearcache.HiDensityNearCacheTestUtils.createNativeMemoryConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(NightlyTest.class)
public class ClientHDMapNearCacheInvalidationMemberAddRemoveTest extends ClientMapInvalidationMemberAddRemoveTest {

    @Override
    protected Config createConfig() {
        return getHDConfig(super.getConfig());
    }

    @Override
    protected ClientConfig createClientConfig() {
        return super.createClientConfig()
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }

    @Override
    protected NearCacheConfig createNearCacheConfig(String mapName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaximumSizePolicy(ENTRY_COUNT)
                .setSize(90);

        return super.createNearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(evictionConfig);
    }
}
