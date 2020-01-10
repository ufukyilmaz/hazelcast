package com.hazelcast.client.cache.nearcache;

import com.hazelcast.client.cache.impl.nearcache.ClientCacheNearCacheCacheOnUpdateTest;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.internal.nearcache.impl.HDNearCacheTestUtils.createNativeMemoryConfig;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHDCacheNearCacheCacheOnUpdateTest extends ClientCacheNearCacheCacheOnUpdateTest {

    @Override
    protected Config createConfig() {
        return getHDConfig(super.createConfig());
    }

    @Override
    protected ClientConfig getClientConfig() {
        return super.getClientConfig()
                .setNativeMemoryConfig(createNativeMemoryConfig());
    }

    @Override
    protected NearCacheConfig getNearCacheConfig(NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        NearCacheConfig nearCacheConfig = super.getNearCacheConfig(localUpdatePolicy);
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);
        return nearCacheConfig;
    }
}
